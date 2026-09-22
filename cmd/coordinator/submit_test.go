package main

import (
	"context"
	"errors"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type testWorker struct {
	runtimepb.UnimplementedWorkerServiceServer
	execute func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error)
}

func (w *testWorker) ExecuteJob(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
	return w.execute(ctx, req)
}

func registerTestWorker(t *testing.T, s *CoordinatorServer, id string, execute func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error), options ...grpc.ServerOption) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer(options...)
	runtimepb.RegisterWorkerServiceServer(server, &testWorker{execute: execute})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	resp, err := s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: id, Address: listener.Addr().String()})
	if err != nil || !resp.Accepted {
		t.Fatalf("registration: response=%v error=%v", resp, err)
	}
}

func successfulResult(req *runtimepb.ExecuteJobRequest) *runtimepb.ExecuteJobResponse {
	return &runtimepb.ExecuteJobResponse{JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: "done"}
}

func TestSubmitRetryAndFencing(t *testing.T) {
	for _, scenario := range []string{"success", "task failure", "rpc failure", "worker canceled", "wrong job", "wrong attempt", "expired lease", "superseded attempt", "exhausted retries"} {
		t.Run(scenario, func(t *testing.T) {
			s := &CoordinatorServer{}
			var firstCalls, secondCalls atomic.Int32
			registerTestWorker(t, s, "first", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				firstCalls.Add(1)
				if req.JobId != 500 || req.AttemptId != 1 || req.TaskType != "hash" || req.Payload != "hello" || req.TimeoutMs != 1000 {
					t.Errorf("incorrect dispatch: %v", req)
				}
				if _, ok := ctx.Deadline(); !ok {
					t.Error("attempt did not inherit parent deadline")
				}
				resp := successfulResult(req)
				switch scenario {
				case "task failure":
					resp.Status, resp.Error, resp.Output = "Failed", "task failed", ""
				case "rpc failure", "exhausted retries":
					return nil, status.Error(codes.Unavailable, "worker unavailable")
				case "worker canceled":
					return nil, status.Error(codes.Canceled, "worker canceled independently")
				case "wrong job":
					// Even a response matching another valid lease must be rejected.
					s.setLease(999, req.AttemptId, "first", time.Minute)
					resp.JobId = 999
				case "wrong attempt":
					resp.AttemptId = 0
				case "expired lease":
					s.setLease(req.JobId, req.AttemptId, "first", -time.Second)
				case "superseded attempt":
					s.startAttempt(req.JobId, "other", time.Minute)
				}
				return resp, nil
			})
			registerTestWorker(t, s, "second", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				secondCalls.Add(1)
				if ctx.Err() != nil {
					t.Errorf("retry inherited canceled context: %v", ctx.Err())
				}
				if scenario == "exhausted retries" {
					return nil, status.Error(codes.Unavailable, "second worker unavailable")
				}
				return successfulResult(req), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 500, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
			wantAttempt := int64(2)
			wantSecond := int32(1)
			if scenario == "success" || scenario == "task failure" {
				wantAttempt, wantSecond = 1, 0
			}
			if scenario == "superseded attempt" {
				wantAttempt = 3
			}
			if firstCalls.Load() != 1 || secondCalls.Load() != wantSecond {
				t.Fatalf("unexpected dispatch counts: first=%d second=%d", firstCalls.Load(), secondCalls.Load())
			}
			if scenario == "exhausted retries" {
				if resp != nil || status.Code(err) != codes.Unavailable {
					t.Fatalf("expected retry exhaustion, got response=%v error=%v", resp, err)
				}
			} else {
				if err != nil || resp == nil || resp.JobId != 500 || resp.AttemptId != wantAttempt {
					t.Fatalf("unexpected response=%v error=%v", resp, err)
				}
				if scenario == "task failure" {
					if resp.Status != "Failed" || resp.Error != "task failed" || resp.Output != "" {
						t.Fatalf("task failure not preserved: %v", resp)
					}
				} else if resp.Status != "Succeeded" || resp.Output != "done" || resp.Error != "" {
					t.Fatalf("task result not preserved: %v", resp)
				}
			}
			lease, ok := s.getLease(500)
			if !ok || lease.AttemptID != wantAttempt || s.jobAttempts[500] != wantAttempt {
				t.Fatalf("attempt/lease mismatch: lease=%+v attempts=%v", lease, s.jobAttempts)
			}
			wantWorker := "second"
			if wantSecond == 0 {
				wantWorker = "first"
			}
			if lease.WorkerID != wantWorker {
				t.Fatalf("lease worker=%q, want %q", lease.WorkerID, wantWorker)
			}
		})
	}
}

func TestSubmitCanceledParentDoesNotDispatch(t *testing.T) {
	for _, expired := range []bool{false, true} {
		s := &CoordinatorServer{}
		var calls atomic.Int32
		registerTestWorker(t, s, "worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			calls.Add(1)
			return successfulResult(req), nil
		})
		ctx, cancel := context.WithCancel(context.Background())
		want := context.Canceled
		if expired {
			cancel()
			ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
			want = context.DeadlineExceeded
		} else {
			cancel()
		}
		resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 1})
		cancel()
		if resp != nil || !errors.Is(err, want) || calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
			t.Fatalf("canceled submission mutated state or returned wrong error: response=%v error=%v", resp, err)
		}
	}
}

func TestSubmitCancellationDuringRPCDoesNotRetry(t *testing.T) {
	s := &CoordinatorServer{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	workerDone := make(chan struct{})
	allowReturn := make(chan struct{})
	defer close(allowReturn)
	var calls, secondCalls atomic.Int32
	registerTestWorker(t, s, "first", func(attemptCtx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		cancel()
		<-attemptCtx.Done()
		close(workerDone)
		// SubmitJob must stop without waiting for this handler to return.
		<-allowReturn
		return nil, status.Error(codes.Unavailable, "connection interrupted")
	})
	registerTestWorker(t, s, "second", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		return successfulResult(req), nil
	})
	resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 500})
	if resp != nil || !errors.Is(err, context.Canceled) || calls.Load() != 1 || s.jobAttempts[500] != 1 {
		t.Fatalf("canceled RPC retried or returned wrong error: response=%v error=%v calls=%d", resp, err, calls.Load())
	}
	if secondCalls.Load() != 0 {
		t.Fatalf("parent cancellation dispatched a second attempt: %d calls", secondCalls.Load())
	}
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("attempt context was not canceled")
	}
	if !s.claimJob(500) {
		t.Fatal("parent cancellation did not release job ownership")
	}
	s.releaseJob(500)
}

func TestSubmitLeaseExpiryRetriesWithLiveParent(t *testing.T) {
	s := &CoordinatorServer{leaseDuration: 100 * time.Millisecond}
	parentCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	firstCanceled := make(chan error, 1)
	firstReturned := make(chan struct{})
	allowFirstReturn := make(chan struct{})
	var releaseFirst sync.Once
	defer releaseFirst.Do(func() { close(allowFirstReturn) })
	var firstCalls, secondCalls atomic.Int32

	registerTestWorker(t, s, "first", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		defer close(firstReturned)
		if req.JobId != 500 || req.AttemptId != 1 {
			t.Errorf("first worker received wrong identity: %v", req)
		}
		lease, ok := s.getLease(req.JobId)
		if !ok || lease.AttemptID != 1 || lease.WorkerID != "first" {
			t.Errorf("first attempt lease missing or incorrect: %+v", lease)
		}
		<-ctx.Done()
		if time.Now().Before(lease.ExpiresAt) {
			t.Error("first attempt was canceled before its lease expired")
		}
		firstCanceled <- ctx.Err()
		// Hold even after cancellation, so retry success cannot depend on
		// this handler completing or returning its late success response.
		<-allowFirstReturn
		return successfulResult(req), nil
	})
	registerTestWorker(t, s, "second", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		if req.JobId != 500 || req.AttemptId != 2 {
			t.Errorf("second worker received wrong identity: %v", req)
		}
		if err := parentCtx.Err(); err != nil {
			t.Errorf("lease expiry canceled the parent: %v", err)
		}
		if err := ctx.Err(); err != nil {
			t.Errorf("second attempt inherited a canceled context: %v", err)
		}
		deadline, ok := ctx.Deadline()
		parentDeadline, _ := parentCtx.Deadline()
		// gRPC transports a relative timeout, so reconstructed deadlines
		// can differ slightly. It must inherit the parent, not the lease.
		if !ok || deadline.Before(parentDeadline.Add(-time.Second)) || deadline.After(parentDeadline.Add(time.Second)) {
			t.Errorf("second attempt deadline %v does not match parent %v", deadline, parentDeadline)
		}
		if s.claimJob(500) {
			s.releaseJob(500)
			t.Error("job ownership was released between attempts")
		}
		return successfulResult(req), nil
	})

	resp, err := s.SubmitJob(parentCtx, &runtimepb.SubmitJobRequest{JobId: 500})
	if err != nil || resp == nil || resp.JobId != 500 || resp.Status != "Succeeded" || resp.AttemptId != 2 || resp.Output != "done" || resp.Error != "" {
		t.Fatalf("lease retry failed: response=%v error=%v", resp, err)
	}
	if err := parentCtx.Err(); err != nil {
		t.Fatalf("attempt cancellation affected parent: %v", err)
	}
	select {
	case err := <-firstCanceled:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("first attempt cancellation: got %v, want context.Canceled", err)
		}
	case <-parentCtx.Done():
		t.Fatal("first attempt did not observe cancellation")
	}
	select {
	case <-firstReturned:
		t.Fatal("first handler returned before retry completed")
	default:
	}
	if s.isLeaseCurrent(500, 1) {
		t.Fatal("late first-attempt result still passes fencing")
	}

	// Allow the stale response only after SubmitJob has returned success.
	releaseFirst.Do(func() { close(allowFirstReturn) })
	select {
	case <-firstReturned:
	case <-parentCtx.Done():
		t.Fatal("first worker did not finish after release")
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || s.jobAttempts[500] != 2 {
		t.Fatalf("unexpected retries: first=%d second=%d attempts=%d", firstCalls.Load(), secondCalls.Load(), s.jobAttempts[500])
	}
	lease, ok := s.getLease(500)
	if !ok || lease.AttemptID != 2 || lease.WorkerID != "second" {
		t.Fatalf("late first response changed the second lease: %+v", lease)
	}
	if !s.claimJob(500) {
		t.Fatal("successful submission did not release job ownership")
	}
	s.releaseJob(500)
}

func TestWorkerRegistrationLivenessAndRoundRobin(t *testing.T) {
	s := &CoordinatorServer{}
	ctx := context.Background()
	resp, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "missing-address"})
	if err != nil || resp.Accepted || len(s.workers) != 0 {
		t.Fatalf("invalid registration accepted: %v %v", resp, err)
	}
	for _, id := range []string{"a", "b", "a"} {
		resp, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: id, Address: id + ":1234"})
		if err != nil || !resp.Accepted {
			t.Fatalf("registration failed: %v %v", resp, err)
		}
	}
	if len(s.workerOrder) != 2 {
		t.Fatal("re-registration duplicated worker order")
	}
	for _, want := range []string{"a", "b", "a", "b"} {
		worker, ok := s.getNextWorker()
		if !ok || worker.ID != want {
			t.Fatalf("round robin got %+v, want %s", worker, want)
		}
	}
	s.mu.Lock()
	worker := s.workers["a"]
	worker.LastSeen = time.Now().Add(-time.Hour)
	s.workers["a"] = worker
	s.mu.Unlock()
	s.checkWorkerLiveness(time.Minute)
	for i := 0; i < 3; i++ {
		worker, ok := s.getNextWorker()
		if !ok || worker.ID != "b" {
			t.Fatalf("selected dead worker: %+v", worker)
		}
	}
	hb, err := s.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: "unknown"})
	if err != nil || hb.Accepted {
		t.Fatalf("unknown heartbeat accepted: %v %v", hb, err)
	}
	hb, err = s.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: "a"})
	if err != nil || !hb.Accepted || s.workers["a"].Status != WorkerAlive || time.Since(s.workers["a"].LastSeen) > time.Second {
		t.Fatalf("heartbeat did not revive worker: %v %v", hb, err)
	}
}

func TestSubmitNoAliveWorkers(t *testing.T) {
	for _, workers := range []map[string]WorkerInfo{nil, {"dead": {ID: "dead", Status: WorkerDead}}} {
		s := &CoordinatorServer{workers: workers}
		for id := range workers {
			s.workerOrder = append(s.workerOrder, id)
		}
		resp, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 1})
		if resp != nil || status.Code(err) != codes.Unavailable || len(s.jobAttempts) != 0 {
			t.Fatalf("expected no-worker error without allocating attempt: %v %v", resp, err)
		}
	}
}

func TestConcurrentAttemptAllocation(t *testing.T) {
	s := &CoordinatorServer{}
	const count = 100
	ids := make(chan int64, count)
	var wg sync.WaitGroup
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ids <- s.startAttempt(500, "worker", time.Minute)
		}()
	}
	wg.Wait()
	close(ids)
	seen := make(map[int64]bool)
	for id := range ids {
		if id < 1 || id > count || seen[id] {
			t.Fatalf("invalid or duplicate attempt: %d", id)
		}
		seen[id] = true
	}
	lease, ok := s.getLease(500)
	if !ok || lease.AttemptID != count || !s.isLeaseCurrent(500, count) || s.isLeaseCurrent(500, count-1) {
		t.Fatalf("latest attempt and lease diverged: %+v", lease)
	}
	if next := s.startAttempt(501, "worker", time.Minute); next != 1 {
		t.Fatalf("attempt IDs are not per-job: %d", next)
	}
}

func TestLeaseExpiryBoundaryAndMissingLease(t *testing.T) {
	s := &CoordinatorServer{}
	if s.leaseExpired(1, time.Now()) || s.isLeaseCurrent(1, 1) {
		t.Fatal("missing lease reported expired or current")
	}
	s.setLease(1, 1, "worker", time.Minute)
	lease, _ := s.getLease(1)
	if !s.leaseExpired(1, lease.ExpiresAt) {
		t.Fatal("lease must be expired at its expiry time")
	}
}

func TestRetrySkipsDeadWorker(t *testing.T) {
	s := &CoordinatorServer{}
	registerTestWorker(t, s, "first", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return nil, status.Error(codes.Unavailable, "worker unavailable")
	})
	var deadCalls atomic.Int32
	registerTestWorker(t, s, "dead", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		deadCalls.Add(1)
		return successfulResult(req), nil
	})
	registerTestWorker(t, s, "last", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(req), nil
	})
	s.mu.Lock()
	worker := s.workers["dead"]
	worker.LastSeen = time.Now().Add(-time.Hour)
	s.workers["dead"] = worker
	s.mu.Unlock()
	s.checkWorkerLiveness(time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 1})
	lease, _ := s.getLease(1)
	if err != nil || resp == nil || resp.AttemptId != 2 || lease.WorkerID != "last" || deadCalls.Load() != 0 {
		t.Fatalf("retry did not skip dead worker: response=%v error=%v lease=%+v dead calls=%d", resp, err, lease, deadCalls.Load())
	}
}

func TestConcurrentWorkerOperations(t *testing.T) {
	s := &CoordinatorServer{}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: "worker", Address: "localhost:1234"})
			s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: "worker"})
			s.checkWorkerLiveness(time.Minute)
			s.getNextWorker()
		}()
	}
	wg.Wait()
	if len(s.workers) != 1 || len(s.workerOrder) != 1 || s.workers["worker"].Status != WorkerAlive {
		t.Fatalf("concurrent worker operations corrupted registry: %+v", s.workers)
	}
}

func TestClaimJobExclusive(t *testing.T) {
	s := &CoordinatorServer{}

	if !s.claimJob(500) {
		t.Fatal("expected first claim to succeed")
	}

	if s.claimJob(500) {
		t.Fatal("expected second claim to fail")
	}

	s.releaseJob(500)

	if !s.claimJob(500) {
		t.Fatal("expected claim after release to succeed")
	}
}

func TestConcurrentClaimJobOnlyOneSucceeds(t *testing.T) {
	s := &CoordinatorServer{}

	const goroutines = 20

	var wg sync.WaitGroup
	results := make(chan bool, goroutines)

	for i := 0; i < goroutines; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			results <- s.claimJob(500)
		}()
	}

	wg.Wait()
	close(results)

	successes := 0

	for ok := range results {
		if ok {
			successes++
		}
	}

	if successes != 1 {
		t.Fatalf(
			"expected exactly one successful claim, got %d",
			successes,
		)
	}
}

func TestLateSuccessfulAttemptCannotReplaceAcceptedRetry(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstStarted := make(chan int64, 1)
	secondStarted := make(chan int64, 1)
	allowFirstReturn := make(chan struct{})
	allowSecondReturn := make(chan struct{})
	var releaseFirst, releaseSecond sync.Once
	defer releaseFirst.Do(func() { close(allowFirstReturn) })
	defer releaseSecond.Do(func() { close(allowSecondReturn) })
	lateReturned := make(chan *runtimepb.ExecuteJobResponse, 1)
	dispatchDone := make(chan int64, 2)
	var firstCalls, secondCalls atomic.Int32
	s := &CoordinatorServer{
		leaseDuration: 100 * time.Millisecond,
		onDispatchDone: func(jobID, attemptID int64) {
			if jobID != 500 {
				t.Errorf("unexpected dispatch job: %d", jobID)
			}
			dispatchDone <- attemptID
		},
	}
	registerTestWorker(t, s, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		firstStarted <- req.AttemptId
		// Deliberately ignore cancellation: remote work can outlive its RPC.
		<-allowFirstReturn
		resp := successfulResult(req)
		resp.Output = "late-first"
		return resp, nil
	}, grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if err != nil {
			t.Errorf("delayed worker did not return success: %v", err)
		} else {
			// Observe the actual handler return, not just its intent to return.
			lateReturned <- resp.(*runtimepb.ExecuteJobResponse)
		}
		return resp, err
	}))
	registerTestWorker(t, s, "second", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		secondStarted <- req.AttemptId
		select {
		case <-allowSecondReturn:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		resp := successfulResult(req)
		resp.Output = "accepted-second"
		return resp, nil
	})

	type submissionResult struct {
		resp *runtimepb.SubmitJobResponse
		err  error
	}
	submitted := make(chan submissionResult, 1)
	go func() {
		resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 500})
		submitted <- submissionResult{resp: resp, err: err}
	}()
	waitAttempt := func(ch <-chan int64, want int64, label string) {
		t.Helper()
		select {
		case got := <-ch:
			if got != want {
				t.Fatalf("%s: attempt=%d, want %d", label, got, want)
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", label)
		}
	}
	waitAttempt(firstStarted, 1, "first worker start")
	waitAttempt(secondStarted, 2, "second worker start")
	if s.isLeaseCurrent(500, 1) {
		t.Fatal("attempt 1 still current after attempt 2 started")
	}
	releaseSecond.Do(func() { close(allowSecondReturn) })
	var accepted *runtimepb.SubmitJobResponse
	select {
	case result := <-submitted:
		accepted = result.resp
		if result.err != nil {
			t.Fatalf("retry failed: %v", result.err)
		}
	case <-ctx.Done():
		t.Fatal("submission did not complete")
	}
	if accepted == nil || accepted.JobId != 500 || accepted.AttemptId != 2 || accepted.Status != "Succeeded" || accepted.Output != "accepted-second" || accepted.Error != "" {
		t.Fatalf("unexpected accepted response: %v", accepted)
	}
	before, _ := s.getLease(500)
	select {
	case <-lateReturned:
		t.Fatal("first worker returned before it was released")
	default:
	}

	// Completion notifications occur after each production resultCh send.
	// Attempt 1 must finish even while its remote handler remains blocked.
	completed := make(map[int64]bool)
	for i := 0; i < 2; i++ {
		select {
		case id := <-dispatchDone:
			if id < 1 || id > 2 || completed[id] {
				t.Fatalf("unexpected dispatch completion: %d", id)
			}
			completed[id] = true
		case <-ctx.Done():
			t.Fatal("abandoned dispatch blocked publishing its result")
		}
	}
	releaseFirst.Do(func() { close(allowFirstReturn) })
	select {
	case late := <-lateReturned:
		if late.JobId != 500 || late.AttemptId != 1 || late.Status != "Succeeded" || late.Output != "late-first" || late.Error != "" {
			t.Fatalf("expected late successful attempt 1: %v", late)
		}
		if s.isLeaseCurrent(late.JobId, late.AttemptId) {
			t.Fatal("late successful response passes fencing")
		}
	case <-ctx.Done():
		t.Fatal("first worker never returned its late success")
	}
	after, _ := s.getLease(500)
	if after != before || after.AttemptID != 2 || after.WorkerID != "second" {
		t.Fatalf("late result altered lease: before=%+v after=%+v", before, after)
	}
	if accepted.AttemptId != 2 || accepted.Status != "Succeeded" || accepted.Output != "accepted-second" {
		t.Fatalf("late result changed accepted response: %v", accepted)
	}
	s.mu.Lock()
	attempts := s.jobAttempts[500]
	_, owned := s.activeJobs[500]
	s.mu.Unlock()
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || attempts != 2 || owned {
		t.Fatalf("calls=(%d,%d) attempts=%d owned=%v", firstCalls.Load(), secondCalls.Load(), attempts, owned)
	}
}

func TestSupersededSuccessFailsFencingBeforeOldLeaseExpires(t *testing.T) {
	s := &CoordinatorServer{}
	first := s.startAttempt(500, "first", time.Hour)
	oldLease, _ := s.getLease(500)
	late := successfulResult(&runtimepb.ExecuteJobRequest{JobId: 500, AttemptId: first})
	second := s.startAttempt(500, "second", time.Hour)
	if first != 1 || second != 2 {
		t.Fatalf("attempt IDs: first=%d second=%d", first, second)
	}
	if !time.Now().Before(oldLease.ExpiresAt) {
		t.Fatal("test requires the original lease to be unexpired")
	}
	if s.isLeaseCurrent(late.JobId, late.AttemptId) {
		t.Fatal("superseded success accepted despite newer attempt")
	}
	if !s.isLeaseCurrent(500, second) {
		t.Fatal("newer attempt should still pass fencing")
	}
}
