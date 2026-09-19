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

func registerTestWorker(t *testing.T, s *CoordinatorServer, id string, execute func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error)) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
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
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	workerDone := make(chan struct{})
	var calls atomic.Int32
	registerTestWorker(t, s, "first", func(attemptCtx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		cancel()
		<-attemptCtx.Done()
		close(workerDone)
		return nil, status.Error(codes.Unavailable, "connection interrupted")
	})
	resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 500})
	if resp != nil || !errors.Is(err, context.Canceled) || calls.Load() != 1 || s.jobAttempts[500] != 1 {
		t.Fatalf("canceled RPC retried or returned wrong error: response=%v error=%v calls=%d", resp, err, calls.Load())
	}
	select {
	case <-workerDone:
	case <-time.After(5 * time.Second):
		t.Fatal("attempt context was not canceled")
	}
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
