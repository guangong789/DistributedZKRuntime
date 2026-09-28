package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recoveryPassStore struct {
	JobStore
	list func(JobState) ([]JobRecord, error)
	load func(int64) (JobRecord, bool, error)
	save func(JobRecord) error
}

func (s *recoveryPassStore) ListByState(state JobState) ([]JobRecord, error) {
	if s.list != nil {
		return s.list(state)
	}
	return s.JobStore.ListByState(state)
}

func (s *recoveryPassStore) Load(id int64) (JobRecord, bool, error) {
	if s.load != nil {
		return s.load(id)
	}
	return s.JobStore.Load(id)
}

func (s *recoveryPassStore) Save(record JobRecord) error {
	if s.save != nil {
		return s.save(record)
	}
	return s.JobStore.Save(record)
}

func saveRecoveryRecord(t *testing.T, store JobStore, record JobRecord) {
	t.Helper()
	if err := store.Save(record); err != nil {
		t.Fatal(err)
	}
}

func assertRecoveryRecord(t *testing.T, store JobStore, want JobRecord) {
	t.Helper()
	got, ok, err := store.Load(want.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("record=%+v found=%v error=%v, want %+v", got, ok, err, want)
	}
}

func assertRecoveryOwnershipReleased(t *testing.T, s *CoordinatorServer, id int64) {
	t.Helper()
	if !s.claimJob(id) {
		t.Fatalf("ownership retained for job %d", id)
	}
	s.releaseJob(id)
}

func TestRegistrationSignalsRecoveryWithoutBlocking(t *testing.T) {
	for _, configured := range []bool{true, false} {
		s := newTestCoordinator()
		if configured {
			s.recoveryReady = make(chan struct{}, 1)
		}
		done := make(chan struct{})
		go func() {
			defer close(done)
			for i := 0; i < 2; i++ {
				resp, err := s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: "worker", Address: "localhost:1234"})
				if err != nil || resp == nil || !resp.Accepted {
					t.Errorf("registration response=%v error=%v", resp, err)
				}
			}
		}()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Fatal("registration blocked on a full or nil readiness channel")
		}
		if configured {
			select {
			case <-s.recoveryReady:
			default:
				t.Fatal("successful registration did not signal readiness")
			}
			if len(s.recoveryReady) != 0 {
				t.Fatal("readiness signals were not coalesced")
			}
		}
		worker, ok := s.getNextWorker()
		if !ok || worker.Status != WorkerAlive || len(s.workerOrder) != 1 {
			t.Fatal("registration did not leave one alive worker")
		}
	}

	s := &CoordinatorServer{recoveryReady: make(chan struct{}, 1)}
	resp, err := s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: "missing-address"})
	if err != nil || resp.Accepted || len(s.recoveryReady) != 0 {
		t.Fatalf("rejected registration signaled readiness: response=%v error=%v", resp, err)
	}
}

func TestRecoveryLoopRunsAgainAfterLaterRegistration(t *testing.T) {
	inner := NewMemoryJobStore()
	record := recoveringTestRecord()
	saveRecoveryRecord(t, inner, record)
	var scans, calls atomic.Int32
	terminal := make(chan JobRecord, 2)
	store := &recoveryPassStore{JobStore: inner, list: func(state JobState) ([]JobRecord, error) {
		scans.Add(1)
		if state != JobRecovering {
			t.Errorf("listed state=%v, want Recovering", state)
		}
		return inner.ListByState(state)
	}, save: func(record JobRecord) error {
		if err := inner.Save(record); err != nil {
			return err
		}
		if record.State == JobSucceeded {
			terminal <- record
		}
		return nil
	}}
	s := &CoordinatorServer{jobStore: store, recoveryReady: make(chan struct{}, 1)}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.runRecoveryLoop(ctx); close(done) }()
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.AttemptId != 8 || req.TaskType != record.TaskType || req.Payload != record.Payload || req.TimeoutMs != record.TimeoutMs {
			t.Errorf("recovery dispatch lost task spec or allocated wrong attempt: %+v", req)
		}
		return successfulResult(req), nil
	})
	select {
	case got := <-terminal:
		if got.JobID != record.JobID || got.AttemptID != 8 {
			t.Fatalf("first terminal record=%+v", got)
		}
	case <-ctx.Done():
		t.Fatal("first readiness signal did not execute recovery")
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, "worker"
	assertRecoveryRecord(t, inner, want)
	select {
	case <-done:
		t.Fatal("recovery loop exited after first pass")
	default:
	}

	later := record
	later.JobID++
	saveRecoveryRecord(t, inner, later)
	s.mu.Lock()
	workerAddress := s.workers["worker"].Address
	s.mu.Unlock()
	resp, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker", Address: workerAddress})
	if err != nil || !resp.Accepted {
		t.Fatalf("later registration: %v %v", resp, err)
	}
	select {
	case got := <-terminal:
		if got.JobID != later.JobID || got.AttemptID != 8 {
			t.Fatalf("second terminal record=%+v", got)
		}
	case <-ctx.Done():
		t.Fatal("later registration did not trigger a second pass")
	}
	later.State, later.AttemptID, later.WorkerID = JobSucceeded, 8, "worker"
	assertRecoveryRecord(t, inner, later)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery loop did not exit")
	}
	if scans.Load() != 2 || calls.Load() != 2 {
		t.Fatalf("repeated recovery: scans=%d calls=%d, want 2 each", scans.Load(), calls.Load())
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
	assertRecoveryOwnershipReleased(t, s, later.JobID)
}

func TestRecoveryPassSkipsStaleAndMissingRecords(t *testing.T) {
	for _, scenario := range []string{"terminal", "newer attempt", "missing", "changed after reload"} {
		t.Run(scenario, func(t *testing.T) {
			snapshot := recoveringTestRecord()
			current := snapshot
			if scenario == "newer attempt" {
				current.AttemptID++
			} else {
				current.State = JobSucceeded
			}
			inner := NewMemoryJobStore()
			if scenario != "missing" {
				saveRecoveryRecord(t, inner, current)
			}
			store := &recoveryPassStore{JobStore: inner, list: func(JobState) ([]JobRecord, error) {
				return []JobRecord{snapshot}, nil
			}}
			loads := 0
			store.load = func(id int64) (JobRecord, bool, error) {
				loads++
				if scenario == "changed after reload" && loads == 1 {
					// Simulate a SubmitJob completion after the pass's read but
					// before retryRecoveringJob takes ownership.
					return snapshot, true, nil
				}
				return inner.Load(id)
			}
			s := &CoordinatorServer{jobStore: store}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.runRecoveryPass(context.Background()); err != nil {
				t.Fatal(err)
			}
			if calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatal("stale or missing record was dispatched")
			}
			if scenario == "missing" {
				if _, ok, err := inner.Load(snapshot.JobID); err != nil || ok {
					t.Fatalf("missing record was recreated: found=%v error=%v", ok, err)
				}
			} else {
				assertRecoveryRecord(t, inner, current)
			}
			assertRecoveryOwnershipReleased(t, s, snapshot.JobID)
		})
	}
}

func TestRecoveryPassWithoutWorkersPreservesRecoveringRecord(t *testing.T) {
	s := newTestCoordinator()
	record := recoveringTestRecord()
	saveRecoveryRecord(t, s.jobStore, record)
	if err := s.runRecoveryPass(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, s.jobStore, record)
	if len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("no-worker recovery allocated an attempt or lease")
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestRecoveryPassContinuesAfterJobErrors(t *testing.T) {
	for _, scenario := range []string{"load failure", "rpc failure", "already active"} {
		t.Run(scenario, func(t *testing.T) {
			first, second := recoveringTestRecord(), recoveringTestRecord()
			second.JobID++
			inner := NewMemoryJobStore()
			saveRecoveryRecord(t, inner, first)
			saveRecoveryRecord(t, inner, second)
			store := &recoveryPassStore{JobStore: inner, list: func(JobState) ([]JobRecord, error) {
				return []JobRecord{first, second}, nil
			}}
			if scenario == "load failure" {
				store.load = func(id int64) (JobRecord, bool, error) {
					if id == first.JobID {
						return JobRecord{}, false, errors.New("load failed")
					}
					return inner.Load(id)
				}
			}
			s := &CoordinatorServer{jobStore: store}
			if scenario == "already active" {
				if !s.claimJob(first.JobID) {
					t.Fatal("cannot pre-claim job")
				}
				defer s.releaseJob(first.JobID)
			}
			var failedCalls, successCalls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if req.JobId == first.JobID {
					failedCalls.Add(1)
					return nil, status.Error(codes.Unavailable, "worker unavailable")
				}
				successCalls.Add(1)
				return successfulResult(req), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.runRecoveryPass(ctx); err != nil {
				t.Fatal(err)
			}
			wantFailedCalls := int32(0)
			if scenario == "rpc failure" {
				wantFailedCalls = 2
				first.State, first.AttemptID, first.WorkerID = JobRunning, 9, "worker"
			}
			if failedCalls.Load() != wantFailedCalls || successCalls.Load() != 1 {
				t.Fatalf("calls: failed=%d success=%d", failedCalls.Load(), successCalls.Load())
			}
			assertRecoveryRecord(t, inner, first)
			second.State, second.AttemptID, second.WorkerID = JobSucceeded, 8, "worker"
			assertRecoveryRecord(t, inner, second)
			assertRecoveryOwnershipReleased(t, s, second.JobID)
			if scenario == "already active" {
				if s.claimJob(first.JobID) {
					t.Fatal("recovery released another owner's claim")
				}
			} else {
				assertRecoveryOwnershipReleased(t, s, first.JobID)
			}
		})
	}
}

func TestRecoveryPassListErrorAndCanceledLoop(t *testing.T) {
	wantErr := errors.New("list failed")
	var scans atomic.Int32
	s := &CoordinatorServer{recoveryReady: make(chan struct{}, 1), jobStore: &recoveryPassStore{list: func(JobState) ([]JobRecord, error) {
		scans.Add(1)
		return nil, wantErr
	}}}
	if err := s.runRecoveryPass(context.Background()); !errors.Is(err, wantErr) {
		t.Fatalf("list error=%v, want %v", err, wantErr)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.runRecoveryPass(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled pass error=%v", err)
	}
	done := make(chan struct{})
	go func() { s.runRecoveryLoop(ctx); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery loop did not exit")
	}
	s.recoveryReady <- struct{}{}
	if scans.Load() != 1 || len(s.activeJobs) != 0 {
		t.Fatal("canceled recovery listed or claimed jobs, including after a later signal")
	}
}

func TestRecoveryLoopCoalescesSignalsDuringExecution(t *testing.T) {
	record := recoveringTestRecord()
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, record)
	var scans, calls atomic.Int32
	secondScan := make(chan struct{})
	store := &recoveryPassStore{JobStore: inner, list: func(state JobState) ([]JobRecord, error) {
		if scans.Add(1) == 2 {
			close(secondScan)
		}
		return inner.ListByState(state)
	}}
	s := &CoordinatorServer{jobStore: store, recoveryReady: make(chan struct{}, 1)}
	workerStarted := make(chan struct{})
	releaseWorker := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseWorker) }) })
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if calls.Add(1) == 1 {
			close(workerStarted)
		}
		<-releaseWorker
		return successfulResult(req), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.runRecoveryLoop(ctx); close(done) }()
	select {
	case <-workerStarted:
	case <-ctx.Done():
		t.Fatal("first pass never reached worker")
	}
	// The first pass is blocked inside its worker RPC. All later registrations
	// must fit into one pending notification without blocking registration.
	s.mu.Lock()
	address := s.workers["worker"].Address
	s.mu.Unlock()
	registered := make(chan struct{})
	go func() {
		defer close(registered)
		for i := 0; i < 10; i++ {
			if resp, err := s.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker", Address: address}); err != nil || !resp.Accepted {
				t.Errorf("registration %d: response=%v error=%v", i, resp, err)
			}
		}
	}()
	select {
	case <-registered:
	case <-ctx.Done():
		t.Fatal("registrations blocked behind recovery")
	}
	if scans.Load() != 1 || calls.Load() != 1 || len(s.recoveryReady) != 1 {
		t.Fatalf("overlapping passes or missing coalesced signal: scans=%d calls=%d queued=%d",
			scans.Load(), calls.Load(), len(s.recoveryReady))
	}
	releaseOnce.Do(func() { close(releaseWorker) })
	select {
	case <-secondScan:
	case <-ctx.Done():
		t.Fatal("coalesced signal did not trigger second pass")
	}
	if len(s.recoveryReady) != 0 {
		t.Fatalf("extra recovery signals queued: %d", len(s.recoveryReady))
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery loop did not exit")
	}
	if scans.Load() != 2 || calls.Load() != 1 || s.jobAttempts[record.JobID] != 8 {
		t.Fatalf("duplicate execution: scans=%d calls=%d attempt=%d", scans.Load(), calls.Load(), s.jobAttempts[record.JobID])
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, "worker"
	assertRecoveryRecord(t, inner, want)
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestRecoveryLoopRetriesRecoveringJobAfterLaterRegistration(t *testing.T) {
	record := recoveringTestRecord()
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, record)
	// The first pass has no registered workers. It leaves AttemptID 7 intact.
	s := &CoordinatorServer{jobStore: inner}
	if err := s.runRecoveryPass(context.Background()); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, inner, record)
	if len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("unscheduled pass allocated an attempt or lease")
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)

	completed := make(chan struct{}, 1)
	store := &recoveryPassStore{JobStore: inner, save: func(got JobRecord) error {
		if err := inner.Save(got); err != nil {
			return err
		}
		if got.State == JobSucceeded {
			completed <- struct{}{}
		}
		return nil
	}}
	s.jobStore = store
	s.recoveryReady = make(chan struct{}, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.runRecoveryLoop(ctx); close(done) }()
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.AttemptId != 8 || req.TaskType != record.TaskType || req.Payload != record.Payload || req.TimeoutMs != record.TimeoutMs {
			t.Errorf("recovered request=%+v, want saved spec at attempt 8", req)
		}
		return successfulResult(req), nil
	})
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("later registration did not recover job")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery loop did not exit")
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, "worker"
	assertRecoveryRecord(t, inner, want)
	if calls.Load() != 1 || s.jobAttempts[record.JobID] != 8 {
		t.Fatalf("unexpected retry: calls=%d attempt=%d", calls.Load(), s.jobAttempts[record.JobID])
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestRecoveryLoopContinuesAfterFailedPass(t *testing.T) {
	record := recoveringTestRecord()
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, record)
	var scans atomic.Int32
	firstScan := make(chan struct{})
	completed := make(chan struct{}, 1)
	store := &recoveryPassStore{JobStore: inner, list: func(state JobState) ([]JobRecord, error) {
		if scans.Add(1) == 1 {
			close(firstScan)
			return nil, errors.New("temporary list failure")
		}
		return inner.ListByState(state)
	}, save: func(got JobRecord) error {
		if err := inner.Save(got); err != nil {
			return err
		}
		if got.State == JobSucceeded {
			completed <- struct{}{}
		}
		return nil
	}}
	s := &CoordinatorServer{jobStore: store, recoveryReady: make(chan struct{}, 1)}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan struct{})
	go func() { s.runRecoveryLoop(ctx); close(done) }()
	select {
	case <-firstScan:
	case <-ctx.Done():
		t.Fatal("first pass did not run")
	}
	select {
	case <-done:
		t.Fatal("list error killed the recovery loop")
	default:
	}
	s.recoveryReady <- struct{}{}
	select {
	case <-completed:
	case <-ctx.Done():
		t.Fatal("second pass did not recover after list failure")
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("recovery loop did not exit")
	}
	if scans.Load() != 2 || calls.Load() != 1 {
		t.Fatalf("error continuation: scans=%d calls=%d", scans.Load(), calls.Load())
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, "worker"
	assertRecoveryRecord(t, inner, want)
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestRecoveryPassCancellationDuringExecution(t *testing.T) {
	first, second := recoveringTestRecord(), recoveringTestRecord()
	second.JobID++
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, first)
	saveRecoveryRecord(t, inner, second)
	s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, list: func(JobState) ([]JobRecord, error) {
		return []JobRecord{first, second}, nil
	}}}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var calls atomic.Int32
	workerCanceled := make(chan struct{})
	registerTestWorker(t, s, "worker", func(attemptCtx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		cancel()
		<-attemptCtx.Done()
		close(workerCanceled)
		return nil, attemptCtx.Err()
	})
	done := make(chan error, 1)
	go func() { done <- s.runRecoveryPass(ctx) }()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery error=%v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("canceled recovery did not stop")
	}
	select {
	case <-workerCanceled:
	case <-time.After(5 * time.Second):
		t.Fatal("attempt context was not canceled")
	}
	if calls.Load() != 1 || s.jobAttempts[first.JobID] != 8 {
		t.Fatal("cancellation retried or allocated an incorrect attempt")
	}
	first.State, first.AttemptID, first.WorkerID = JobRunning, 8, "worker"
	assertRecoveryRecord(t, inner, first)
	assertRecoveryRecord(t, inner, second)
	assertRecoveryOwnershipReleased(t, s, first.JobID)
	assertRecoveryOwnershipReleased(t, s, second.JobID)
}
