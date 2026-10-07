package main

import (
	"context"
	"errors"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoverRunningJobsPreservesIdentityAndTerminalStates(t *testing.T) {
	store := NewMemoryJobStore()
	records := []JobRecord{
		{JobID: 100, State: JobRunning, AttemptID: 7, WorkerID: "worker-a", TaskType: "hash", Payload: "job-100", TimeoutMs: 1500},
		{JobID: 200, State: JobRunning, AttemptID: 3, WorkerID: "worker-b", TaskType: "sleep", Payload: "250ms", TimeoutMs: 2000},
		{JobID: 300, State: JobSucceeded, AttemptID: 2, WorkerID: "worker-c", TaskType: "hash", Payload: "completed", TimeoutMs: 1000},
		{JobID: 400, State: JobFailed, AttemptID: 4, WorkerID: "worker-d"},
		{JobID: 500, State: JobCancelled, AttemptID: 5, WorkerID: "worker-e"},
	}
	for _, record := range records {
		if err := store.Save(record); err != nil {
			t.Fatalf("seed job %d: %v", record.JobID, err)
		}
	}

	coordinator := &CoordinatorServer{jobStore: store}
	var workerCalls atomic.Int32
	registerTestWorker(t, coordinator, "available-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		workerCalls.Add(1)
		return successfulResult(req), nil
	})
	if err := coordinator.recoverRunningJobs(); err != nil {
		t.Fatalf("recover running jobs: %v", err)
	}
	for _, want := range records {
		if want.State == JobRunning {
			want.State = JobRecovering
		}
		got, ok, err := store.Load(want.JobID)
		if err != nil || !ok || got != want {
			t.Fatalf("job %d after recovery: got=%+v found=%v error=%v, want %+v", want.JobID, got, ok, err, want)
		}
	}
	assertListedRecords(t, store, JobRunning)
	recovering100, recovering200 := records[0], records[1]
	recovering100.State, recovering200.State = JobRecovering, JobRecovering
	assertListedRecords(t, store, JobRecovering, recovering100, recovering200)
	if len(coordinator.jobAttempts) != 0 || len(coordinator.leases) != 0 {
		t.Fatalf("recovery allocated attempts or leases: attempts=%v leases=%v", coordinator.jobAttempts, coordinator.leases)
	}
	if workerCalls.Load() != 0 || len(coordinator.activeJobs) != 0 {
		t.Fatalf("recovery marking dispatched or claimed jobs: calls=%d active=%v", workerCalls.Load(), coordinator.activeJobs)
	}
}

func TestRecoverRunningJobsWithNoRunningJobs(t *testing.T) {
	store := NewMemoryJobStore()
	want := JobRecord{JobID: 300, State: JobSucceeded, AttemptID: 2, WorkerID: "worker-c", TaskType: "hash", Payload: "completed", TimeoutMs: 1000}
	if err := store.Save(want); err != nil {
		t.Fatalf("seed terminal job: %v", err)
	}
	if err := (&CoordinatorServer{jobStore: store}).recoverRunningJobs(); err != nil {
		t.Fatalf("recover without running jobs: %v", err)
	}
	got, ok, err := store.Load(want.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("terminal job changed: got=%+v found=%v error=%v", got, ok, err)
	}
}

func TestRecoverRunningJobsWithNilStore(t *testing.T) {
	if err := (&CoordinatorServer{}).recoverRunningJobs(); err != nil {
		t.Fatalf("recover with nil store: %v", err)
	}
}

type failingRecoveryStore struct {
	JobStore
	listErr     error
	saveErr     error
	listedState JobState
	listCalls   int
	saveCalls   int
}

func (s *failingRecoveryStore) ListByState(state JobState) ([]JobRecord, error) {
	s.listCalls++
	s.listedState = state
	if s.listErr != nil {
		return nil, s.listErr
	}
	return s.JobStore.ListByState(state)
}

func (s *failingRecoveryStore) Save(record JobRecord) error {
	s.saveCalls++
	if s.saveErr != nil {
		return s.saveErr
	}
	return s.JobStore.Save(record)
}

func TestRecoverRunningJobsPropagatesListError(t *testing.T) {
	wantErr := errors.New("list failed")
	store := &failingRecoveryStore{JobStore: NewMemoryJobStore(), listErr: wantErr}
	err := (&CoordinatorServer{jobStore: store}).recoverRunningJobs()
	if !errors.Is(err, wantErr) || store.listCalls != 1 ||
		store.listedState != JobRunning || store.saveCalls != 0 {
		t.Fatalf("list failure: error=%v listCalls=%d state=%d saveCalls=%d",
			err, store.listCalls, store.listedState, store.saveCalls)
	}
}

func TestRecoverRunningJobsPropagatesSaveError(t *testing.T) {
	inner := NewMemoryJobStore()
	want := JobRecord{JobID: 100, State: JobRunning, AttemptID: 7, WorkerID: "worker-a", TaskType: "hash", Payload: "job-100", TimeoutMs: 1500}
	if err := inner.Save(want); err != nil {
		t.Fatalf("seed running job: %v", err)
	}
	wantErr := errors.New("save failed")
	store := &failingRecoveryStore{JobStore: inner, saveErr: wantErr}
	err := (&CoordinatorServer{jobStore: store}).recoverRunningJobs()
	if !errors.Is(err, wantErr) || store.listCalls != 1 ||
		store.listedState != JobRunning || store.saveCalls != 1 {
		t.Fatalf("save failure: error=%v listCalls=%d state=%d saveCalls=%d",
			err, store.listCalls, store.listedState, store.saveCalls)
	}
	got, ok, err := inner.Load(want.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("failed save changed record: got=%+v found=%v error=%v", got, ok, err)
	}
}

func TestRecoverRunningJobsPersistsSQLiteStateWithoutChangingLegacyTerminals(t *testing.T) {
	store, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close SQLite store: %v", err)
		}
	}()

	running := JobRecord{JobID: 100, State: JobRunning, AttemptID: 7, WorkerID: "worker-a", TaskType: "hash", Payload: "job-100", TimeoutMs: 1500}
	if err := store.Save(running); err != nil {
		t.Fatalf("seed running job: %v", err)
	}
	// These integer values were persisted before JobRecovering was added.
	legacyTerminals := []struct {
		record JobRecord
		value  int
	}{
		{JobRecord{JobID: 300, State: JobSucceeded, AttemptID: 2, WorkerID: "worker-c"}, 2},
		{JobRecord{JobID: 400, State: JobFailed, AttemptID: 4, WorkerID: "worker-d"}, 3},
		{JobRecord{JobID: 500, State: JobCancelled, AttemptID: 5, WorkerID: "worker-e"}, 4},
	}
	for _, terminal := range legacyTerminals {
		_, err := store.db.Exec(
			"INSERT INTO jobs (job_id, state, attempt_id, worker_id) VALUES (?, ?, ?, ?)",
			terminal.record.JobID, terminal.value, terminal.record.AttemptID, terminal.record.WorkerID,
		)
		if err != nil {
			t.Fatalf("seed legacy job %d: %v", terminal.record.JobID, err)
		}
	}

	coordinator := &CoordinatorServer{jobStore: store}
	if err := coordinator.recoverRunningJobs(); err != nil {
		t.Fatalf("recover SQLite jobs: %v", err)
	}
	want := running
	want.State = JobRecovering
	got, ok, err := store.Load(running.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("SQLite recovering job: got=%+v found=%v error=%v, want %+v", got, ok, err, want)
	}
	for _, terminal := range legacyTerminals {
		got, ok, err := store.Load(terminal.record.JobID)
		if err != nil || !ok || got != terminal.record {
			t.Fatalf("legacy terminal job %d changed: got=%+v found=%v error=%v, want %+v",
				terminal.record.JobID, got, ok, err, terminal.record)
		}
	}
	if len(coordinator.jobAttempts) != 0 || len(coordinator.leases) != 0 {
		t.Fatalf("SQLite recovery allocated attempts or leases: attempts=%v leases=%v", coordinator.jobAttempts, coordinator.leases)
	}
}

func recoveringTestRecord() JobRecord {
	return JobRecord{
		JobID: 700, State: JobRecovering, AttemptID: 7, WorkerID: "old-worker",
		TaskType: "hash", Payload: "saved-input", TimeoutMs: 1500,
	}
}

func TestRetryRecoveringJobStartsOneNewAttemptAndReleasesOwnership(t *testing.T) {
	record := recoveringTestRecord()
	s := newTestCoordinator()
	if err := s.jobStore.Save(record); err != nil {
		t.Fatalf("seed recovering job: %v", err)
	}
	var calls atomic.Int32
	registerTestWorker(t, s, "recovery-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.JobId != record.JobID || req.AttemptId != 8 || req.TaskType != record.TaskType ||
			req.Payload != record.Payload || req.TimeoutMs != record.TimeoutMs {
			t.Errorf("recovery dispatch=%+v, want attempt 8 with original task specification", req)
		}
		return successfulResult(req), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.retryRecoveringJob(ctx, record); err != nil {
		t.Fatalf("retry recovering job: %v", err)
	}
	if calls.Load() != 1 {
		t.Fatalf("worker calls=%d, want 1", calls.Load())
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, "recovery-worker"
	got, ok, err := s.jobStore.Load(record.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("persisted job=%+v found=%v error=%v, want %+v", got, ok, err, want)
	}
	lease, ok := s.getLease(record.JobID)
	if !ok || lease.AttemptID != 8 || lease.WorkerID != "recovery-worker" || s.jobAttempts[record.JobID] != 8 {
		t.Fatalf("attempt was not exactly 8: lease=%+v found=%v memory=%d", lease, ok, s.jobAttempts[record.JobID])
	}
	if !s.claimJob(record.JobID) {
		t.Fatal("successful recovery did not release JobID ownership")
	}
	s.releaseJob(record.JobID)
}

func TestRetryRecoveringJobRejectsOwnershipConflict(t *testing.T) {
	record := recoveringTestRecord()
	s := newTestCoordinator()
	if err := s.jobStore.Save(record); err != nil {
		t.Fatalf("seed recovering job: %v", err)
	}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	if !s.claimJob(record.JobID) {
		t.Fatal("cannot pre-claim JobID")
	}
	defer s.releaseJob(record.JobID)
	if err := s.retryRecoveringJob(context.Background(), record); err == nil {
		t.Fatal("expected ownership conflict")
	}
	if s.claimJob(record.JobID) {
		t.Fatal("conflicting call released the original ownership claim")
	}
	got, ok, err := s.jobStore.Load(record.JobID)
	if err != nil || !ok || got != record || calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatalf("conflict changed job or dispatched: record=%+v found=%v error=%v calls=%d attempts=%v leases=%v",
			got, ok, err, calls.Load(), s.jobAttempts, s.leases)
	}
}

func TestRetryRecoveringJobReleasesOwnershipAfterInfrastructureFailure(t *testing.T) {
	record := recoveringTestRecord()
	s := newTestCoordinator()
	if err := s.jobStore.Save(record); err != nil {
		t.Fatalf("seed recovering job: %v", err)
	}
	var calls atomic.Int32
	registerTestWorker(t, s, "failing-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return nil, status.Error(codes.Unavailable, "worker infrastructure unavailable")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.retryRecoveringJob(ctx, record); status.Code(err) != codes.Unavailable {
		t.Fatalf("recovery error=%v, want Unavailable", err)
	}
	if calls.Load() != 2 || s.jobAttempts[record.JobID] != 9 {
		t.Fatalf("retry attempts: calls=%d attempt=%d, want two calls ending at 9", calls.Load(), s.jobAttempts[record.JobID])
	}
	want := record
	want.AttemptID, want.WorkerID = 9, "failing-worker"
	assertRecoveryRecord(t, s.jobStore, want)
	if !s.claimJob(record.JobID) {
		t.Fatal("failed recovery did not release JobID ownership")
	}
	s.releaseJob(record.JobID)
}

func TestRetryRecoveringJobRejectsInvalidStateWithoutDispatch(t *testing.T) {
	for _, tc := range []struct {
		name  string
		state JobState
	}{
		{name: "running", state: JobRunning},
		{name: "succeeded", state: JobSucceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := recoveringTestRecord()
			record.State = tc.state
			s := newTestCoordinator()
			if err := s.jobStore.Save(record); err != nil {
				t.Fatalf("seed job: %v", err)
			}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.retryRecoveringJob(context.Background(), record); err == nil {
				t.Fatal("expected invalid-state error")
			}
			got, ok, err := s.jobStore.Load(record.JobID)
			if err != nil || !ok || got != record || calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatalf("invalid state changed job or dispatched: record=%+v found=%v error=%v calls=%d",
					got, ok, err, calls.Load())
			}
			if !s.claimJob(record.JobID) {
				t.Fatal("invalid-state call retained JobID ownership")
			}
			s.releaseJob(record.JobID)
		})
	}
}

func TestRetryRecoveringJobCanceledParentReleasesOwnership(t *testing.T) {
	record := recoveringTestRecord()
	s := newTestCoordinator()
	if err := s.jobStore.Save(record); err != nil {
		t.Fatalf("seed recovering job: %v", err)
	}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := s.retryRecoveringJob(ctx, record); !errors.Is(err, context.Canceled) {
		t.Fatalf("recovery error=%v, want context.Canceled", err)
	}
	got, ok, err := s.jobStore.Load(record.JobID)
	if err != nil || !ok || got != record || calls.Load() != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatalf("canceled recovery changed job or dispatched: record=%+v found=%v error=%v calls=%d",
			got, ok, err, calls.Load())
	}
	if !s.claimJob(record.JobID) {
		t.Fatal("canceled recovery did not release JobID ownership")
	}
	s.releaseJob(record.JobID)
}
