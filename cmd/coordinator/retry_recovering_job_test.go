package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

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
