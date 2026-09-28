package main

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestJobSpecConversions(t *testing.T) {
	want := JobSpec{JobID: 700, TaskType: "hash", Payload: "saved-input", TimeoutMs: 1500}
	request := &runtimepb.SubmitJobRequest{
		JobId: 700, TaskType: "hash", Payload: "saved-input", TimeoutMs: 1500,
	}
	record := JobRecord{
		JobID: 700, State: JobRecovering, AttemptID: 7, WorkerID: "old-worker",
		TaskType: "hash", Payload: "saved-input", TimeoutMs: 1500,
	}
	fromRequest, fromRecord := jobSpecFromRequest(request), jobSpecFromRecord(record)
	// The execution inputs remain snapshots when their source values change.
	request.JobId, request.TaskType, request.Payload, request.TimeoutMs = 900, "sleep", "50ms", 500
	record.JobID, record.TaskType, record.Payload, record.TimeoutMs = 901, "sleep", "100ms", 600
	for name, got := range map[string]JobSpec{"request": fromRequest, "recovering record": fromRecord} {
		t.Run(name, func(t *testing.T) {
			if got != want {
				t.Fatalf("spec=%+v, want %+v", got, want)
			}
		})
	}
}

func TestExecuteJobFromRecordKeepsCallerOwnership(t *testing.T) {
	record := JobRecord{
		JobID: 700, State: JobRecovering, AttemptID: 7, WorkerID: "old-worker",
		TaskType: "hash", Payload: "saved-input", TimeoutMs: 1500,
	}
	store := NewMemoryJobStore()
	if err := store.Save(record); err != nil {
		t.Fatalf("seed record: %v", err)
	}
	s := &CoordinatorServer{jobStore: store}
	var calls atomic.Int32
	registerTestWorker(t, s, "new-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.JobId != record.JobID || req.AttemptId != 8 || req.TaskType != record.TaskType ||
			req.Payload != record.Payload || req.TimeoutMs != record.TimeoutMs {
			t.Errorf("incorrect dispatch from saved spec: %+v", req)
		}
		return successfulResult(req), nil
	})
	if !s.claimJob(record.JobID) {
		t.Fatal("cannot claim job")
	}
	defer s.releaseJob(record.JobID)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	// Explicit invocation under caller ownership, independent of the startup waiter.
	resp, err := s.executeJob(ctx, jobSpecFromRecord(record))
	if err != nil || resp == nil || resp.JobId != record.JobID || resp.AttemptId != 8 || resp.Status != "Succeeded" {
		t.Fatalf("executeJob response=%+v error=%v", resp, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("worker calls=%d, want 1", calls.Load())
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 8, "new-worker"
	got, ok, err := store.Load(record.JobID)
	if err != nil || !ok || got != want {
		t.Fatalf("terminal record=%+v found=%v error=%v, want %+v", got, ok, err, want)
	}
	if s.claimJob(record.JobID) {
		t.Fatal("executeJob released ownership belonging to its caller")
	}
}

func TestSubmitJobRejectsAlreadyOwnedJob(t *testing.T) {
	s := newTestCoordinator()
	if !s.claimJob(700) {
		t.Fatal("cannot claim job")
	}
	defer s.releaseJob(700)
	resp, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{
		JobId: 700, TaskType: "hash", Payload: "duplicate", TimeoutMs: 1500,
	})
	if resp != nil || status.Code(err) != codes.AlreadyExists {
		t.Fatalf("duplicate submission response=%+v error=%v", resp, err)
	}
	if s.claimJob(700) {
		t.Fatal("rejected submission released the existing owner's claim")
	}
	if len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("rejected submission created an attempt or lease")
	}
}
