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

func TestSubmitRetryPreservesPersistedTaskSpecification(t *testing.T) {
	const jobID int64 = 605
	request := &runtimepb.SubmitJobRequest{
		JobId: jobID, TaskType: "hash", Payload: "retry-input", TimeoutMs: 1500,
	}
	store := NewMemoryJobStore()
	coordinator := &CoordinatorServer{jobStore: store}
	var firstCalls, secondCalls atomic.Int32

	assertRunning := func(req *runtimepb.ExecuteJobRequest, attemptID int64, workerID string) {
		t.Helper()
		if req.JobId != jobID || req.AttemptId != attemptID ||
			req.TaskType != request.TaskType || req.Payload != request.Payload ||
			req.TimeoutMs != request.TimeoutMs {
			t.Errorf("attempt %d received incorrect task: %+v", attemptID, req)
		}
		got, ok, err := store.Load(jobID)
		want := JobRecord{
			JobID: jobID, State: JobRunning, AttemptID: attemptID, WorkerID: workerID,
			TaskType: request.TaskType, Payload: request.Payload, TimeoutMs: request.TimeoutMs,
		}
		if err != nil || !ok || got != want {
			t.Errorf("attempt %d running record=%+v found=%v error=%v, want %+v",
				attemptID, got, ok, err, want)
		}
	}
	registerTestWorker(t, coordinator, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		assertRunning(req, 1, "first")
		return nil, status.Error(codes.Unavailable, "worker infrastructure failure")
	})
	registerTestWorker(t, coordinator, "second", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		assertRunning(req, 2, "second")
		return successfulResult(req), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := coordinator.SubmitJob(ctx, request)
	if err != nil || resp == nil || resp.JobId != jobID ||
		resp.AttemptId != 2 || resp.Status != "Succeeded" {
		t.Fatalf("retry response=%+v error=%v", resp, err)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || coordinator.jobAttempts[jobID] != 2 {
		t.Fatalf("unexpected retries: first=%d second=%d generation=%d",
			firstCalls.Load(), secondCalls.Load(), coordinator.jobAttempts[jobID])
	}
	got, ok, err := store.Load(jobID)
	want := JobRecord{
		JobID: jobID, State: JobSucceeded, AttemptID: 2, WorkerID: "second",
		TaskType: request.TaskType, Payload: request.Payload, TimeoutMs: request.TimeoutMs,
	}
	if err != nil || !ok || got != want {
		t.Fatalf("terminal record=%+v found=%v error=%v, want %+v", got, ok, err, want)
	}
}
