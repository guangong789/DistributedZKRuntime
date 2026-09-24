package main

import (
	"context"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitJobPersistsTerminalStateAcrossSQLiteReopen(t *testing.T) {
	for _, tc := range []struct {
		name   string
		status string
		state  JobState
	}{
		{name: "succeeded", status: "Succeeded", state: JobSucceeded},
		{name: "failed", status: "Failed", state: JobFailed},
		{name: "cancelled", status: "Cancelled", state: JobCancelled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const jobID int64 = 601
			const workerID = "terminal-worker"
			dbPath := filepath.Join(t.TempDir(), "jobs.db")
			var accepted *runtimepb.SubmitJobResponse

			// Closing this scope closes store A and discards coordinator A.
			func() {
				storeA, err := NewSQLiteJobStore(dbPath)
				if err != nil {
					t.Fatalf("open first SQLite store: %v", err)
				}
				defer func() {
					if err := storeA.Close(); err != nil {
						t.Errorf("close first SQLite store: %v", err)
					}
				}()

				coordinatorA := &CoordinatorServer{jobStore: storeA}
				var calls atomic.Int32
				registerTestWorker(t, coordinatorA, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					if req.JobId != jobID || req.AttemptId != 1 {
						t.Errorf("unexpected worker request: %+v", req)
					}
					return &runtimepb.ExecuteJobResponse{
						JobId: req.JobId, AttemptId: req.AttemptId,
						Status: tc.status, Output: "output", Error: "task error",
					}, nil
				})

				ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
				defer cancel()
				accepted, err = coordinatorA.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID})
				if err != nil || accepted == nil || accepted.JobId != jobID ||
					accepted.AttemptId != 1 || accepted.Status != tc.status {
					t.Fatalf("SubmitJob response=%+v error=%v", accepted, err)
				}
				if calls.Load() != 1 || coordinatorA.jobAttempts[jobID] != 1 {
					t.Fatalf("terminal result retried: calls=%d attempts=%d", calls.Load(), coordinatorA.jobAttempts[jobID])
				}
				if !coordinatorA.claimJob(jobID) {
					t.Fatal("SubmitJob did not release job ownership")
				}
				coordinatorA.releaseJob(jobID)

				record, ok, err := storeA.Load(jobID)
				if err != nil || !ok {
					t.Fatalf("load before close: record=%+v found=%v error=%v", record, ok, err)
				}
				want := JobRecord{JobID: jobID, State: tc.state, AttemptID: accepted.AttemptId, WorkerID: workerID}
				if record != want {
					t.Fatalf("record before close=%+v, want %+v", record, want)
				}
			}()

			storeB, err := NewSQLiteJobStore(dbPath)
			if err != nil {
				t.Fatalf("reopen SQLite store: %v", err)
			}
			defer func() {
				if err := storeB.Close(); err != nil {
					t.Errorf("close reopened SQLite store: %v", err)
				}
			}()
			record, ok, err := storeB.Load(jobID)
			if err != nil || !ok {
				t.Fatalf("load after reopen: record=%+v found=%v error=%v", record, ok, err)
			}
			want := JobRecord{JobID: jobID, State: tc.state, AttemptID: accepted.AttemptId, WorkerID: workerID}
			if record != want {
				t.Fatalf("record after reopen=%+v, want %+v", record, want)
			}
		})
	}
}

func TestLateAttemptCannotOverwriteSQLiteTerminalState(t *testing.T) {
	const jobID int64 = 602
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	firstStarted := make(chan struct{})
	lateReturned := make(chan *runtimepb.ExecuteJobResponse, 1)
	allowFirstReturn := make(chan struct{})
	var releaseFirst sync.Once
	defer releaseFirst.Do(func() { close(allowFirstReturn) })
	dispatchDone := make(chan int64, 2)
	var firstCalls, secondCalls atomic.Int32
	coordinator := &CoordinatorServer{
		jobStore:      store,
		leaseDuration: 100 * time.Millisecond,
		onDispatchDone: func(_, attemptID int64) {
			dispatchDone <- attemptID
		},
	}
	registerTestWorker(t, coordinator, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		if req.JobId != jobID || req.AttemptId != 1 {
			t.Errorf("unexpected first attempt: %+v", req)
		}
		close(firstStarted)
		<-allowFirstReturn // Simulate remote work that ignores attempt cancellation.
		resp := successfulResult(req)
		lateReturned <- resp
		return resp, nil
	})
	registerTestWorker(t, coordinator, "second", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		return successfulResult(req), nil
	})

	type submissionResult struct {
		resp *runtimepb.SubmitJobResponse
		err  error
	}
	submitted := make(chan submissionResult, 1)
	go func() {
		resp, err := coordinator.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID})
		submitted <- submissionResult{resp: resp, err: err}
	}()
	select {
	case <-firstStarted:
	case <-ctx.Done():
		t.Fatal("first attempt did not start")
	}

	var accepted *runtimepb.SubmitJobResponse
	select {
	case result := <-submitted:
		accepted = result.resp
		if result.err != nil || accepted == nil || accepted.Status != "Succeeded" || accepted.AttemptId != 2 {
			t.Fatalf("accepted response=%+v error=%v", accepted, result.err)
		}
	case <-ctx.Done():
		t.Fatal("retry did not complete")
	}
	want := JobRecord{JobID: jobID, State: JobSucceeded, AttemptID: 2, WorkerID: "second"}
	record, ok, err := store.Load(jobID)
	if err != nil || !ok || record != want {
		t.Fatalf("accepted record=%+v found=%v error=%v, want %+v", record, ok, err, want)
	}

	// Both dispatch goroutines finish before the old worker sends its late success.
	completed := make(map[int64]bool)
	for i := 0; i < 2; i++ {
		select {
		case attemptID := <-dispatchDone:
			if attemptID < 1 || attemptID > 2 || completed[attemptID] {
				t.Fatalf("unexpected dispatch completion for attempt %d", attemptID)
			}
			completed[attemptID] = true
		case <-ctx.Done():
			t.Fatal("dispatch goroutine did not finish")
		}
	}
	if coordinator.isLeaseCurrent(jobID, 1) {
		t.Fatal("old attempt still passes lease fencing")
	}
	releaseFirst.Do(func() { close(allowFirstReturn) })
	select {
	case late := <-lateReturned:
		if late.JobId != jobID || late.AttemptId != 1 || late.Status != "Succeeded" {
			t.Fatalf("late response=%+v, want successful attempt 1", late)
		}
	case <-ctx.Done():
		t.Fatal("first worker did not return its late success")
	}
	record, ok, err = store.Load(jobID)
	if err != nil || !ok || record != want {
		t.Fatalf("late success changed record: got=%+v found=%v error=%v", record, ok, err)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || coordinator.jobAttempts[jobID] != 2 {
		t.Fatalf("unexpected attempts: first=%d second=%d generation=%d", firstCalls.Load(), secondCalls.Load(), coordinator.jobAttempts[jobID])
	}
	if !coordinator.claimJob(jobID) {
		t.Fatal("SubmitJob did not release job ownership")
	}
	coordinator.releaseJob(jobID)

	if err := store.Close(); err != nil {
		t.Fatalf("close first SQLite store: %v", err)
	}
	closed = true
	reopened, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("reopen SQLite store: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened SQLite store: %v", err)
		}
	}()
	record, ok, err = reopened.Load(jobID)
	if err != nil || !ok || record != want {
		t.Fatalf("reopened record=%+v found=%v error=%v, want %+v", record, ok, err, want)
	}
}

func TestSubmitJobDoesNotReportSuccessWhenSQLiteTerminalSaveFails(t *testing.T) {
	const jobID int64 = 603
	const workerID = "terminal-worker"
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	closed := false
	defer func() {
		if !closed {
			_ = store.Close()
		}
	}()

	coordinator := &CoordinatorServer{jobStore: store}
	workerStarted := make(chan struct{})
	allowResult := make(chan struct{})
	var releaseResult sync.Once
	defer releaseResult.Do(func() { close(allowResult) })
	var calls atomic.Int32
	registerTestWorker(t, coordinator, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		close(workerStarted)
		<-allowResult
		return successfulResult(req), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	type submissionResult struct {
		resp *runtimepb.SubmitJobResponse
		err  error
	}
	submitted := make(chan submissionResult, 1)
	go func() {
		resp, err := coordinator.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID})
		submitted <- submissionResult{resp: resp, err: err}
	}()
	select {
	case <-workerStarted: // startAttempt has already persisted JobRunning.
	case <-ctx.Done():
		t.Fatal("worker did not receive attempt")
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close SQLite store before terminal save: %v", err)
	}
	closed = true
	releaseResult.Do(func() { close(allowResult) })
	select {
	case result := <-submitted:
		if result.resp != nil || status.Code(result.err) != codes.Internal {
			t.Fatalf("terminal save failure response=%+v error=%v", result.resp, result.err)
		}
	case <-ctx.Done():
		t.Fatal("SubmitJob did not return after terminal save failed")
	}
	if calls.Load() != 1 || coordinator.jobAttempts[jobID] != 1 {
		t.Fatalf("unexpected retry after terminal save failure: calls=%d attempt=%d", calls.Load(), coordinator.jobAttempts[jobID])
	}
	if !coordinator.claimJob(jobID) {
		t.Fatal("SubmitJob did not release job ownership")
	}
	coordinator.releaseJob(jobID)

	reopened, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("reopen SQLite store: %v", err)
	}
	defer func() {
		if err := reopened.Close(); err != nil {
			t.Errorf("close reopened SQLite store: %v", err)
		}
	}()
	record, ok, err := reopened.Load(jobID)
	want := JobRecord{JobID: jobID, State: JobRunning, AttemptID: 1, WorkerID: workerID}
	if err != nil || !ok || record != want {
		t.Fatalf("record after failed terminal save=%+v found=%v error=%v, want %+v", record, ok, err, want)
	}
}

func TestMismatchedWorkerResultDoesNotPersistSQLiteTerminalState(t *testing.T) {
	const jobID int64 = 604
	const workerID = "mismatched-worker"
	store, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	defer func() {
		if err := store.Close(); err != nil {
			t.Errorf("close SQLite store: %v", err)
		}
	}()

	coordinator := &CoordinatorServer{jobStore: store}
	var calls atomic.Int32
	registerTestWorker(t, coordinator, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		resp := successfulResult(req)
		resp.AttemptId = 0 // A successful status with an invalid fence must be rejected.
		return resp, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := coordinator.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID})
	if resp != nil || status.Code(err) != codes.Unavailable {
		t.Fatalf("mismatched results response=%+v error=%v", resp, err)
	}
	if calls.Load() != 2 || coordinator.jobAttempts[jobID] != 2 {
		t.Fatalf("unexpected attempts: calls=%d generation=%d", calls.Load(), coordinator.jobAttempts[jobID])
	}
	record, ok, err := store.Load(jobID)
	want := JobRecord{JobID: jobID, State: JobRunning, AttemptID: 2, WorkerID: workerID}
	if err != nil || !ok || record != want {
		t.Fatalf("mismatched result persisted terminal state: record=%+v found=%v error=%v, want %+v", record, ok, err, want)
	}
	if !coordinator.claimJob(jobID) {
		t.Fatal("SubmitJob did not release job ownership")
	}
	coordinator.releaseJob(jobID)
}
