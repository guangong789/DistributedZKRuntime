package main

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitCancellationPersistsCurrentAttempt(t *testing.T) {
	for _, tc := range []struct {
		name     string
		deadline bool
		retry    bool
		fail     string
	}{
		{name: "in-flight cancellation"},
		{name: "in-flight deadline", deadline: true},
		{name: "cancellation after retry", retry: true},
		{name: "cancellation Save failure", fail: "save"},
		{name: "cancellation Load failure", fail: "load"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const jobID int64 = 9401
			req := &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "cancelled-session", TimeoutMs: 1000}
			store := newWorkerFailureStore(t)
			observer := &recordingMetrics{}
			persistenceErr := errors.New("injected cancellation persistence failure")
			var failLoad atomic.Bool
			s := &CoordinatorServer{metrics: observer, jobStore: &recoveryPassStore{
				JobStore: store,
				load: func(id int64) (JobRecord, bool, error) {
					if failLoad.Load() {
						return JobRecord{}, false, persistenceErr
					}
					return store.Load(id)
				},
				save: func(record JobRecord) error {
					if tc.fail == "save" && record.State == JobCancelled {
						return persistenceErr
					}
					return store.Save(record)
				},
			}}
			observer.hook = func(event recordedMetric) {
				if event.name == "terminal" {
					// Both durability and lock release must precede the event.
					s.getLease(jobID)
					if event.source != metrics.SourceSubmit {
						t.Error("submit cancellation reported the wrong execution source")
					}
					if s.claimJob(jobID) {
						s.releaseJob(jobID)
						t.Error("submit released ownership before cancellation finalization")
					}
					record, ok, err := store.Load(jobID)
					if err != nil || !ok || record.State != JobCancelled {
						t.Error("Cancelled terminal metric preceded durable cancellation")
					}
				}
			}
			started := make(chan int64, 1)
			lateReturned := make(chan struct{})
			releaseWorker := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseWorker) }) }
			var calls atomic.Int32
			workerID := "blocked-worker"
			if tc.retry {
				registerTestWorker(t, s, "failed-worker", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
					calls.Add(1)
					return nil, status.Error(codes.Unavailable, "injected first-attempt failure")
				})
			}
			registerTestWorker(t, s, workerID, func(ctx context.Context, request *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				started <- request.AttemptId
				<-ctx.Done()
				// Produce a late success only after the submit session has ended.
				<-releaseWorker
				defer close(lateReturned)
				return successfulResult(request), nil
			})
			parent, cancel := context.WithCancel(context.Background())
			if tc.deadline {
				cancel()
				parent, cancel = context.WithTimeout(context.Background(), 250*time.Millisecond)
			}
			waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopWaiting()
			finished := make(chan struct{})
			var result workerFailureResult
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("submit did not stop during cleanup")
				}
			})
			go func() {
				defer close(finished)
				result.resp, result.err = s.SubmitJob(parent, req)
			}()
			attempt := int64(1)
			if tc.retry {
				attempt = 2
			}
			select {
			case got := <-started:
				if got != attempt {
					t.Fatalf("in-flight attempt=%d, want %d", got, attempt)
				}
			case <-waitCtx.Done():
				t.Fatal("worker did not receive the request")
			}
			want := JobRecord{JobID: jobID, State: JobRunning, AttemptID: attempt, WorkerID: workerID,
				TaskType: req.TaskType, Payload: req.Payload, TimeoutMs: req.TimeoutMs}
			assertRecoveryRecord(t, store, want)
			if parent.Err() != nil {
				t.Fatal("parent must still be live when the running attempt is observed")
			}
			if tc.fail == "load" {
				failLoad.Store(true)
			}
			if tc.deadline {
				<-parent.Done()
			} else {
				cancel()
			}
			select {
			case <-finished:
			case <-waitCtx.Done():
				t.Fatal("cancelled submit did not finish")
			}
			wantErr := context.Canceled
			if tc.deadline {
				wantErr = context.DeadlineExceeded
			}
			if result.resp != nil || !errors.Is(result.err, wantErr) {
				t.Errorf("cancelled submission: response=%v error=%v", result.resp, result.err)
			}
			if tc.fail != "" && !errors.Is(result.err, persistenceErr) {
				t.Error("cancellation persistence failure was not surfaced")
			}
			wantTerminals := 0
			if tc.fail == "" {
				want.State = JobCancelled
				wantTerminals = 1
			}
			assertRecoveryRecord(t, store, want)
			if s.jobAttempts[jobID] != attempt || int64(calls.Load()) != attempt {
				t.Error("cancellation created another attempt or dispatch")
			}
			assertRecoveryOwnershipReleased(t, s, jobID)
			if observer.count("submitted", "") != 1 || observer.count("started", "") != int(attempt) ||
				observer.count("finished", "") != int(attempt) || observer.count("finished", string(metrics.AttemptCancelled)) != 1 ||
				observer.count("terminal", "") != wantTerminals ||
				observer.count("terminal", string(metrics.TerminalCancelled)) != wantTerminals {
				t.Errorf("cancellation lifecycle metrics=%+v", observer.snapshot())
			}
			release()
			select {
			case <-lateReturned:
			case <-waitCtx.Done():
				t.Fatal("old worker did not return its late success")
			}
			assertRecoveryRecord(t, store, want)
			if observer.count("finished", "") != int(attempt) || observer.count("terminal", "") != wantTerminals {
				t.Error("late success changed lifecycle counts")
			}
			if tc.fail == "" {
				fresh := &CoordinatorServer{jobStore: store}
				if err := fresh.recoverRunningJobs(); err != nil {
					t.Fatal(err)
				}
				if err := fresh.runRecoveryPass(context.Background()); err != nil {
					t.Fatal(err)
				}
				assertRecoveryRecord(t, store, want)
				if len(fresh.jobAttempts) != 0 || len(fresh.leases) != 0 {
					t.Error("startup recovery recreated work for the durably Cancelled job")
				}
			}
			t.Logf("final state=%v, attempt=%d, parent error=%v, persistence failure=%q", want.State, attempt, parent.Err(), tc.fail)
		})
	}
}

func TestPersistSubmitCancellationProtectsCurrentDurableState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   JobState
		attempt int64
	}{
		{"current Running", JobRunning, 7},
		{"Succeeded", JobSucceeded, 7},
		{"Failed", JobFailed, 7},
		{"Cancelled", JobCancelled, 7},
		{"Recovering", JobRecovering, 7},
		{"older generation", JobRunning, 6},
		{"newer generation", JobRunning, 8},
		{"no published attempt in session", JobRunning, 7},
		{"ownership released", JobRunning, 7},
		{"missing record", JobRunning, 7},
	} {
		t.Run(tc.name, func(t *testing.T) {
			const jobID int64 = 9404
			inner := NewMemoryJobStore()
			record := JobRecord{JobID: jobID, State: tc.state, AttemptID: tc.attempt, WorkerID: "worker",
				TaskType: "hash", Payload: "persisted-task", TimeoutMs: 1234}
			if tc.name != "missing record" {
				saveRecoveryRecord(t, inner, record)
			}
			saves := 0
			s := &CoordinatorServer{jobAttempts: map[int64]int64{jobID: 7}, jobStore: &recoveryPassStore{
				JobStore: inner, save: func(got JobRecord) error {
					saves++
					return inner.Save(got)
				},
			}}
			if !s.claimJob(jobID) {
				t.Fatal("cannot claim job")
			}
			defer s.releaseJob(jobID)
			initialAttempt := int64(6)
			if tc.name == "no published attempt in session" {
				initialAttempt = 7
			}
			if tc.name == "ownership released" {
				s.releaseJob(jobID)
			}
			persisted, err := s.persistSubmitCancellation(jobID, initialAttempt)
			wantPersisted := tc.name == "current Running"
			if err != nil || persisted != wantPersisted {
				t.Fatalf("cancellation persistence=%v error=%v, want %v", persisted, err, wantPersisted)
			}
			if wantPersisted {
				record.State = JobCancelled
				if saves != 1 {
					t.Errorf("cancellation Save calls=%d, want 1", saves)
				}
			} else if saves != 0 {
				t.Error("ineligible cancellation overwrote a durable record")
			}
			if tc.name == "missing record" {
				if _, found, err := inner.Load(jobID); err != nil || found {
					t.Error("cancellation fabricated a missing record")
				}
			} else {
				assertRecoveryRecord(t, inner, record)
			}
		})
	}
}

func TestSubmitCancellationBeforeAttemptPreservesExistingRecord(t *testing.T) {
	for _, afterClaim := range []bool{false, true} {
		t.Run(map[bool]string{false: "before Submit", true: "after claim before execution"}[afterClaim], func(t *testing.T) {
			s := newTestCoordinator()
			const jobID int64 = 9402
			record := JobRecord{JobID: jobID, State: JobRunning, AttemptID: 7, WorkerID: "old-worker",
				TaskType: "hash", Payload: "old-task", TimeoutMs: 1000}
			saveRecoveryRecord(t, s.jobStore, record)
			s.jobAttempts = map[int64]int64{jobID: record.AttemptID}
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			observer := &recordingMetrics{}
			s.metrics = observer
			observer.hook = func(event recordedMetric) {
				if afterClaim && event.name == "submitted" {
					cancel()
				}
			}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if !afterClaim {
				cancel()
			}
			resp, err := s.SubmitJob(parent, &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "new-task", TimeoutMs: 1000})
			if resp != nil || !errors.Is(err, context.Canceled) || calls.Load() != 0 ||
				s.jobAttempts[jobID] != 7 || len(s.leases) != 0 {
				t.Fatalf("pre-attempt cancellation created work: response=%v error=%v", resp, err)
			}
			assertRecoveryRecord(t, s.jobStore, record)
			assertRecoveryOwnershipReleased(t, s, jobID)
			if observer.count("started", "") != 0 || observer.count("finished", "") != 0 || observer.count("terminal", "") != 0 {
				t.Error("pre-attempt cancellation fabricated lifecycle events")
			}
		})
	}
}

func TestSubmitCancellationAfterCommittedTerminalPreservesResult(t *testing.T) {
	for _, tc := range []struct {
		status string
		state  JobState
	}{
		{"Succeeded", JobSucceeded}, {"Failed", JobFailed}, {"Cancelled", JobCancelled},
	} {
		t.Run(tc.status, func(t *testing.T) {
			const jobID int64 = 9403
			store := newWorkerFailureStore(t)
			parent, cancel := context.WithCancel(context.Background())
			defer cancel()
			observer := &recordingMetrics{}
			s := &CoordinatorServer{metrics: observer, jobStore: &recoveryPassStore{JobStore: store, save: func(record JobRecord) error {
				if err := store.Save(record); err != nil {
					return err
				}
				if record.State != JobRunning {
					cancel() // Durable commit has completed before cancellation.
				}
				return nil
			}}}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				resp := successfulResult(req)
				resp.Status = tc.status
				return resp, nil
			})
			resp, err := s.SubmitJob(parent, &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "terminal-task", TimeoutMs: 1000})
			if err != nil || resp == nil || resp.Status != tc.status || parent.Err() == nil {
				t.Fatalf("accepted terminal changed: response=%v error=%v parent=%v", resp, err, parent.Err())
			}
			want := JobRecord{JobID: jobID, State: tc.state, AttemptID: 1, WorkerID: "worker",
				TaskType: "hash", Payload: "terminal-task", TimeoutMs: 1000}
			assertRecoveryRecord(t, store, want)
			if calls.Load() != 1 || len(store.savedRecords()) != 2 || observer.count("terminal", "") != 1 ||
				observer.count("finished", "") != 1 || observer.count("started", "") != 1 {
				t.Error("post-commit cancellation wrote or counted another terminal outcome")
			}
			assertRecoveryOwnershipReleased(t, s, jobID)
		})
	}
}
