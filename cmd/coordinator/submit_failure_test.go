package main

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitInfrastructureFailurePersistsAcrossRestart(t *testing.T) {
	const jobID int64 = 9501
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	history := &workerFailureStore{JobStore: store}
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: history, metrics: observer}
	var calls atomic.Int32
	for _, id := range []string{"first", "second"} {
		registerTestWorker(t, s, id, func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			calls.Add(1)
			return nil, status.Error(codes.Unavailable, "injected transport failure")
		})
	}
	observer.hook = func(event recordedMetric) {
		if event.name == "terminal" {
			s.getLease(jobID) // Metrics must run outside the coordinator lock.
			if s.claimJob(jobID) {
				s.releaseJob(jobID)
				t.Error("ownership released before failure finalization")
			}
		}
	}
	req := &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "persisted-spec", TimeoutMs: 1234}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.SubmitJob(ctx, req)
	if resp != nil || status.Code(err) != codes.Unavailable || calls.Load() != 2 {
		t.Fatalf("failed session: response=%v error=%v calls=%d", resp, err, calls.Load())
	}
	want := JobRecord{JobID: jobID, State: JobFailed, AttemptID: 2, WorkerID: "second",
		TaskType: req.TaskType, Payload: req.Payload, TimeoutMs: req.TimeoutMs}
	assertRecoveryRecord(t, history, want)
	running1, running2 := want, want
	running1.State, running1.AttemptID, running1.WorkerID = JobRunning, 1, "first"
	running2.State = JobRunning
	assertWorkerFailureSaves(t, history, []JobRecord{running1, running2, want})
	if observer.count("terminal", "") != 1 || observer.count("terminal", string(metrics.TerminalFailed)) != 1 ||
		observer.count("finished", "") != 2 || observer.count("finished", string(metrics.AttemptRPCError)) != 2 {
		t.Fatalf("failure metrics=%+v", observer.snapshot())
	}
	assertRecoveryOwnershipReleased(t, s, jobID)
	if err := s.runRecoveryPass(ctx); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || s.jobAttempts[jobID] != 2 {
		t.Fatal("failed submission was retried by online recovery")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	s = nil // The new coordinator retains only the reopened database.
	reopened, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reopened.Close() })
	fresh := &CoordinatorServer{jobStore: reopened}
	var freshCalls atomic.Int32
	registerTestWorker(t, fresh, "after-restart", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		freshCalls.Add(1)
		if req.AttemptId != 3 || req.TaskType != want.TaskType || req.Payload != want.Payload || req.TimeoutMs != want.TimeoutMs {
			t.Errorf("resubmission lost generation/spec: %+v", req)
		}
		return successfulResult(req), nil
	})
	if err := fresh.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	if err := fresh.runRecoveryPass(ctx); err != nil {
		t.Fatal(err)
	}
	assertRecoveryRecord(t, reopened, want)
	if freshCalls.Load() != 0 || len(fresh.jobAttempts) != 0 || len(fresh.leases) != 0 {
		t.Fatal("restart recovered a finalized failure")
	}
	resp, err = fresh.SubmitJob(ctx, req)
	if err != nil || resp.GetStatus() != "Succeeded" || resp.GetAttemptId() != 3 || freshCalls.Load() != 1 {
		t.Fatalf("explicit resubmission: response=%v error=%v", resp, err)
	}
	want.State, want.AttemptID, want.WorkerID = JobSucceeded, 3, "after-restart"
	assertRecoveryRecord(t, reopened, want)
	assertRecoveryOwnershipReleased(t, fresh, jobID)
}

func TestSubmitFailureWithoutNextWorker(t *testing.T) {
	s := newTestCoordinator()
	var calls atomic.Int32
	registerTestWorker(t, s, "only-worker", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		s.mu.Lock()
		worker := s.workers["only-worker"]
		worker.Status = WorkerDead
		s.workers[worker.ID] = worker
		s.mu.Unlock()
		return nil, status.Error(codes.Unavailable, "worker lost")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 9502, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	if resp != nil || status.Code(err) != codes.Unavailable || calls.Load() != 1 || s.jobAttempts[9502] != 1 {
		t.Fatalf("no backup: response=%v error=%v calls=%d", resp, err, calls.Load())
	}
	assertRecoveryRecord(t, s.jobStore, JobRecord{JobID: 9502, State: JobFailed, AttemptID: 1,
		WorkerID: "only-worker", TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	assertRecoveryOwnershipReleased(t, s, 9502)
}

func TestPersistSubmitFailureGuards(t *testing.T) {
	for _, name := range []string{"matching expired lease", "succeeded", "failed", "cancelled", "recovering", "newer durable generation", "older durable generation", "newer memory generation", "older memory generation", "no session attempt", "zero expected attempt", "missing record", "wrong record JobID", "no ownership", "nil store", "cancelled parent", "expired parent"} {
		t.Run(name, func(t *testing.T) {
			want := JobRecord{JobID: 9503, State: JobRunning, AttemptID: 8, WorkerID: "worker",
				TaskType: "zk_preimage_prove", Payload: preimagePublicPayload(zk.ComputePreimageDigest(42)), TimeoutMs: 1234}
			initial, expected, memory := int64(7), int64(8), int64(8)
			switch name {
			case "succeeded":
				want.State = JobSucceeded
			case "failed":
				want.State = JobFailed
			case "cancelled":
				want.State = JobCancelled
			case "recovering":
				want.State = JobRecovering
			case "newer durable generation":
				want.AttemptID = 9
			case "older durable generation":
				want.AttemptID = 7
			case "newer memory generation":
				memory = 9
			case "older memory generation":
				memory = 7
			case "no session attempt":
				initial = 8
			case "zero expected attempt":
				expected = 0
			}
			inner := NewMemoryJobStore()
			if name != "missing record" {
				saveRecoveryRecord(t, inner, want)
			}
			saves := 0
			store := &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
				saves++
				return inner.Save(record)
			}}
			if name == "wrong record JobID" {
				store.load = func(int64) (JobRecord, bool, error) { wrong := want; wrong.JobID++; return wrong, true, nil }
			}
			s := &CoordinatorServer{jobStore: store, jobAttempts: map[int64]int64{want.JobID: memory}}
			s.claimJob(want.JobID)
			defer s.releaseJob(want.JobID)
			s.setLease(want.JobID, memory, want.WorkerID, -time.Second)
			if name == "no ownership" {
				s.releaseJob(want.JobID)
			}
			if name == "nil store" {
				s.jobStore = nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if name == "cancelled parent" {
				cancel()
			} else if name == "expired parent" {
				cancel()
				ctx, cancel = context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
				defer cancel()
			}
			persisted, err := s.persistSubmitFailure(ctx, want.JobID, initial, expected)
			if name == "cancelled parent" {
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("context error=%v", err)
				}
			} else if name == "expired parent" {
				if !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("context error=%v", err)
				}
			} else if err != nil {
				t.Fatal(err)
			}
			shouldSave := name == "matching expired lease"
			wantSaves := 0
			if shouldSave {
				wantSaves = 1
			}
			if persisted != shouldSave || saves != wantSaves {
				t.Fatalf("persisted=%t saves=%d", persisted, saves)
			}
			if shouldSave {
				want.State = JobFailed
			}
			if name == "missing record" {
				if _, found, err := inner.Load(want.JobID); found || err != nil {
					t.Fatal("created missing record")
				}
			} else {
				assertRecoveryRecord(t, inner, want)
			}
		})
	}
}

func TestSubmitFailureCancellationAndStoreErrors(t *testing.T) {
	for _, name := range []string{"cancel before finalization", "cancel during finalization Load", "cancel after Failed commit", "Load error", "Save error", "uncertain Save error"} {
		t.Run(name, func(t *testing.T) {
			const jobID int64 = 9504
			inner := NewMemoryJobStore()
			observer := &recordingMetrics{}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			var finalizing atomic.Bool
			storeErr := errors.New("injected finalization storage error")
			store := &recoveryPassStore{JobStore: inner,
				load: func(id int64) (JobRecord, bool, error) {
					if finalizing.Load() && name == "Load error" {
						return JobRecord{}, false, storeErr
					}
					if finalizing.Load() && name == "cancel during finalization Load" {
						cancel()
					}
					return inner.Load(id)
				},
				save: func(record JobRecord) error {
					if record.State == JobFailed && name == "Save error" {
						return storeErr
					}
					if err := inner.Save(record); err != nil {
						return err
					}
					if record.State == JobFailed {
						if name == "cancel after Failed commit" {
							cancel()
						}
						if name == "uncertain Save error" {
							return storeErr
						}
					}
					return nil
				}}
			s := &CoordinatorServer{jobStore: store, metrics: observer}
			observer.hook = func(event recordedMetric) {
				if event.name == "finished" && observer.count("finished", "") == 2 {
					finalizing.Store(true)
					if name == "cancel before finalization" {
						cancel()
					}
				}
				if event.name == "terminal" {
					if s.claimJob(jobID) {
						s.releaseJob(jobID)
						t.Error("ownership released before terminal event")
					}
				}
			}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return nil, status.Error(codes.Unavailable, "injected RPC failure")
			})
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "hello", TimeoutMs: 1234})
			wantState, wantTerminal := JobFailed, metrics.TerminalFailed
			wantTerminals := 1
			switch name {
			case "cancel before finalization", "cancel during finalization Load":
				wantState, wantTerminal = JobCancelled, metrics.TerminalCancelled
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("cancelled session error=%v", err)
				}
			case "Load error", "Save error", "uncertain Save error":
				wantTerminals = 0
				if name != "uncertain Save error" {
					wantState = JobRunning
				}
				if status.Code(err) != codes.Internal || !strings.Contains(err.Error(), storeErr.Error()) || !strings.Contains(err.Error(), "injected RPC failure") {
					t.Fatalf("finalization error=%v", err)
				}
			default:
				if status.Code(err) != codes.Unavailable || ctx.Err() == nil {
					t.Fatalf("post-commit result=%v parent=%v", err, ctx.Err())
				}
			}
			if resp != nil || calls.Load() != 2 || s.jobAttempts[jobID] != 2 {
				t.Fatalf("response=%v calls=%d", resp, calls.Load())
			}
			assertRecoveryRecord(t, inner, JobRecord{JobID: jobID, State: wantState, AttemptID: 2, WorkerID: "worker", TaskType: "hash", Payload: "hello", TimeoutMs: 1234})
			if observer.count("terminal", "") != wantTerminals || observer.count("terminal", string(wantTerminal)) != wantTerminals || observer.count("finished", "") != 2 {
				t.Fatalf("metrics=%+v", observer.snapshot())
			}
			assertRecoveryOwnershipReleased(t, s, jobID)
		})
	}
}

func TestSubmitFailureUsesExactSessionGeneration(t *testing.T) {
	s := newTestCoordinator()
	for _, id := range []string{"first", "second"} {
		registerTestWorker(t, s, id, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			if req.AttemptId == 2 {
				if attempt, err := s.startAttempt(req.JobId, "newer-publisher", time.Minute, "hash", "newer-spec", 2000); err != nil || attempt != 3 {
					t.Errorf("publish superseding generation: attempt=%d error=%v", attempt, err)
				}
			}
			return nil, status.Error(codes.Unavailable, "injected failure")
		})
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 9505, TaskType: "hash", Payload: "original-spec", TimeoutMs: 1000})
	var exhausted *retryExhaustionError
	if resp != nil || status.Code(err) != codes.Unavailable || !errors.As(err, &exhausted) || exhausted.attemptID != 2 {
		t.Fatalf("session provenance: response=%v error=%v", resp, err)
	}
	assertRecoveryRecord(t, s.jobStore, JobRecord{JobID: 9505, State: JobRunning, AttemptID: 3, WorkerID: "newer-publisher", TaskType: "hash", Payload: "newer-spec", TimeoutMs: 2000})
	assertRecoveryOwnershipReleased(t, s, 9505)
}

func TestSubmitWithoutWorkerPreservesExistingRecord(t *testing.T) {
	for _, state := range []JobState{JobRunning, JobRecovering, JobSucceeded, JobFailed, JobCancelled} {
		t.Run(fmt.Sprintf("state %d", state), func(t *testing.T) {
			store := NewMemoryJobStore()
			want := JobRecord{JobID: 9506, State: state, AttemptID: 7, WorkerID: "old-worker", TaskType: "hash", Payload: "old-spec", TimeoutMs: 1000}
			saveRecoveryRecord(t, store, want)
			s := &CoordinatorServer{jobStore: store}
			resp, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: want.JobID, TaskType: "hash", Payload: "new-spec", TimeoutMs: 2000})
			if resp != nil || status.Code(err) != codes.Unavailable || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
				t.Fatalf("no-worker result=%v error=%v", resp, err)
			}
			assertRecoveryRecord(t, store, want)
			assertRecoveryOwnershipReleased(t, s, want.JobID)
		})
	}
}

func TestSubmitFailureDoesNotReclassifyOtherExits(t *testing.T) {
	for _, name := range []string{"task Failed", "task Cancelled", "invalid status", "first publication error", "second publication error", "terminal Save error"} {
		t.Run(name, func(t *testing.T) {
			const jobID int64 = 9508
			inner := NewMemoryJobStore()
			before := JobRecord{JobID: jobID, State: JobRecovering, AttemptID: 7, WorkerID: "old-worker", TaskType: "hash", Payload: "old-spec", TimeoutMs: 1000}
			saveRecoveryRecord(t, inner, before)
			observer := &recordingMetrics{}
			store := &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
				if (name == "first publication error" && record.State == JobRunning) ||
					(name == "second publication error" && record.State == JobRunning && record.AttemptID == 9) ||
					(name == "terminal Save error" && record.State == JobSucceeded) {
					return status.Error(codes.Unavailable, "injected storage failure")
				}
				return inner.Save(record)
			}}
			s := &CoordinatorServer{jobStore: store, metrics: observer}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				if name == "second publication error" {
					return nil, status.Error(codes.Unavailable, "first RPC failed")
				}
				resp := successfulResult(req)
				switch name {
				case "task Failed":
					resp.Status = "Failed"
				case "task Cancelled":
					resp.Status = "Cancelled"
				case "invalid status":
					resp.Status = "invalid"
				}
				return resp, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "new-spec", TimeoutMs: 2000})
			want := JobRecord{JobID: jobID, State: JobRunning, AttemptID: 8, WorkerID: "worker", TaskType: "hash", Payload: "new-spec", TimeoutMs: 2000}
			wantCalls, wantTerminals := int32(1), 0
			switch name {
			case "task Failed", "task Cancelled":
				wantTerminals = 1
				want.State = JobFailed
				if name == "task Cancelled" {
					want.State = JobCancelled
				}
				if err != nil || resp.GetStatus() != strings.TrimPrefix(name, "task ") {
					t.Fatalf("task outcome: response=%v error=%v", resp, err)
				}
			default:
				if name == "first publication error" {
					want, wantCalls = before, 0
				}
				var exhausted *retryExhaustionError
				if resp != nil || status.Code(err) != codes.Internal || errors.As(err, &exhausted) {
					t.Fatalf("unrelated exit classified as exhaustion: response=%v error=%v", resp, err)
				}
			}
			assertRecoveryRecord(t, inner, want)
			if calls.Load() != wantCalls || observer.count("terminal", "") != wantTerminals {
				t.Fatalf("calls=%d metrics=%+v", calls.Load(), observer.snapshot())
			}
			assertRecoveryOwnershipReleased(t, s, jobID)
		})
	}
}

func TestSubmitFailureHoldsOwnershipThroughSave(t *testing.T) {
	const jobID int64 = 9509
	inner := NewMemoryJobStore()
	entered, release := make(chan struct{}), make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	store := &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
		if record.State == JobFailed {
			close(entered)
			<-release
		}
		return inner.Save(record)
	}}
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, metrics: observer}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return nil, status.Error(codes.Unavailable, "injected RPC failure")
	})
	contender := make(chan workerFailureResult, 1)
	observer.hook = func(event recordedMetric) {
		if event.name == "terminal" {
			// Hold the owner's return after Save unlocks, so the competing claim is
			// observed before the existing SubmitJob defer releases ownership.
			select {
			case result := <-contender:
				if result.resp != nil || status.Code(result.err) != codes.AlreadyExists {
					t.Errorf("competing submission=%+v", result)
				}
			case <-ctx.Done():
				t.Error("competing claim did not finish")
			}
		}
	}
	req := &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
	finished := make(chan workerFailureResult, 1)
	go func() { resp, err := s.SubmitJob(ctx, req); finished <- workerFailureResult{resp, err} }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("failure Save did not begin")
	}
	go func() { resp, err := s.SubmitJob(ctx, req); contender <- workerFailureResult{resp, err} }()
	unblock()
	select {
	case result := <-finished:
		if result.resp != nil || status.Code(result.err) != codes.Unavailable {
			t.Fatalf("owner result=%+v", result)
		}
	case <-ctx.Done():
		t.Fatal("owner did not finish")
	}
	if calls.Load() != 2 {
		t.Fatalf("competing submission dispatched: calls=%d", calls.Load())
	}
	assertRecoveryRecord(t, inner, JobRecord{JobID: jobID, State: JobFailed, AttemptID: 2, WorkerID: "worker", TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	assertRecoveryOwnershipReleased(t, s, jobID)
}

func TestLateSuccessfulProofCannotOverwriteFailedSubmission(t *testing.T) {
	const jobID int64 = 9507
	output, digest, realVerifier := preimageProofFixture(t)
	payload := preimagePublicPayload(digest)
	store := newWorkerFailureStore(t)
	verifier := &observedPreimageVerifier{verifier: realVerifier}
	dispatchDone := make(chan int64, 2)
	s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier, leaseDuration: 100 * time.Millisecond,
		onDispatchDone: func(_, attempt int64) { dispatchDone <- attempt }}
	started, cancelled, returned := make(chan struct{}), make(chan struct{}), make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	unblock := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(unblock)
	var firstCalls, secondCalls atomic.Int32
	registerTestWorker(t, s, "late-worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		if req.AttemptId != 1 {
			t.Errorf("late worker attempt=%d", req.AttemptId)
		}
		close(started)
		<-ctx.Done()
		close(cancelled)
		<-release // Deliberately produce a real proof after RPC cancellation.
		defer close(returned)
		resp := successfulResult(req)
		resp.Output = output
		return resp, nil
	})
	registerTestWorker(t, s, "failed-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		if req.AttemptId != 2 {
			t.Errorf("backup attempt=%d", req.AttemptId)
		}
		return nil, status.Error(codes.Unavailable, "backup failed")
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	finished := make(chan workerFailureResult, 1)
	go func() {
		resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 1000})
		finished <- workerFailureResult{resp: resp, err: err}
	}()
	select {
	case <-started:
	case <-ctx.Done():
		t.Fatal("first attempt did not start")
	}
	select {
	case result := <-finished:
		if result.resp != nil || status.Code(result.err) != codes.Unavailable {
			t.Fatalf("submission=%+v", result)
		}
	case <-ctx.Done():
		t.Fatal("submission did not finish")
	}
	want := preimageRecord(jobID, 2, "failed-worker", JobFailed, payload)
	assertRecoveryRecord(t, store, want)
	select {
	case <-cancelled:
	case <-ctx.Done():
		t.Fatal("first worker did not observe cancellation")
	}
	select {
	case <-returned:
		t.Fatal("late worker returned before Failed committed")
	default:
	}
	unblock()
	select {
	case <-returned:
	case <-ctx.Done():
		t.Fatal("late worker did not return")
	}
	seen := map[int64]bool{}
	for i := 0; i < 2; i++ {
		select {
		case attempt := <-dispatchDone:
			seen[attempt] = true
		case <-ctx.Done():
			t.Fatal("abandoned dispatch did not finish")
		}
	}
	if !seen[1] || !seen[2] || firstCalls.Load() != 1 || secondCalls.Load() != 1 || verifier.calls.Load() != 0 || s.jobAttempts[jobID] != 2 {
		t.Fatal("late proof affected execution or verification")
	}
	assertWorkerFailureSaves(t, store, []JobRecord{preimageRecord(jobID, 1, "late-worker", JobRunning, payload), preimageRecord(jobID, 2, "failed-worker", JobRunning, payload), want})
	assertRecoveryRecord(t, store, want)
	assertRecoveryOwnershipReleased(t, s, jobID)
}
