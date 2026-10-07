package main

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestRecoveryInfrastructureFailureCanRecoverOnNextOpportunity(t *testing.T) {
	db, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	record := recoveringTestRecord()
	record.AttemptID = 1
	saveRecoveryRecord(t, db, record)
	var writes []JobRecord
	store := &recoveryPassStore{JobStore: db, save: func(got JobRecord) error {
		if err := db.Save(got); err != nil {
			return err
		}
		writes = append(writes, got)
		return nil
	}}
	s := &CoordinatorServer{jobStore: store}
	started := make(chan int64, 1)
	fail := make(chan struct{})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	registerTestWorker(t, s, "failed-worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		started <- req.AttemptId
		select {
		case <-fail:
			return nil, status.Error(codes.Unavailable, "injected transport failure")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	})
	done := make(chan error, 1)
	go func() { done <- s.retryRecoveringJob(ctx, record) }()
	select {
	case attempt := <-started:
		if attempt != 2 {
			t.Fatalf("failed attempt=%d, want 2", attempt)
		}
	case <-ctx.Done():
		t.Fatal("recovery did not dispatch")
	}
	running2 := record
	running2.State, running2.AttemptID, running2.WorkerID = JobRunning, 2, "failed-worker"
	assertRecoveryRecord(t, db, running2)
	// Remove the only worker from eligibility before returning its RPC error.
	// The unchanged retry budget therefore cannot start another attempt.
	s.mu.Lock()
	worker := s.workers["failed-worker"]
	worker.LastSeen = time.Now().Add(-time.Hour)
	s.workers[worker.ID] = worker
	s.mu.Unlock()
	s.checkWorkerLiveness(time.Second)
	close(fail)
	select {
	case err := <-done:
		if status.Code(err) != codes.Unavailable {
			t.Fatalf("recovery error=%v, want Unavailable", err)
		}
	case <-ctx.Done():
		t.Fatal("failed recovery did not finish")
	}
	recovering2 := running2
	recovering2.State = JobRecovering
	assertRecoveryRecord(t, db, recovering2)
	assertRecoveryOwnershipReleased(t, s, record.JobID)

	registerTestWorker(t, s, "usable-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if req.AttemptId != 3 || req.TaskType != record.TaskType ||
			req.Payload != record.Payload || req.TimeoutMs != record.TimeoutMs {
			t.Errorf("next recovery request lost generation/spec: %+v", req)
		}
		running3 := record
		running3.State, running3.AttemptID, running3.WorkerID = JobRunning, 3, "usable-worker"
		got, ok, err := db.Load(record.JobID)
		if err != nil || !ok || got != running3 {
			t.Errorf("Running 3 not persisted before dispatch: record=%+v found=%v error=%v", got, ok, err)
		}
		return successfulResult(req), nil
	})
	// A later pass must find the repaired Recovering record without a restart.
	if err := s.runRecoveryPass(ctx); err != nil {
		t.Fatal(err)
	}
	running3 := record
	running3.State, running3.AttemptID, running3.WorkerID = JobRunning, 3, "usable-worker"
	succeeded3 := running3
	succeeded3.State = JobSucceeded
	assertRecoveryRecord(t, db, succeeded3)
	assertRecoveryOwnershipReleased(t, s, record.JobID)
	if want := []JobRecord{running2, recovering2, running3, succeeded3}; !reflect.DeepEqual(writes, want) {
		t.Fatalf("writes=%+v, want %+v", writes, want)
	}
}

func TestRecoveryCancellationRestoresCurrentGeneration(t *testing.T) {
	record := recoveringTestRecord()
	record.AttemptID = 1
	s := newTestCoordinator()
	saveRecoveryRecord(t, s.jobStore, record)
	started := make(chan int64, 1)
	dispatchDone := make(chan struct{}, 1)
	s.onDispatchDone = func(_, _ int64) { dispatchDone <- struct{}{} }
	registerTestWorker(t, s, "worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		started <- req.AttemptId
		<-ctx.Done()
		return nil, ctx.Err()
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	done := make(chan error, 1)
	go func() { done <- s.retryRecoveringJob(ctx, record) }()
	select {
	case attempt := <-started:
		if attempt != 2 {
			t.Fatalf("attempt=%d, want 2", attempt)
		}
	case <-waitCtx.Done():
		t.Fatal("recovery did not dispatch")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("recovery error=%v, want context.Canceled", err)
		}
	case <-waitCtx.Done():
		t.Fatal("canceled recovery did not finish")
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobRecovering, 2, "worker"
	assertRecoveryRecord(t, s.jobStore, want)
	assertRecoveryOwnershipReleased(t, s, record.JobID)
	select {
	case <-dispatchDone:
	case <-waitCtx.Done():
		t.Fatal("canceled dispatch did not finish")
	}
}

func TestRestoreRecoveringJobChecksDurableGenerationAndState(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   JobState
		attempt int64
		missing bool
		repair  bool
	}{
		{"matching running", JobRunning, 2, false, true},
		{"succeeded", JobSucceeded, 2, false, false},
		{"failed", JobFailed, 2, false, false},
		{"cancelled", JobCancelled, 2, false, false},
		{"recovering", JobRecovering, 2, false, false},
		{"older generation", JobRunning, 1, false, false},
		{"newer generation", JobRunning, 3, false, false},
		{"missing", JobRunning, 2, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := recoveringTestRecord()
			record.State, record.AttemptID = tc.state, tc.attempt
			inner := NewMemoryJobStore()
			if !tc.missing {
				saveRecoveryRecord(t, inner, record)
			}
			saves := 0
			s := &CoordinatorServer{jobStore: &recoveryPassStore{
				JobStore: inner,
				save: func(got JobRecord) error {
					saves++
					return inner.Save(got)
				},
			}}
			if !s.claimJob(record.JobID) {
				t.Fatal("cannot claim job")
			}
			defer s.releaseJob(record.JobID)
			if err := s.restoreRecoveringJob(record.JobID, 2); err != nil {
				t.Fatal(err)
			}
			wantSaves := 0
			if tc.repair {
				wantSaves = 1
				record.State = JobRecovering
			}
			if saves != wantSaves {
				t.Fatalf("repair saves=%d, want %d", saves, wantSaves)
			}
			if tc.missing {
				if _, ok, err := inner.Load(record.JobID); err != nil || ok {
					t.Fatalf("repair created missing job: found=%v error=%v", ok, err)
				}
			} else {
				assertRecoveryRecord(t, inner, record)
			}
			if s.claimJob(record.JobID) {
				t.Fatal("repair released execution ownership")
			}
		})
	}
}

func TestRecoveryPreservesAcceptedTerminalResults(t *testing.T) {
	for _, tc := range []struct {
		status string
		state  JobState
	}{
		{"Succeeded", JobSucceeded},
		{"Failed", JobFailed},
		{"Cancelled", JobCancelled},
	} {
		t.Run(tc.status, func(t *testing.T) {
			record := recoveringTestRecord()
			record.AttemptID = 1
			s := newTestCoordinator()
			saveRecoveryRecord(t, s.jobStore, record)
			calls := make(chan int64, 2)
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls <- req.AttemptId
				resp := successfulResult(req)
				resp.Status = tc.status
				return resp, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.retryRecoveringJob(ctx, record); err != nil {
				t.Fatal(err)
			}
			want := record
			want.State, want.AttemptID, want.WorkerID = tc.state, 2, "worker"
			assertRecoveryRecord(t, s.jobStore, want)
			if len(calls) != 1 || <-calls != 2 {
				t.Fatal("accepted terminal result caused extra attempts")
			}
			assertRecoveryOwnershipReleased(t, s, record.JobID)
		})
	}
}

func TestFailedRecoveryDoesNotOverwriteAdvancedDurableRecord(t *testing.T) {
	for _, tc := range []struct {
		name    string
		state   JobState
		attempt int64
	}{
		{"succeeded", JobSucceeded, 2},
		{"failed", JobFailed, 2},
		{"cancelled", JobCancelled, 2},
		{"newer running generation", JobRunning, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			record := recoveringTestRecord()
			record.AttemptID = 1
			s := newTestCoordinator()
			saveRecoveryRecord(t, s.jobStore, record)
			advanced := record
			advanced.State, advanced.AttemptID, advanced.WorkerID = tc.state, tc.attempt, "other-writer"
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if req.AttemptId != 2 {
					t.Errorf("recovery attempt=%d, want 2", req.AttemptId)
				}
				// Model an independent writer advancing persistence before repair.
				if err := s.jobStore.Save(advanced); err != nil {
					t.Errorf("save advanced record: %v", err)
				}
				s.mu.Lock()
				worker := s.workers["worker"]
				worker.Status = WorkerDead
				s.workers[worker.ID] = worker
				s.mu.Unlock()
				return nil, status.Error(codes.Unavailable, "injected failure")
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := s.retryRecoveringJob(ctx, record); status.Code(err) != codes.Unavailable {
				t.Fatalf("recovery error=%v, want Unavailable", err)
			}
			assertRecoveryRecord(t, s.jobStore, advanced)
			if s.jobAttempts[record.JobID] != 2 {
				t.Fatalf("repair allocated an attempt: %v", s.jobAttempts)
			}
			assertRecoveryOwnershipReleased(t, s, record.JobID)
		})
	}
}

func TestRestoreRecoveringJobPropagatesStoreErrors(t *testing.T) {
	for _, operation := range []string{"load", "save"} {
		t.Run(operation, func(t *testing.T) {
			record := recoveringTestRecord()
			record.State, record.AttemptID = JobRunning, 2
			inner := NewMemoryJobStore()
			saveRecoveryRecord(t, inner, record)
			wantErr := errors.New("injected repair store failure")
			store := &recoveryPassStore{JobStore: inner}
			if operation == "load" {
				store.load = func(int64) (JobRecord, bool, error) {
					return JobRecord{}, false, wantErr
				}
			} else {
				store.save = func(JobRecord) error { return wantErr }
			}
			s := &CoordinatorServer{jobStore: store}
			if !s.claimJob(record.JobID) {
				t.Fatal("cannot claim job")
			}
			defer s.releaseJob(record.JobID)
			if err := s.restoreRecoveringJob(record.JobID, 2); !errors.Is(err, wantErr) {
				t.Fatalf("repair error=%v, want %v", err, wantErr)
			}
			assertRecoveryRecord(t, inner, record)
		})
	}
}

func TestRecoveryCancellationReportsRepairFailureAndReleasesOwnership(t *testing.T) {
	record := recoveringTestRecord()
	record.AttemptID = 1
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, record)
	repairErr := errors.New("cannot save repair")
	store := &recoveryPassStore{JobStore: inner, save: func(got JobRecord) error {
		if got.State == JobRecovering && got.AttemptID == 2 {
			return repairErr
		}
		return inner.Save(got)
	}}
	s := &CoordinatorServer{jobStore: store}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		cancel()
		return nil, status.Error(codes.Unavailable, "injected failure")
	})
	err := s.runRecoveryPass(ctx)
	if !errors.Is(err, context.Canceled) || !errors.Is(err, repairErr) {
		t.Fatalf("error=%v, want cancellation and repair failure", err)
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobRunning, 2, "worker"
	assertRecoveryRecord(t, inner, want)
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}
