package main

import (
	"context"
	"errors"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestMetricsRecoveryTransitionsFollowPersistence(t *testing.T) {
	for _, scenario := range []string{"startup", "startup save failure", "repair", "repair load failure", "repair save failure", "compare skip"} {
		t.Run(scenario, func(t *testing.T) {
			observer := &recordingMetrics{}
			inner := NewMemoryJobStore()
			record := recoveringTestRecord()
			record.State = JobRunning
			if scenario == "compare skip" {
				record.State = JobSucceeded
			}
			saveRecoveryRecord(t, inner, record)
			wantErr := errors.New("injected persistence failure")
			store := &recoveryPassStore{JobStore: inner,
				load: func(id int64) (JobRecord, bool, error) {
					if scenario == "repair load failure" {
						observer.record(recordedMetric{name: "load_failed"})
						return JobRecord{}, false, wantErr
					}
					return inner.Load(id)
				},
				save: func(got JobRecord) error {
					if scenario == "startup save failure" || scenario == "repair save failure" {
						observer.record(recordedMetric{name: "save_failed"})
						return wantErr
					}
					if err := inner.Save(got); err != nil {
						return err
					}
					observer.record(recordedMetric{name: "recovering_saved"})
					return nil
				},
			}
			s := &CoordinatorServer{jobStore: store, metrics: observer}
			var err error
			if scenario == "startup" || scenario == "startup save failure" {
				err = s.recoverRunningJobs()
			} else {
				if !s.claimJob(record.JobID) {
					t.Fatal("cannot claim repair job")
				}
				err = s.restoreRecoveringJob(record.JobID, record.AttemptID)
				s.releaseJob(record.JobID)
			}
			wantNames := []string{"recovering_saved", "transition"}
			switch scenario {
			case "startup save failure":
				wantNames = []string{"save_failed"}
			case "repair load failure":
				wantNames = []string{"load_failed", "repair_failed"}
				if observer.count("repair_failed", string(metrics.RepairLoad)) != 1 {
					t.Fatal("missing repair Load failure event")
				}
			case "repair save failure":
				wantNames = []string{"save_failed", "repair_failed"}
				if observer.count("repair_failed", string(metrics.RepairSave)) != 1 {
					t.Fatal("missing repair Save failure event")
				}
			case "compare skip":
				wantNames = nil
			}
			var names []string
			for _, event := range observer.snapshot() {
				names = append(names, event.name)
			}
			if !reflect.DeepEqual(names, wantNames) {
				t.Fatalf("events=%v, want %v", names, wantNames)
			}
			if scenario == "startup" || scenario == "repair" {
				if err != nil {
					t.Fatal(err)
				}
				record.State = JobRecovering
				cause := metrics.RecoveryStartup
				if scenario == "repair" {
					cause = metrics.RecoveryRepair
				}
				if observer.count("transition", string(cause)) != 1 {
					t.Fatalf("wrong transition cause: %+v", observer.snapshot())
				}
			} else if scenario != "compare skip" && !errors.Is(err, wantErr) {
				t.Fatalf("error=%v, want %v", err, wantErr)
			}
			assertRecoveryRecord(t, inner, record)
		})
	}
}

func TestMetricsRecoverySourcesRetriesAndNoWorker(t *testing.T) {
	record := recoveringTestRecord()
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, record)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: inner, metrics: observer}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.runRecoveryPass(ctx); err != nil {
		t.Fatal(err)
	}
	if observer.count("started", "") != 0 || observer.count("terminal", "") != 0 ||
		observer.count("recovery_failed", string(metrics.FailureUnavailable)) != 1 ||
		observer.count("pass", string(metrics.PassCompletedWithErrors)) != 1 {
		t.Fatalf("no-worker fabricated lifecycle events: %+v", observer.snapshot())
	}
	assertRecoveryRecord(t, inner, record)
	var fail atomic.Bool
	fail.Store(true)
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if fail.Load() {
			return nil, status.Error(codes.Unavailable, "injected RPC failure")
		}
		return successfulResult(req), nil
	})
	if err := s.runRecoveryPass(ctx); err != nil {
		t.Fatal(err)
	}
	want := record
	want.AttemptID, want.WorkerID = 9, "worker"
	assertRecoveryRecord(t, inner, want)
	if observer.count("started", "") != 2 || observer.count("finished", string(metrics.AttemptRPCError)) != 2 ||
		observer.count("transition", string(metrics.RecoveryRepair)) != 1 {
		t.Fatalf("exhausted recovery metrics=%+v", observer.snapshot())
	}
	fail.Store(false)
	if err := s.runRecoveryPass(ctx); err != nil {
		t.Fatal(err)
	}
	want.State, want.AttemptID = JobSucceeded, 10
	assertRecoveryRecord(t, inner, want)
	if observer.count("submitted", "") != 0 || observer.count("started", "") != 3 ||
		observer.count("finished", "") != 3 || observer.count("terminal", "") != 1 ||
		observer.count("execution", "") != 3 || observer.count("pass", string(metrics.PassCompleted)) != 1 {
		t.Fatalf("duplicate lifecycle events=%+v", observer.snapshot())
	}
	for _, event := range observer.snapshot() {
		switch event.name {
		case "started", "finished", "terminal", "execution":
			if event.source != metrics.SourceRecovery {
				t.Errorf("incorrect execution source: %+v", event)
			}
		}
	}
}

func TestMetricsRepairFailureIsRecordedDuringShutdown(t *testing.T) {
	record := recoveringTestRecord()
	record.AttemptID = 1
	inner := NewMemoryJobStore()
	saveRecoveryRecord(t, inner, record)
	observer := &recordingMetrics{}
	store := &recoveryPassStore{JobStore: inner, save: func(got JobRecord) error {
		if got.State == JobRecovering {
			return errors.New("injected repair Save failure")
		}
		return inner.Save(got)
	}}
	s := &CoordinatorServer{jobStore: store, metrics: observer, recoveryReady: make(chan struct{}, 1)}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	dispatchDone := make(chan struct{}, 1)
	s.onDispatchDone = func(_, _ int64) { dispatchDone <- struct{}{} }
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		cancel()
		return nil, status.Error(codes.Unavailable, "worker unavailable")
	})
	done := make(chan struct{})
	go func() { s.runRecoveryLoop(ctx); close(done) }()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	select {
	case <-done:
	case <-waitCtx.Done():
		t.Fatal("recovery loop did not finish shutdown")
	}
	select {
	case <-dispatchDone:
	case <-waitCtx.Done():
		t.Fatal("canceled dispatch did not finish")
	}
	if observer.count("repair_failed", string(metrics.RepairSave)) != 1 ||
		observer.count("recovery_failed", string(metrics.FailureCancelled)) != 1 ||
		observer.count("pass", string(metrics.PassCancelled)) != 1 ||
		observer.count("finished", string(metrics.AttemptCancelled)) != 1 ||
		observer.count("transition", "") != 0 || observer.count("terminal", "") != 0 {
		t.Fatalf("shutdown lost/doubled events: %+v", observer.snapshot())
	}
	want := record
	want.State, want.AttemptID, want.WorkerID = JobRunning, 2, "worker"
	assertRecoveryRecord(t, inner, want)
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestMetricsSubmissionRejectedBeforeExecution(t *testing.T) {
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: NewMemoryJobStore(), metrics: observer}
	if !s.claimJob(904) {
		t.Fatal("cannot preclaim")
	}
	defer s.releaseJob(904)
	if _, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 904}); status.Code(err) != codes.AlreadyExists {
		t.Fatalf("ownership error=%v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 905}); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled submission=%v", err)
	}
	if len(observer.snapshot()) != 0 {
		t.Fatalf("rejected submissions emitted metrics: %+v", observer.snapshot())
	}
}
