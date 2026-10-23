package main

import (
	"context"
	"errors"
	"flag"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestDisableZKCoordinatorAdmission(t *testing.T) {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	cfg := coordinatorExecutionFlags(fs)
	if cfg.DisableZK || (&CoordinatorServer{}).checkZKAvailable("zk_preimage_prove") != nil {
		t.Fatal("zero/default changed ZK availability")
	}
	if err := fs.Parse([]string{"-disable-zk"}); err != nil || !cfg.DisableZK {
		t.Fatal("disable flag not propagated", err)
	}
	store := newRecordingPreimageStore()
	observer := &recordingMetrics{}
	s := &CoordinatorServer{zkDisabled: true, jobStore: store, metrics: observer}
	req := &runtimepb.SubmitJobRequest{JobId: 123, TaskType: "zk_preimage_prove", Payload: `{"secret":"PRIVATE_MARKER"}`, TimeoutMs: 1000}
	for _, shared := range []bool{false, true} {
		var err error
		if shared {
			_, err = s.executeJobFromSource(context.Background(), jobSpecFromRequest(req), metrics.SourceRecovery)
		} else {
			_, err = s.SubmitJob(context.Background(), req)
		}
		if status.Code(err) != codes.FailedPrecondition || strings.Contains(err.Error(), "PRIVATE_MARKER") {
			t.Fatalf("policy rejection: %v", err)
		}
	}
	if _, ok, err := store.Load(req.JobId); err != nil || ok {
		t.Fatal("rejected job persisted", err)
	}
	if len(store.savedRecords()) != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || len(s.activeJobs) != 0 || len(observer.snapshot()) != 0 {
		t.Fatal("disabled submission had side effects")
	}
	if !s.claimJob(req.JobId) {
		t.Fatal("cannot preclaim")
	}
	defer s.releaseJob(req.JobId)
	if _, err := s.SubmitJob(context.Background(), req); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("ownership preceded policy", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := s.SubmitJob(ctx, req); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation precedence changed", err)
	}
	if _, err := s.executeJobFromSource(ctx, jobSpecFromRequest(req), metrics.SourceRecovery); !errors.Is(err, context.Canceled) {
		t.Fatal("shared cancellation precedence changed", err)
	}
}

func TestDisableZKAuthorizationPrecedesAdmission(t *testing.T) {
	f := newMTLSCoordinator(t)
	f.s.zkDisabled = true
	req := &runtimepb.SubmitJobRequest{JobId: 124, TaskType: "zk_preimage_prove", Payload: "PRIVATE_MARKER", TimeoutMs: 1000}
	for _, tc := range []struct {
		identity string
		code     codes.Code
	}{{testpki.Worker, codes.PermissionDenied}, {testpki.Client, codes.FailedPrecondition}} {
		client := runtimepb.NewCoordinatorServiceClient(f.connection(t, tc.identity))
		before := f.snapshot()
		if _, err := client.SubmitJob(mtlsContext(t), req); status.Code(err) != tc.code {
			t.Fatalf("%s: %v", tc.identity, err)
		}
		f.assertUnchanged(t, before, req.JobId)
	}
}

type zkStartupReadStore struct {
	JobStore
	states    []JobState
	failState JobState
	err       error
}

func (s *zkStartupReadStore) ListByState(state JobState) ([]JobRecord, error) {
	s.states = append(s.states, state)
	if s.err != nil && state == s.failState {
		return nil, s.err
	}
	return s.JobStore.ListByState(state)
}
func (s *zkStartupReadStore) Save(JobRecord) error { panic("startup guard must not save") }

func TestDisableZKStartupGuardReadsBothStates(t *testing.T) {
	inner := NewMemoryJobStore()
	if err := inner.Save(JobRecord{JobID: 1, State: JobRunning, TaskType: "zk_preimage_prove", Payload: "PRIVATE_MARKER"}); err != nil {
		t.Fatal(err)
	}
	for _, fail := range []bool{false, true} {
		injected := errors.New("list unavailable")
		store := &zkStartupReadStore{JobStore: inner, failState: JobRecovering}
		if fail {
			store.err = injected
		}
		s := &CoordinatorServer{jobStore: store}
		err := s.checkDisabledZKRecovery()
		if err == nil || strings.Contains(err.Error(), "PRIVATE_MARKER") || (fail && !errors.Is(err, injected)) {
			t.Fatal("startup rejection", err)
		}
		if !reflect.DeepEqual(store.states, []JobState{JobRunning, JobRecovering}) || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
			t.Fatal("guard skipped a state or allocated work")
		}
	}
	store := &zkStartupReadStore{JobStore: inner, failState: JobRunning, err: errors.New("first list failed")}
	if err := (&CoordinatorServer{jobStore: store}).checkDisabledZKRecovery(); !errors.Is(err, store.err) {
		t.Fatal("first list error lost", err)
	}
}

func nonZKCoordinatorSecurity(t *testing.T) coordinatorSecurityConfig {
	t.Helper()
	files := testpki.New(t).Issue(t, testpki.Coordinator, nil)
	return coordinatorSecurityConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, Identity: testpki.Coordinator}
}

func TestDisableZKStartupPreservesSQLite(t *testing.T) {
	security := nonZKCoordinatorSecurity(t)
	for _, tc := range []struct {
		name      string
		state     JobState
		both      bool
		malformed bool
	}{{"running", JobRunning, false, false}, {"recovering", JobRecovering, false, false}, {"both", JobRunning, true, false}, {"malformed", JobRunning, true, true}} {
		t.Run(tc.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "jobs.db")
			store, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			payload := `{"witness_ref":"demo","digest":"1"}`
			if tc.malformed {
				payload = "PRIVATE_MARKER"
			}
			records := []JobRecord{
				{JobID: 1, State: tc.state, AttemptID: 7, WorkerID: "old", TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 1000},
				{JobID: 2, State: JobRunning, AttemptID: 3, TaskType: "hash", Payload: "hello", TimeoutMs: 1000},
				{JobID: 3, State: JobRecovering, AttemptID: 4, TaskType: "sleep", Payload: "1ms", TimeoutMs: 1000},
			}
			if tc.both {
				r := records[0]
				r.JobID = 4
				r.State = JobRecovering
				records = append(records, r)
			}
			for _, r := range records {
				if err := store.Save(r); err != nil {
					t.Fatal(err)
				}
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			cfg := coordinatorExecutionConfig{DisableZK: true, ListenAddress: "127.0.0.1:0", AttemptLease: time.Second, ShutdownTimeout: time.Second}
			err = runCoordinator(ctx, cfg, security, path, filepath.Join(t.TempDir(), "missing-vk"), "127.0.0.1:0")
			if err == nil || !strings.Contains(err.Error(), "unfinished ZK") || strings.Contains(err.Error(), "PRIVATE_MARKER") {
				t.Fatal("unexpected startup result", err)
			}
			reopened, err := NewSQLiteJobStore(path)
			if err != nil {
				t.Fatal(err)
			}
			defer reopened.Close()
			for _, r := range records {
				assertSQLiteJobRecord(t, reopened, r)
			}
		})
	}
}

func TestDisableZKRecoveryGateAllowsOrdinaryAndTerminalJobs(t *testing.T) {
	store, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	records := []JobRecord{{JobID: 1, State: JobRunning, AttemptID: 3, TaskType: "hash", Payload: "hello", TimeoutMs: 1000}, {JobID: 2, State: JobRecovering, AttemptID: 4, TaskType: "sleep", Payload: "1ms", TimeoutMs: 1000}}
	for i, state := range []JobState{JobSucceeded, JobFailed, JobCancelled} {
		records = append(records, JobRecord{JobID: int64(i + 3), State: state, AttemptID: 7, TaskType: "zk_preimage_prove", Payload: "untouched", TimeoutMs: 1000})
	}
	for _, r := range records {
		if err := store.Save(r); err != nil {
			t.Fatal(err)
		}
	}
	s := &CoordinatorServer{zkDisabled: true, jobStore: store}
	if err := s.checkDisabledZKRecovery(); err != nil {
		t.Fatal(err)
	}
	if err := s.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	registerTestWorker(t, s, "ordinary", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	for i := 0; i < 2; i++ {
		r := records[i]
		r.State = JobRecovering
		if err := s.retryRecoveringJob(context.Background(), r); err != nil {
			t.Fatal(err)
		}
		r.State = JobSucceeded
		r.AttemptID++
		r.WorkerID = "ordinary"
		assertSQLiteJobRecord(t, store, r)
	}
	for _, r := range records[2:] {
		assertSQLiteJobRecord(t, store, r)
	}
}

// Recovery generation testing uses a controlled verifier, not a Groth16 setup.
func TestDisableZKRejectedDatabaseCanResumeEnabled(t *testing.T) {
	path := filepath.Join(t.TempDir(), "jobs.db")
	store, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	r := JobRecord{JobID: 1, State: JobRunning, AttemptID: 7, WorkerID: "old", TaskType: "zk_preimage_prove", Payload: `{"witness_ref":"demo","digest":"1"}`, TimeoutMs: 1000}
	if err := store.Save(r); err != nil {
		t.Fatal(err)
	}
	if err := (&CoordinatorServer{zkDisabled: true, jobStore: store}).checkDisabledZKRecovery(); err == nil {
		t.Fatal("unfinished job allowed")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := NewSQLiteJobStore(path)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	assertSQLiteJobRecord(t, reopened, r)
	verifier := &countingPreimageVerifier{}
	s := &CoordinatorServer{jobStore: reopened, preimageVerifier: verifier}
	if err := s.recoverRunningJobs(); err != nil {
		t.Fatal(err)
	}
	r.State = JobRecovering
	assertSQLiteJobRecord(t, reopened, r)
	registerTestWorker(t, s, "new", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		resp := successfulResult(req)
		resp.Output = "cHJvb2Y="
		return resp, nil
	})
	if err := s.retryRecoveringJob(context.Background(), r); err != nil {
		t.Fatal(err)
	}
	r.State = JobSucceeded
	r.AttemptID = 8
	r.WorkerID = "new"
	assertSQLiteJobRecord(t, reopened, r)
	if verifier.calls.Load() != 1 {
		t.Fatal("enabled recovery bypassed verifier")
	}
}

func TestDisableZKDefaultCoordinatorStillRequiresVerifier(t *testing.T) {
	security := nonZKCoordinatorSecurity(t)
	for _, malformed := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "verifying.key")
		if malformed {
			if err := os.WriteFile(path, []byte("invalid key"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		cfg := coordinatorExecutionConfig{ListenAddress: "127.0.0.1:0", AttemptLease: time.Second, ShutdownTimeout: time.Second}
		err := runCoordinator(context.Background(), cfg, security, filepath.Join(t.TempDir(), "jobs.db"), path, "127.0.0.1:0")
		if err == nil || !strings.Contains(err.Error(), "load preimage verifier") {
			t.Fatal("enabled startup accepted invalid verifier", err)
		}
	}
}
