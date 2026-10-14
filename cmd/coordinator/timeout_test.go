package main

import (
	"context"
	"fmt"
	"math"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestCoordinatorTimeoutValidationBeforeSideEffects(t *testing.T) {
	f := newMTLSCoordinator(t)
	var calls atomic.Int32
	address := startMTLSBackend(t, f, testpki.Worker, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	for _, ms := range []int64{0, -1, math.MinInt64, 9223372036855, 1 << 58, 1<<58 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			before := f.snapshot()
			resp, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 12101, TaskType: "hash", Payload: "hello", TimeoutMs: ms})
			if resp != nil || status.Code(err) != codes.InvalidArgument {
				t.Fatalf("timeout=%d response=%v error=%v", ms, resp, err)
			}
			f.assertUnchanged(t, before, 12101)
		})
	}
	// Validation must also precede ownership conflict, without releasing its owner.
	if !f.s.claimJob(12101) {
		t.Fatal("cannot preclaim")
	}
	before := f.snapshot()
	if _, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: 12101, TimeoutMs: 0}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("ownership preceded validation")
	}
	f.assertUnchanged(t, before, 12101)
	f.s.releaseJob(12101)
	if calls.Load() != 0 {
		t.Fatal("invalid timeout dispatched")
	}
	for _, ms := range []int64{1, 5000, 9223372036854} {
		resp, err := client.SubmitJob(mtlsContext(t), &runtimepb.SubmitJobRequest{JobId: ms, TaskType: "hash", Payload: "hello", TimeoutMs: ms})
		if err != nil || resp.GetStatus() != "Succeeded" || resp.GetAttemptId() != 1 {
			t.Fatalf("valid timeout=%d response=%v error=%v", ms, resp, err)
		}
		record, ok, err := f.store.Load(ms)
		if err != nil || !ok || record.TimeoutMs != ms {
			t.Fatal("valid timeout not preserved")
		}
	}
}

func TestInvalidLegacyTimeoutPreservedAndRecoveryBacksOff(t *testing.T) {
	for _, ms := range []int64{0, -1, 9223372036855, 1 << 58, 1<<58 + 1} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			store := newWorkerFailureStore(t)
			running := JobRecord{JobID: 12110, State: JobRunning, AttemptID: 7, WorkerID: "old", TaskType: "hash", Payload: "hello", TimeoutMs: ms}
			recovering := running
			recovering.JobID, recovering.State = 12111, JobRecovering
			saveRecoveryRecord(t, store, running)
			saveRecoveryRecord(t, store, recovering)
			s := &CoordinatorServer{jobStore: store}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return successfulResult(req), nil
			})
			if err := s.recoverRunningJobs(); err != nil {
				t.Fatal(err)
			}
			if err := s.retryRecoveringJob(context.Background(), recovering); status.Code(err) != codes.InvalidArgument {
				t.Fatalf("invalid persisted timeout: %v", err)
			}
			clock := newManualRecoveryClock()
			q := testRecoverySchedule(clock)
			for i, delay := range []time.Duration{time.Second, 2 * time.Second} {
				if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
					t.Fatal(err)
				}
				e := q.entries[recovering.JobID]
				if e.generation != 7 || e.failures != uint32(i+1) || !e.nextRetry.Equal(clock.now().Add(delay)) {
					t.Fatalf("invalid spec must retain generation and bounded cooldown: %+v", e)
				}
				if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
					t.Fatal(err)
				}
				if q.entries[recovering.JobID] != e {
					t.Fatal("early scan bypassed cooldown")
				}
				clock.advance(delay)
			}
			assertRecoveryRecord(t, store, running)
			assertRecoveryRecord(t, store, recovering)
			if calls.Load() != 0 || len(store.savedRecords()) != 2 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || s.nextWorker != 0 || len(s.activeJobs) != 0 {
				t.Fatal("invalid legacy timeout was overwritten, dispatched or allocated an attempt")
			}
		})
	}
}

func TestCoordinatorTaskDeadlineCancelledWithoutRetryAndResubmits(t *testing.T) {
	f := newMTLSCoordinator(t)
	fixture := newIntegrationZKFixture(t)
	f.s.preimageVerifier = fixture.verifier
	var calls atomic.Int32
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	prover := &blockedTransportProver{real: fixture.prover, entered: make(chan struct{}), release: release}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	proving := &admissionProvingMetrics{}
	taskContexts := make(chan context.Context, 1)
	address := startMTLSBackend(t, f, testpki.Worker, func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		if calls.Add(1) == 1 {
			// Keep the RPC alive while the shorter Worker task budget expires.
			timeout, err := runtime.TaskTimeoutFromMilliseconds(req.TimeoutMs)
			if err != nil {
				return nil, err
			}
			taskCtx, cancel := context.WithTimeout(ctx, timeout)
			defer cancel()
			taskContexts <- taskCtx
			return controlledMTLSProvingWorker(fixture, prover, limiter, proving, nil)(taskCtx, req)
		}
		return fixture.execute(ctx, req, nil)
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	ctx := mtlsContext(t)
	done := make(chan workerFailureResult, 1)
	submissionFinished := make(chan struct{})
	go func() {
		defer close(submissionFinished)
		r, err := f.s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 12120, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 250})
		done <- workerFailureResult{r, err}
	}()
	t.Cleanup(func() {
		once.Do(func() { close(release) })
		select {
		case <-submissionFinished:
		case <-time.After(5 * time.Second):
			t.Error("submission did not finish after releasing Prove")
		}
	})
	select {
	case <-prover.entered:
	case <-ctx.Done():
		t.Fatal("Prove did not start")
	}
	var taskCtx context.Context
	select {
	case taskCtx = <-taskContexts:
	case <-ctx.Done():
		t.Fatal("task context not published")
	}
	select {
	case <-taskCtx.Done():
	case <-ctx.Done():
		t.Fatal("task deadline did not expire")
	}
	if limiter.TryAcquire() {
		limiter.Release()
		t.Fatal("deadline released running proof")
	}
	select {
	case <-done:
		t.Fatal("deadline falsely completed synchronous Prove")
	default:
	}
	once.Do(func() { close(release) })
	select {
	case result := <-done:
		if result.err != nil || result.resp.GetStatus() != "Cancelled" || result.resp.GetAttemptId() != 1 || result.resp.GetOutput() != "" || result.resp.GetError() != context.DeadlineExceeded.Error() {
			t.Fatalf("task timeout changed terminal mapping: %+v error=%v", result.resp, result.err)
		}
	case <-ctx.Done():
		t.Fatal("late task did not finish")
	}
	want := JobRecord{JobID: 12120, State: JobCancelled, AttemptID: 1, WorkerID: "worker-a", TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 250}
	assertRecoveryRecord(t, f.store, want)
	if calls.Load() != 1 || f.observer.count("verification", "") != 0 || f.observer.count("started", "") != 1 || proving.samples.Load() != 1 {
		t.Fatal("task Cancelled was retried, verified or double-counted")
	}
	assertRecoveryOwnershipReleased(t, f.s, want.JobID)
	// Explicit resubmission has a new task budget and exactly the next generation.
	r, err := f.s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: want.JobID, TaskType: want.TaskType, Payload: want.Payload, TimeoutMs: 5000})
	if err != nil || r.GetStatus() != "Succeeded" || r.GetAttemptId() != 2 || calls.Load() != 2 || f.observer.count("verification", "") != 1 {
		t.Fatalf("resubmission: response=%v error=%v", r, err)
	}
	want.State, want.AttemptID, want.TimeoutMs = JobSucceeded, 2, 5000
	assertRecoveryRecord(t, f.store, want)
	assertRecoveryOwnershipReleased(t, f.s, want.JobID)
	if f.observer.count("terminal", string(metrics.TerminalCancelled)) != 1 || f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 {
		t.Fatal("unexpected terminal classification")
	}
}
