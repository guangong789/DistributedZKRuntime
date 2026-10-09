package main

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type admissionProvingMetrics struct {
	metrics.NoopMetrics
	samples atomic.Int32
}

func (m *admissionProvingMetrics) ObserveZKProving(time.Duration) { m.samples.Add(1) }

// cmd/worker is a separate main package. These local test workers run the real
// runtime task and model its busy RPC mapping, tested at WorkerService itself
// in cmd/worker. No coordinator production behavior is replaced here.
func limitedPreimageWorker(fixture integrationZKFixture, limiter *runtime.ProvingLimiter, observer metrics.Metrics, calls *atomic.Int32) func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
	return func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		jobCtx, cancel := context.WithTimeout(ctx, time.Duration(req.TimeoutMs)*time.Millisecond)
		defer cancel()
		task := runtime.ZKPreimageTask{Payload: req.Payload, Prover: fixture.prover, WitnessStore: fixture.witnesses,
			Limiter: limiter, Metrics: observer}
		result := runtime.ExecuteJob(jobCtx, runtime.Job{ID: int(req.JobId), Task: task, Status: runtime.JobRunning})
		if errors.Is(result.Err, runtime.ErrProvingBusy) {
			return nil, status.Error(codes.ResourceExhausted, result.Err.Error())
		}
		errText := ""
		if result.Err != nil {
			errText = result.Err.Error()
		}
		return &runtimepb.ExecuteJobResponse{JobId: int64(result.JobID), AttemptId: req.AttemptId,
			Status: result.Status.String(), Output: result.Output, Error: errText}, nil
	}
}

func TestSubmitBusyProvingWorkerRetriesAvailableWorker(t *testing.T) {
	fixture := newIntegrationZKFixture(t)
	store := newWorkerFailureStore(t)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, preimageVerifier: fixture.verifier, metrics: observer}
	busyLimiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	availableLimiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if !busyLimiter.TryAcquire() {
		t.Fatal("cannot occupy first worker's capacity")
	}
	defer busyLimiter.Release()
	busyMetrics, availableMetrics := &admissionProvingMetrics{}, &admissionProvingMetrics{}
	var busyCalls, availableCalls atomic.Int32
	// Explicit registration order fixes the first and second round-robin choices.
	registerTestWorker(t, s, "busy-worker", limitedPreimageWorker(fixture, busyLimiter, busyMetrics, &busyCalls))
	registerTestWorker(t, s, "available-worker", limitedPreimageWorker(fixture, availableLimiter, availableMetrics, &availableCalls))
	client := startTestCoordinator(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const jobID int64 = 9603
	resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: jobID, TaskType: "zk_preimage_prove",
		Payload: fixture.payload, TimeoutMs: preimageTestTimeoutMs})
	if err != nil || resp == nil || resp.Status != "Succeeded" || resp.AttemptId != 2 {
		t.Fatalf("busy worker was treated as terminal failure: response=%v error=%v", resp, err)
	}
	assertWorkerFailureSaves(t, store, []JobRecord{
		preimageRecord(jobID, 1, "busy-worker", JobRunning, fixture.payload),
		preimageRecord(jobID, 2, "available-worker", JobRunning, fixture.payload),
		preimageRecord(jobID, 2, "available-worker", JobSucceeded, fixture.payload),
	})
	if busyCalls.Load() != 1 || availableCalls.Load() != 1 || busyMetrics.samples.Load() != 0 || availableMetrics.samples.Load() != 1 {
		t.Fatal("busy worker proved or retry entered Prove more than once")
	}
	if observer.count("submitted", "") != 1 || observer.count("started", "") != 2 || observer.count("finished", "") != 2 ||
		observer.count("finished", string(metrics.AttemptRPCError)) != 1 || observer.count("finished", string(metrics.AttemptAccepted)) != 1 ||
		observer.count("terminal", "") != 1 || observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 ||
		observer.count("verification", "") != 1 {
		t.Fatalf("busy retry metrics=%+v", observer.snapshot())
	}
	assertRecoveryOwnershipReleased(t, s, jobID)
}

func TestRecoveryBusyProvingWorkerRestoresRecovering(t *testing.T) {
	fixture := newIntegrationZKFixture(t)
	store := newWorkerFailureStore(t)
	record := preimageRecord(9604, 7, "old-worker", JobRecovering, fixture.payload)
	saveRecoveryRecord(t, store, record)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, preimageVerifier: fixture.verifier, metrics: observer}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("cannot occupy worker capacity")
	}
	defer limiter.Release()
	provingMetrics := &admissionProvingMetrics{}
	var calls atomic.Int32
	registerTestWorker(t, s, "busy-worker", limitedPreimageWorker(fixture, limiter, provingMetrics, &calls))
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := s.retryRecoveringJob(ctx, record); status.Code(err) != codes.Unavailable {
		t.Fatalf("busy recovery error=%v, want retry exhaustion", err)
	}
	want := record
	want.AttemptID, want.WorkerID = 9, "busy-worker"
	assertRecoveryRecord(t, store, want)
	running8, running9 := want, want
	running8.State, running8.AttemptID = JobRunning, 8
	running9.State = JobRunning
	assertWorkerFailureSaves(t, store, []JobRecord{record, running8, running9, want})
	if calls.Load() != 2 || provingMetrics.samples.Load() != 0 || observer.count("submitted", "") != 0 ||
		observer.count("started", "") != 2 || observer.count("finished", "") != 2 ||
		observer.count("finished", string(metrics.AttemptRPCError)) != 2 || observer.count("terminal", "") != 0 ||
		observer.count("verification", "") != 0 || observer.count("transition", string(metrics.RecoveryRepair)) != 1 {
		t.Fatalf("busy recovery fabricated terminal/proving events: %+v", observer.snapshot())
	}
	for _, event := range observer.snapshot() {
		if (event.name == "started" || event.name == "finished") && event.source != metrics.SourceRecovery {
			t.Error("recovery busy attempts used incorrect source")
		}
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}
