package main

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type blockedTransportProver struct {
	real    runtime.PreimageProver
	entered chan struct{}
	release <-chan struct{}
	calls   atomic.Int32
}

func (p *blockedTransportProver) Prove(secret int64, digest fr.Element) ([]byte, error) {
	if p.calls.Add(1) == 1 {
		close(p.entered)
	}
	<-p.release
	return p.real.Prove(secret, digest)
}

// Uses the production task, limiter and busy mapping. The only injected fault is
// a non-interruptible prover barrier; release delegates to real Groth16 Prove.
func controlledMTLSProvingWorker(fixture integrationZKFixture, prover runtime.PreimageProver, limiter *runtime.ProvingLimiter, observer metrics.Metrics, late chan<- *runtimepb.ExecuteJobResponse) func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
	return func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		jobCtx, cancel := context.WithTimeout(ctx, time.Duration(r.TimeoutMs)*time.Millisecond)
		defer cancel()
		task := runtime.ZKPreimageTask{Payload: r.Payload, Prover: prover, WitnessStore: fixture.witnesses, Limiter: limiter, Metrics: observer}
		result := runtime.ExecuteJob(jobCtx, runtime.Job{ID: int(r.JobId), Task: task, Status: runtime.JobRunning})
		if errors.Is(result.Err, runtime.ErrProvingBusy) {
			return nil, status.Error(codes.ResourceExhausted, result.Err.Error())
		}
		text := ""
		if result.Err != nil {
			text = result.Err.Error()
		}
		response := &runtimepb.ExecuteJobResponse{JobId: int64(result.JobID), AttemptId: r.AttemptId, Status: result.Status.String(), Output: result.Output, Error: text}
		if late != nil {
			late <- response
		}
		return response, nil
	}
}

func TestMTLSCancellationRetainsProvingSlotAndRejectsLateProof(t *testing.T) {
	f := newMTLSCoordinator(t)
	fixture := newIntegrationZKFixture(t)
	f.s.preimageVerifier = fixture.verifier
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	prover := &blockedTransportProver{real: fixture.prover, entered: make(chan struct{}), release: release}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	proving := &admissionProvingMetrics{}
	late := make(chan *runtimepb.ExecuteJobResponse, 1)
	address := startMTLSBackend(t, f, testpki.Worker, controlledMTLSProvingWorker(fixture, prover, limiter, proving, late))
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	wait := mtlsContext(t)
	ctx, cancel := context.WithCancel(wait)
	defer cancel()
	result := make(chan error, 1)
	// Direct SubmitJob lets the test wait for server-side cancellation finalization
	// and ownership release. Its actual Worker RPC still uses production mTLS.
	go func() {
		_, err := f.s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 11107, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 5000})
		result <- err
	}()
	select {
	case <-prover.entered:
	case <-wait.Done():
		t.Fatal("prover did not enter")
	}
	cancel()
	select {
	case err := <-result:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("parent cancellation: %v", err)
		}
	case <-wait.Done():
		t.Fatal("Submit did not stop")
	}
	record, ok, err := f.store.Load(11107)
	if err != nil || !ok || record.State != JobCancelled || record.AttemptID != 1 {
		t.Fatal("cancellation was not durably finalized")
	}
	assertRecoveryOwnershipReleased(t, f.s, 11107)
	if limiter.TryAcquire() {
		limiter.Release()
		t.Fatal("cancellation released capacity while synchronous prover was running")
	}
	response, err := f.s.dispatchJob(wait, WorkerInfo{ID: "worker-a", Address: address}, &runtimepb.ExecuteJobRequest{JobId: 11170, AttemptId: 1, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 1000})
	if response != nil || status.Code(err) != codes.ResourceExhausted || prover.calls.Load() != 1 || proving.samples.Load() != 0 {
		t.Fatal("authenticated busy call entered prover or changed busy semantics")
	}
	before := f.store.savedRecords()
	once.Do(func() { close(release) })
	select {
	case r := <-late:
		proof, err := base64.StdEncoding.DecodeString(r.Output)
		if err != nil || r.Status != "Succeeded" || r.AttemptId != 1 {
			t.Fatal("late proof did not finish")
		}
		if err := fixture.verifier.Verify(proof, zk.ComputePreimageDigest(42)); err != nil {
			t.Fatal(err)
		}
	case <-wait.Done():
		t.Fatal("non-interruptible prover did not finish")
	}
	if !limiter.TryAcquire() {
		t.Fatal("finished prover retained capacity")
	}
	limiter.Release()
	if !reflect.DeepEqual(before, f.store.savedRecords()) || f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 0 || f.observer.count("terminal", string(metrics.TerminalCancelled)) != 1 || f.observer.count("verification", "") != 0 || proving.samples.Load() != 1 {
		t.Fatal("canceled attempt's real late proof changed accepted state or metrics")
	}
}

func TestMTLSLeaseRetryFencesLateSuccessfulProving(t *testing.T) {
	f := newMTLSCoordinator(t)
	fixture := newIntegrationZKFixture(t)
	f.s.preimageVerifier = fixture.verifier
	f.s.leaseDuration = 250 * time.Millisecond
	release := make(chan struct{})
	var once sync.Once
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	prover := &blockedTransportProver{real: fixture.prover, entered: make(chan struct{}), release: release}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	proving := &admissionProvingMetrics{}
	late := make(chan *runtimepb.ExecuteJobResponse, 1)
	dispatchDone := make(chan int64, 2)
	f.s.onDispatchDone = func(id, attempt int64) {
		if id == 11108 {
			dispatchDone <- attempt
		}
	}
	first := startMTLSBackend(t, f, testpki.Worker, controlledMTLSProvingWorker(fixture, prover, limiter, proving, late))
	var secondCalls atomic.Int32
	second := startMTLSBackend(t, f, workerBIdentity, func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		if r.AttemptId != 2 || ctx.Err() != nil {
			t.Error("retry did not inherit a live parent context")
		}
		return fixture.execute(ctx, r, nil)
	})
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", first)
	registerMTLSExecutionWorker(t, f, workerBIdentity, "worker-b", second)
	wait := mtlsContext(t)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	done := make(chan workerFailureResult, 1)
	go func() {
		r, err := client.SubmitJob(wait, &runtimepb.SubmitJobRequest{JobId: 11108, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 5000})
		done <- workerFailureResult{r, err}
	}()
	select {
	case <-prover.entered:
	case <-wait.Done():
		t.Fatal("attempt 1 prover did not start")
	}
	select {
	case result := <-done:
		if result.err != nil || result.resp.GetStatus() != "Succeeded" || result.resp.GetAttemptId() != 2 {
			t.Fatalf("lease retry: %v", result.err)
		}
	case <-wait.Done():
		t.Fatal("retry did not finish")
	}
	if wait.Err() != nil || f.s.isLeaseCurrent(11108, 1) || prover.calls.Load() != 1 || secondCalls.Load() != 1 || proving.samples.Load() != 0 {
		t.Fatal("attempt 2 did not supersede still-running attempt 1")
	}
	if limiter.TryAcquire() {
		limiter.Release()
		t.Fatal("lease expiry released running prover's slot")
	}
	seen := map[int64]bool{}
	for len(seen) < 2 {
		select {
		case attempt := <-dispatchDone:
			if seen[attempt] || (attempt != 1 && attempt != 2) {
				t.Fatal("unexpected extra dispatch")
			}
			seen[attempt] = true
		case <-wait.Done():
			t.Fatal("abandoned dispatch goroutine did not finish")
		}
	}
	before := f.store.savedRecords()
	once.Do(func() { close(release) })
	select {
	case r := <-late:
		proof, err := base64.StdEncoding.DecodeString(r.Output)
		if err != nil || r.Status != "Succeeded" || r.AttemptId != 1 {
			t.Fatal("old attempt did not produce a late success")
		}
		if err := fixture.verifier.Verify(proof, zk.ComputePreimageDigest(42)); err != nil {
			t.Fatal(err)
		}
	case <-wait.Done():
		t.Fatal("old prover did not finish")
	}
	record, ok, err := f.store.Load(11108)
	if err != nil || !ok || record.State != JobSucceeded || record.AttemptID != 2 || record.WorkerID != "worker-b" || !reflect.DeepEqual(before, f.store.savedRecords()) || f.observer.count("verification", "") != 1 || f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 || proving.samples.Load() != 1 {
		t.Fatal("old proof overwrote accepted attempt 2 or was verified again")
	}
	assertRecoveryOwnershipReleased(t, f.s, 11108)
}
