package main

import (
	"context"
	"encoding/base64"
	"flag"
	"sync"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestConfiguredLeaseAllowsControlledProofOverMTLS(t *testing.T) {
	fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
	execution := coordinatorExecutionFlags(fs)
	if err := fs.Parse([]string{"-listen", "127.0.0.1:0", "-attempt-lease", "45s"}); err != nil {
		t.Fatal(err)
	}
	f := newMTLSCoordinator(t, *execution)
	fixture := newIntegrationZKFixture(t)
	f.s.preimageVerifier = fixture.verifier
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	release := make(chan struct{})
	prover := &blockedTransportProver{real: fixture.prover, entered: make(chan struct{}), release: release}
	provingMetrics := &admissionProvingMetrics{}
	address := startMTLSBackend(t, f, testpki.Worker,
		controlledMTLSProvingWorker(fixture, prover, limiter, provingMetrics, nil))
	registerMTLSExecutionWorker(t, f, testpki.Worker, "worker-a", address)
	client := runtimepb.NewCoordinatorServiceClient(f.connection(t, testpki.Client))
	parent, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
	defer stopWaiting()
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	done := make(chan workerFailureResult, 1)
	t.Cleanup(func() { unblock(); cancel() })
	const jobID int64 = 11201
	before := time.Now()
	go func() {
		response, err := client.SubmitJob(parent, &runtimepb.SubmitJobRequest{
			JobId: jobID, TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 40000,
		})
		done <- workerFailureResult{resp: response, err: err}
	}()
	select {
	case <-prover.entered:
	case <-waitCtx.Done():
		t.Fatal("configured request did not enter Prove")
	}
	lease, ok := f.s.getLease(jobID)
	if !ok || lease.AttemptID != 1 || lease.WorkerID != "worker-a" ||
		lease.ExpiresAt.Before(before.Add(45*time.Second)) || lease.ExpiresAt.After(time.Now().Add(45*time.Second)) {
		t.Fatal("configured 45-second lease did not reach actual publication")
	}
	// A controlled proving call outlasts this deliberately shorter budget. The
	// channels enforce ordering; no five- or forty-second wall-clock wait is needed.
	shortDeadline := time.Now().Add(50 * time.Millisecond)
	timer := time.NewTimer(time.Until(shortDeadline))
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-waitCtx.Done():
		t.Fatal("controlled proving interval did not finish")
	}
	select {
	case result := <-done:
		t.Fatalf("blocked proving returned early: %v", result.err)
	default:
	}
	if parent.Err() != nil || !f.s.isLeaseCurrent(jobID, 1) || provingMetrics.samples.Load() != 0 ||
		f.observer.count("verification", "") != 0 || f.observer.count("terminal", "") != 0 {
		t.Fatal("proof was accepted before completion or its configured budget expired")
	}
	if limiter.TryAcquire() {
		limiter.Release()
		t.Fatal("blocked Prove did not retain its single capacity slot")
	}
	unblock()
	var result workerFailureResult
	select {
	case result = <-done:
	case <-waitCtx.Done():
		t.Fatal("valid configured proof did not complete")
	}
	if result.err != nil || result.resp.GetStatus() != "Succeeded" || result.resp.GetAttemptId() != 1 || prover.calls.Load() != 1 {
		t.Fatalf("configured proof response=%v error=%v", result.resp, result.err)
	}
	proof, err := base64.StdEncoding.DecodeString(result.resp.Output)
	public, payloadErr := zk.ParsePreimagePublicPayload(fixture.payload)
	if err != nil || payloadErr != nil || fixture.verifier.Verify(proof, public.Digest) != nil {
		t.Fatal("response was not a valid real Groth16 proof")
	}
	want := JobRecord{JobID: jobID, State: JobSucceeded, AttemptID: 1, WorkerID: "worker-a",
		TaskType: "zk_preimage_prove", Payload: fixture.payload, TimeoutMs: 40000}
	running := want
	running.State = JobRunning
	assertWorkerFailureSaves(t, f.store, []JobRecord{running, want})
	assertRecoveryRecord(t, f.store, want)
	if f.observer.count("verification", "") != 1 || f.observer.count("terminal", string(metrics.TerminalSucceeded)) != 1 || provingMetrics.samples.Load() != 1 {
		t.Fatal("completion bypassed verification or duplicated success metrics")
	}
	assertRecoveryOwnershipReleased(t, f.s, jobID)
	// The same configured listener admits mTLS and refuses plaintext. There is
	// no separate fallback listener in the production startup path.
	plain, err := grpc.NewClient(f.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer plain.Close()
	plainCtx, stopPlain := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer stopPlain()
	snapshot := f.snapshot()
	if response, err := runtimepb.NewCoordinatorServiceClient(plain).SubmitJob(plainCtx, &runtimepb.SubmitJobRequest{JobId: jobID + 1}); err == nil || response != nil {
		t.Fatal("configured production listener accepted plaintext")
	}
	f.assertUnchanged(t, snapshot, jobID+1)
}

func TestCoordinatorDefaultLeaseForDirectServices(t *testing.T) {
	s := newTestCoordinator() // Zero leaseDuration retains the existing fallback.
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(r), nil
	})
	before := time.Now()
	response, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 11202, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
	if err != nil || response.GetStatus() != "Succeeded" {
		t.Fatal(err)
	}
	lease, ok := s.getLease(11202)
	if !ok || lease.ExpiresAt.Before(before.Add(5*time.Second)) || lease.ExpiresAt.After(time.Now().Add(5*time.Second)) {
		t.Fatal("direct Coordinator construction changed its five-second lease default")
	}
}
