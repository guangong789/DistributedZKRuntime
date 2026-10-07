package main

import (
	"context"
	"encoding/base64"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

type workerProvingMetrics struct {
	metrics.NoopMetrics
	durations chan time.Duration
}

func (m *workerProvingMetrics) ObserveZKProving(d time.Duration) { m.durations <- d }

func TestWorkerInjectsProvingMetricsWithoutChangingProof(t *testing.T) {
	prover, verifier := newTestPreimageProver(t)
	store := runtime.NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	observer := &workerProvingMetrics{durations: make(chan time.Duration, 1)}
	worker := &WorkerService{preimageProver: prover, witnessStore: store, metrics: observer}
	digest := zk.ComputePreimageDigest(42)
	resp, err := worker.ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{
		JobId: 906, AttemptId: 2, TaskType: "zk_preimage_prove",
		Payload: preimageTestPayload(t, "secret-001", digest), TimeoutMs: 5000,
	})
	if err != nil || resp == nil || resp.JobId != 906 || resp.AttemptId != 2 || resp.Status != "Succeeded" {
		t.Fatalf("worker response=%v error=%v", resp, err)
	}
	proof, err := base64.StdEncoding.DecodeString(resp.Output)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatalf("instrumented proof did not verify: %v", err)
	}
	if len(observer.durations) != 1 || <-observer.durations < 0 {
		t.Fatal("expected exactly one proving duration")
	}
	_, err = worker.ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{
		JobId: 907, TaskType: "hash", Payload: "hello", TimeoutMs: 1000,
	})
	if err != nil || len(observer.durations) != 0 {
		t.Fatal("non-ZK task emitted proving duration")
	}
}
