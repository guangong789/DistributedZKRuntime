package main

import (
	"context"
	"encoding/base64"
	"testing"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

// BenchmarkWorkerExecuteJob measures the real direct handler, including task
// construction, a fresh task timeout, proving, response creation and normal
// production logging. It excludes gRPC, mTLS, the outer lifecycle admission
// wrapper, Coordinator work and persistence. Metrics use the normal Noop path.
func BenchmarkWorkerExecuteJob(b *testing.B) {
	prover, verifier := newTestPreimageProver(b)
	store := runtime.NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	digest := zk.ComputePreimageDigest(42)
	service := &WorkerService{
		preimageProver: prover,
		witnessStore:   store,
		provingLimiter: newTestProvingLimiter(b),
	}
	req := &runtimepb.ExecuteJobRequest{
		JobId: 602, AttemptId: 8, TaskType: "zk_preimage_prove",
		Payload: preimageTestPayload(b, "secret-001", digest), TimeoutMs: 30000,
	}
	// Each handler call derives and cancels its own 30-second task context.
	// The live parent has no shared deadline that could expire across b.N.
	ctx := context.Background()
	check := func(response *runtimepb.ExecuteJobResponse, err error) {
		if err != nil {
			b.Fatalf("execute worker job: %v", err)
		}
		if response == nil || response.JobId != req.JobId || response.AttemptId != req.AttemptId || response.Status != "Succeeded" || response.Error != "" || response.Output == "" {
			b.Fatal("worker returned an invalid or unsuccessful response")
		}
	}
	verify := func(response *runtimepb.ExecuteJobResponse) {
		proof, err := base64.StdEncoding.DecodeString(response.Output)
		if err != nil || len(proof) == 0 {
			b.Fatalf("decode proof: error=%v bytes=%d", err, len(proof))
		}
		if err := verifier.Verify(proof, digest); err != nil {
			b.Fatalf("verify worker output: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		response, err := service.ExecuteJob(ctx, req)
		check(response, err)
		verify(response)
	}
	var response *runtimepb.ExecuteJobResponse
	var err error
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		response, err = service.ExecuteJob(ctx, req)
		check(response, err)
	}
	b.StopTimer()
	verify(response)
}
