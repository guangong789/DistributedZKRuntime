package main

import (
	"context"
	"strings"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func TestWorkerRejectsPrivateZKPayloadBeforeProving(t *testing.T) {
	prover, _ := newTestPreimageProver(t)
	witnesses := runtime.NewMemoryWitnessStore()
	witnesses.Put("secret-001", 42)
	limiter := newTestProvingLimiter(t)
	observer := &workerProvingMetrics{durations: make(chan time.Duration, 1)}
	worker := &WorkerService{preimageProver: prover, witnessStore: witnesses, provingLimiter: limiter, metrics: observer}
	for _, payload := range []string{
		`{"witness_ref":"secret-001","digest":"1","secret":"PRIVATE_VALUE_MARKER"}`,
		`{"witness_ref":"secret-001","digest":"1","digest":"2"}`,
		`{"witness_ref":"secret-001","DIGEST":"1"}`,
		`{"witness_ref":"secret-001","digest":null}`,
		`{"witness_ref":"secret-001","digest":"PRIVATE_VALUE_MARKER"}`,
		`{"witness_ref":"secret-001","digest":"1"}null`,
		strings.Repeat("x", zk.MaxPreimagePayloadBytes+1),
	} {
		// Real prover + no duration samples prove malformed requests never prove.
		resp, err := worker.ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{JobId: 9901, AttemptId: 3, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 1000})
		if err != nil || resp == nil || resp.Status != "Failed" || resp.JobId != 9901 || resp.AttemptId != 3 || resp.Output != "" || len(observer.durations) != 0 {
			t.Fatal("malformed Worker request changed task-failure classification or proved")
		}
		if strings.Contains(resp.Error, "PRIVATE_VALUE_MARKER") {
			t.Fatal("Worker error disclosed private input")
		}
		if !limiter.TryAcquire() {
			t.Fatal("malformed payload retained admission slot")
		}
		limiter.Release()
	}
}
