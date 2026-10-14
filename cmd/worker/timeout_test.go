package main

import (
	"context"
	"fmt"
	"math"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestWorkerTimeoutValidationBeforeTaskConstruction(t *testing.T) {
	prover, _ := newTestPreimageProver(t)
	inner := runtime.NewMemoryWitnessStore()
	inner.Put("secret-001", 42)
	witnesses := &observedWitnessStore{store: inner}
	limiter := newTestProvingLimiter(t)
	observer := &workerProvingMetrics{durations: make(chan time.Duration, 1)}
	service := &WorkerService{preimageProver: prover, witnessStore: witnesses, provingLimiter: limiter, metrics: observer}
	_, client := shutdownWorker(t, service, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	payload := preimageTestPayload(t, "secret-001", zk.ComputePreimageDigest(42))
	for _, ms := range []int64{0, -1, math.MinInt64, 9223372036855, 1 << 58, 1<<58 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(ms), func(t *testing.T) {
			for _, taskType := range []string{"zk_preimage_prove", "unknown"} {
				response, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12001, AttemptId: 1, TaskType: taskType, Payload: payload, TimeoutMs: ms})
				// Unknown task would return a task Failed response if buildTask ran.
				if response != nil || status.Code(err) != codes.InvalidArgument {
					t.Fatalf("task=%s timeout=%d response=%v error=%v", taskType, ms, response, err)
				}
			}
		})
	}
	if witnesses.calls.Load() != 0 || len(observer.durations) != 0 {
		t.Fatal("invalid timeout reached witness lookup or proving")
	}
	if !limiter.TryAcquire() {
		t.Fatal("invalid timeout retained proving capacity")
	}
	limiter.Release()
	for _, ms := range []int64{5000, 9223372036854} {
		response, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12002, AttemptId: 2, TaskType: "hash", Payload: "hello", TimeoutMs: ms})
		if err != nil || response.GetStatus() != "Succeeded" || response.GetAttemptId() != 2 {
			t.Fatalf("valid timeout=%d response=%v error=%v", ms, response, err)
		}
	}
}

func TestWorkerTaskTimeoutIsCancelledResponse(t *testing.T) {
	_, client := shutdownWorker(t, &WorkerService{}, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	response, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12003, AttemptId: 3, TaskType: "sleep", Payload: "1h", TimeoutMs: 10})
	if err != nil || response.GetStatus() != "Cancelled" || response.GetOutput() != "" || response.GetError() != context.DeadlineExceeded.Error() || response.GetAttemptId() != 3 || ctx.Err() != nil {
		t.Fatalf("task timeout became an RPC error or Success: response=%v error=%v", response, err)
	}
}
