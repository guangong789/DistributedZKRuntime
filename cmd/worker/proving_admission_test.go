package main

import (
	"context"
	"encoding/base64"
	"net"
	"sync"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

func TestWorkerProvingAdmissionGRPC(t *testing.T) {
	prover, verifier := newTestPreimageProver(t)
	witnesses := runtime.NewMemoryWitnessStore()
	witnesses.Put("secret-001", 42)
	limiter := newTestProvingLimiter(t)
	observer := &workerProvingMetrics{durations: make(chan time.Duration, 2)}
	worker := &WorkerService{preimageProver: prover, witnessStore: witnesses, provingLimiter: limiter, metrics: observer}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := grpc.NewServer()
	runtimepb.RegisterWorkerServiceServer(server, worker)
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := runtimepb.NewWorkerServiceClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	digest := zk.ComputePreimageDigest(42)
	req := &runtimepb.ExecuteJobRequest{JobId: 9601, AttemptId: 1, TaskType: "zk_preimage_prove",
		Payload: preimageTestPayload(t, "secret-001", digest), TimeoutMs: 5000}
	if !limiter.TryAcquire() {
		t.Fatal("cannot occupy proving capacity")
	}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(limiter.Release) }
	defer release()
	resp, err := client.ExecuteJob(ctx, req)
	if resp != nil || status.Code(err) != codes.ResourceExhausted || len(observer.durations) != 0 {
		t.Fatalf("busy RPC: response=%v error=%v proving samples=%d", resp, err, len(observer.durations))
	}
	for _, tc := range []struct {
		taskType, payload, status string
	}{
		{"hash", "hello", "Succeeded"},
		{"sleep", "0s", "Succeeded"},
		{"hash", "", "Failed"},
		{"sleep", "invalid", "failed"},
		{"unknown", "", "failed"},
		{"zk_preimage_prove", "{", "Failed"},
	} {
		resp, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 9602, AttemptId: 2,
			TaskType: tc.taskType, Payload: tc.payload, TimeoutMs: 1000})
		if err != nil || resp == nil || resp.Status != tc.status || len(observer.durations) != 0 {
			t.Fatalf("full proving capacity changed task %s: response=%v error=%v", tc.taskType, resp, err)
		}
	}
	release()
	resp, err = client.ExecuteJob(ctx, req)
	if err != nil || resp == nil || resp.Status != "Succeeded" || resp.JobId != req.JobId || resp.AttemptId != req.AttemptId {
		t.Fatalf("released capacity rejected real proving: response=%v error=%v", resp, err)
	}
	proof, err := base64.StdEncoding.DecodeString(resp.Output)
	if err != nil {
		t.Fatal(err)
	}
	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatalf("limited real proof did not verify: %v", err)
	}
	if len(observer.durations) != 1 {
		t.Fatal("expected one observation for the single admitted Prove")
	}
	// A misconfigured Worker must not silently bypass the proving bound.
	resp, err = (&WorkerService{preimageProver: prover, witnessStore: witnesses}).ExecuteJob(ctx, req)
	if resp != nil || status.Code(err) != codes.FailedPrecondition {
		t.Fatalf("missing shared limiter: response=%v error=%v", resp, err)
	}
}
