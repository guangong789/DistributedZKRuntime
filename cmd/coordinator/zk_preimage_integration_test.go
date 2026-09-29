package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
)

type observedPreimageVerifier struct {
	verifier *zk.PreimageVerifier
	calls    atomic.Int32
}

func (v *observedPreimageVerifier) Verify(proof []byte, digest fr.Element) error {
	v.calls.Add(1)
	return v.verifier.Verify(proof, digest)
}

func TestPreimageWitnessFileGRPCAndSQLite(t *testing.T) {
	const jobID int64 = 8001
	const workerID = "preimage-worker"
	dir := t.TempDir()
	provingKey := filepath.Join(dir, "proving.key")
	verifyingKey := filepath.Join(dir, "verifying.key")
	if err := zk.GeneratePreimageSetup(provingKey, verifyingKey); err != nil {
		t.Fatalf("generate preimage setup: %v", err)
	}
	prover, err := zk.LoadPreimageProver(provingKey)
	if err != nil {
		t.Fatalf("load preimage prover: %v", err)
	}
	realVerifier, err := zk.LoadPreimageVerifier(verifyingKey)
	if err != nil {
		t.Fatalf("load preimage verifier: %v", err)
	}
	verifier := &observedPreimageVerifier{verifier: realVerifier}

	witnessFile := filepath.Join(dir, "witnesses.json")
	if err := os.WriteFile(witnessFile, []byte(`{"secret-001":42,"secret-002":99}`), 0o600); err != nil {
		t.Fatalf("write private witnesses: %v", err)
	}
	witnesses, err := runtime.LoadMemoryWitnessStore(witnessFile)
	if err != nil {
		t.Fatalf("load private witnesses: %v", err)
	}

	store, err := NewSQLiteJobStore(filepath.Join(dir, "jobs.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	t.Cleanup(func() {
		if err := store.Close(); err != nil {
			t.Errorf("close SQLite store: %v", err)
		}
	})
	coordinator := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
	client := startTestCoordinator(t, coordinator)

	workerListener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for worker: %v", err)
	}
	workerServer := grpc.NewServer()
	var workerCalls atomic.Int32
	runtimepb.RegisterWorkerServiceServer(workerServer, &testWorker{
		execute: func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			workerCalls.Add(1)
			if req.JobId != jobID || req.AttemptId != 1 || req.TaskType != "zk_preimage_prove" {
				t.Errorf("unexpected worker request: %+v", req)
			}
			task := runtime.ZKPreimageTask{
				Payload: req.Payload, Prover: prover, WitnessStore: witnesses,
			}
			result := runtime.ExecuteJob(ctx, runtime.Job{
				ID: int(req.JobId), Task: task, Status: runtime.JobRunning,
			})
			errorText := ""
			if result.Err != nil {
				errorText = result.Err.Error()
			}
			return &runtimepb.ExecuteJobResponse{
				JobId: int64(result.JobID), AttemptId: req.AttemptId,
				Status: result.Status.String(), Output: result.Output, Error: errorText,
			}, nil
		},
	})
	go workerServer.Serve(workerListener)
	t.Cleanup(workerServer.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	registration, err := client.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{
		WorkerId: workerID, Address: workerListener.Addr().String(),
	})
	if err != nil || registration == nil || !registration.Accepted {
		t.Fatalf("register worker through gRPC: response=%+v error=%v", registration, err)
	}

	digest := zk.ComputePreimageDigest(42)
	payload := preimagePublicPayload(digest)
	var publicFields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &publicFields); err != nil {
		t.Fatalf("decode Coordinator-visible payload: %v", err)
	}
	if len(publicFields) != 2 || publicFields["witness_ref"] == nil || publicFields["digest"] == nil {
		t.Fatalf("payload exposes fields beyond witness_ref and digest: %v", publicFields)
	}
	resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
		JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 5000,
	})
	if err != nil || resp == nil || resp.JobId != jobID || resp.AttemptId != 1 ||
		resp.Status != "Succeeded" || resp.Error != "" {
		t.Fatalf("preimage SubmitJob: response=%+v error=%v", resp, err)
	}
	if workerCalls.Load() != 1 || verifier.calls.Load() != 1 {
		t.Fatalf("worker calls=%d verifier calls=%d, want one each", workerCalls.Load(), verifier.calls.Load())
	}
	proof, err := base64.StdEncoding.DecodeString(resp.Output)
	if err != nil || len(proof) == 0 {
		t.Fatalf("invalid returned proof: bytes=%d error=%v", len(proof), err)
	}

	record, ok, err := store.Load(jobID)
	if err != nil || !ok || record.JobID != jobID || record.State != JobSucceeded ||
		record.AttemptID != resp.AttemptId || record.WorkerID != workerID ||
		record.TaskType != "zk_preimage_prove" || record.Payload != payload || record.TimeoutMs != 5000 {
		t.Fatalf("persisted terminal record=%+v found=%v error=%v", record, ok, err)
	}
}
