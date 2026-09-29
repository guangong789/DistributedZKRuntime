package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func newTestPreimageProver(t *testing.T) (*zk.PreimageProver, *zk.PreimageVerifier) {
	t.Helper()

	dir := t.TempDir()
	provingKeyPath := filepath.Join(dir, "proving.key")
	verifyingKeyPath := filepath.Join(dir, "verifying.key")
	if err := zk.GeneratePreimageSetup(provingKeyPath, verifyingKeyPath); err != nil {
		t.Fatalf("generate preimage setup: %v", err)
	}
	prover, err := zk.LoadPreimageProver(provingKeyPath)
	if err != nil {
		t.Fatalf("load preimage prover: %v", err)
	}
	verifier, err := zk.LoadPreimageVerifier(verifyingKeyPath)
	if err != nil {
		t.Fatalf("load preimage verifier: %v", err)
	}
	return prover, verifier
}

func preimageTestPayload(t *testing.T, witnessRef string, digest fr.Element) string {
	t.Helper()
	var digestInt big.Int
	digest.BigInt(&digestInt)
	payloadBytes, err := json.Marshal(runtime.ZKPreimagePayload{
		WitnessRef: witnessRef,
		Digest:     digestInt.String(),
	})
	if err != nil {
		t.Fatalf("marshal public task payload: %v", err)
	}
	return string(payloadBytes)
}

func TestBuildZKPreimageTaskRequiresDependencies(t *testing.T) {
	const payload = `{"witness_ref":"secret-001","digest":"123"}`
	prover, _ := newTestPreimageProver(t)
	store := runtime.NewMemoryWitnessStore()

	t.Run("missing prover", func(t *testing.T) {
		task, err := buildTask("zk_preimage_prove", payload, nil, store)
		if task != nil || err == nil || !strings.Contains(err.Error(), "preimage prover is not configured") {
			t.Fatalf("expected missing-prover error, got task=%#v error=%v", task, err)
		}
	})
	t.Run("missing witness store", func(t *testing.T) {
		task, err := buildTask("zk_preimage_prove", payload, prover, nil)
		if task != nil || err == nil || !strings.Contains(err.Error(), "witness store is not configured") {
			t.Fatalf("expected missing-store error, got task=%#v error=%v", task, err)
		}
	})
}

func TestBuildZKPreimageTaskExecutesAndVerifies(t *testing.T) {
	prover, verifier := newTestPreimageProver(t)
	store := runtime.NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	digest := zk.ComputePreimageDigest(42)
	payload := preimageTestPayload(t, "secret-001", digest)

	task, err := buildTask("zk_preimage_prove", payload, prover, store)
	if err != nil {
		t.Fatalf("build task: %v", err)
	}
	preimageTask, ok := task.(runtime.ZKPreimageTask)
	if !ok || preimageTask.Payload != payload ||
		preimageTask.Prover != prover || preimageTask.WitnessStore != store {
		t.Fatalf("unexpected task: %#v", task)
	}

	result := runtime.ExecuteJob(context.Background(), runtime.Job{ID: 601, Task: task})
	if result.JobID != 601 || result.Status != runtime.JobSucceeded || result.Err != nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	proof, err := base64.StdEncoding.DecodeString(result.Output)
	if err != nil {
		t.Fatalf("decode Base64 proof: %v", err)
	}
	if len(proof) == 0 {
		t.Fatal("proof is empty")
	}
	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatalf("verify preimage proof: %v", err)
	}
}

func TestWorkerExecuteJobRunsZKPreimageTask(t *testing.T) {
	prover, verifier := newTestPreimageProver(t)
	store := runtime.NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	digest := zk.ComputePreimageDigest(42)
	payload := preimageTestPayload(t, "secret-001", digest)

	resp, err := (&WorkerService{
		preimageProver: prover,
		witnessStore:   store,
	}).ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{
		JobId:     602,
		AttemptId: 8,
		TaskType:  "zk_preimage_prove",
		Payload:   payload,
		TimeoutMs: 5000,
	})
	if err != nil {
		t.Fatalf("execute job: %v", err)
	}
	if resp == nil || resp.JobId != 602 || resp.AttemptId != 8 || resp.Status != "Succeeded" || resp.Error != "" {
		t.Fatalf("unexpected worker response: %+v", resp)
	}
	proof, err := base64.StdEncoding.DecodeString(resp.Output)
	if err != nil {
		t.Fatalf("decode Base64 proof: %v", err)
	}
	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatalf("verify worker proof: %v", err)
	}
}

func TestBuildZKPreimageTaskWithLoadedWitnessFile(t *testing.T) {
	prover, verifier := newTestPreimageProver(t)
	path := filepath.Join(t.TempDir(), "witnesses.json")
	if err := os.WriteFile(path, []byte(`{"secret-001":42,"secret-002":99}`), 0o600); err != nil {
		t.Fatalf("write witness file: %v", err)
	}
	store, err := runtime.LoadMemoryWitnessStore(path)
	if err != nil {
		t.Fatalf("load witness file: %v", err)
	}

	digest := zk.ComputePreimageDigest(99)
	payload := preimageTestPayload(t, "secret-002", digest)
	task, err := buildTask("zk_preimage_prove", payload, prover, store)
	if err != nil {
		t.Fatalf("build task: %v", err)
	}
	result := runtime.ExecuteJob(context.Background(), runtime.Job{ID: 603, Task: task})
	if result.JobID != 603 || result.Status != runtime.JobSucceeded || result.Err != nil {
		t.Fatalf("loaded witness execution: %+v", result)
	}
	proof, err := base64.StdEncoding.DecodeString(result.Output)
	if err != nil {
		t.Fatalf("decode Base64 proof: %v", err)
	}
	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatalf("verify proof for secret-002: %v", err)
	}
}
