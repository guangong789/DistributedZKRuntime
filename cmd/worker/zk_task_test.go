package main

import (
	"context"
	"encoding/base64"
	"path/filepath"
	"strings"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func newTestSquareProver(t *testing.T) (*zk.SquareProver, *zk.SquareVerifier) {
	t.Helper()

	dir := t.TempDir()
	provingKeyPath := filepath.Join(dir, "proving.key")
	verifyingKeyPath := filepath.Join(dir, "verifying.key")
	if err := zk.GenerateSquareSetup(provingKeyPath, verifyingKeyPath); err != nil {
		t.Fatalf("generate square setup: %v", err)
	}

	prover, err := zk.LoadSquareProver(provingKeyPath)
	if err != nil {
		t.Fatalf("load square prover: %v", err)
	}
	verifier, err := zk.LoadSquareVerifier(verifyingKeyPath)
	if err != nil {
		t.Fatalf("load square verifier: %v", err)
	}

	return prover, verifier
}

func verifySquareOutput(t *testing.T, verifier *zk.SquareVerifier, output string, y int64) {
	t.Helper()

	proof, err := base64.StdEncoding.DecodeString(output)
	if err != nil {
		t.Fatalf("decode Base64 proof: %v", err)
	}
	if len(proof) == 0 {
		t.Fatal("proof is empty")
	}
	if err := verifier.Verify(proof, y); err != nil {
		t.Fatalf("verify square proof: %v", err)
	}
}

func TestBuildZKSquareTaskExecutesAndVerifies(t *testing.T) {
	const payload = `{"x":5,"y":25}`
	prover, verifier := newTestSquareProver(t)
	task, err := buildTask("zk_square_prove", payload, prover)
	if err != nil {
		t.Fatalf("build task: %v", err)
	}
	zkTask, ok := task.(runtime.ZKSquareTask)
	if !ok || zkTask.Payload != payload || zkTask.Prover != prover {
		t.Fatalf("unexpected task: %#v", task)
	}

	result := runtime.ExecuteJob(context.Background(), runtime.Job{
		ID:      501,
		Timeout: 10 * time.Second,
		Task:    task,
		Status:  runtime.JobRunning,
	})
	if result.JobID != 501 || result.Status != runtime.JobSucceeded || result.Err != nil {
		t.Fatalf("unexpected result: %+v", result)
	}
	verifySquareOutput(t, verifier, result.Output, 25)
}

func TestBuildZKSquareTaskRejectsInvalidWitness(t *testing.T) {
	prover, _ := newTestSquareProver(t)
	task, err := buildTask("zk_square_prove", `{"x":5,"y":24}`, prover)
	if err != nil {
		t.Fatalf("build task: %v", err)
	}

	result := runtime.ExecuteJob(context.Background(), runtime.Job{ID: 502, Task: task})
	if result.Status != runtime.JobFailed || result.Err == nil || result.Output != "" {
		t.Fatalf("expected task failure without proof, got %+v", result)
	}
	if !strings.Contains(result.Err.Error(), "prove square") {
		t.Fatalf("expected proving error, got %v", result.Err)
	}
}

func TestBuildZKSquareTaskRejectsMalformedPayload(t *testing.T) {
	prover, _ := newTestSquareProver(t)
	task, err := buildTask("zk_square_prove", `{"x":`, prover)
	if err != nil {
		t.Fatalf("build task: %v", err)
	}

	result := runtime.ExecuteJob(context.Background(), runtime.Job{ID: 503, Task: task})
	if result.Status != runtime.JobFailed || result.Err == nil || result.Output != "" {
		t.Fatalf("expected task failure without proof, got %+v", result)
	}
	if !strings.Contains(result.Err.Error(), "decode zk square payload") {
		t.Fatalf("expected payload decoding error, got %v", result.Err)
	}
}

func TestZKSquareTaskHonorsCanceledContextBeforeProving(t *testing.T) {
	prover, _ := newTestSquareProver(t)
	task, err := buildTask("zk_square_prove", `{"x":5,"y":25}`, prover)
	if err != nil {
		t.Fatalf("build task: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	result := runtime.ExecuteJob(ctx, runtime.Job{ID: 504, Task: task})
	if result.Status != runtime.JobCancelled || result.Err != context.Canceled || result.Output != "" {
		t.Fatalf("expected cancellation before proving, got %+v", result)
	}
}

func TestBuildZKSquareTaskRejectsMissingProver(t *testing.T) {
	task, err := buildTask("zk_square_prove", `{"x":5,"y":25}`, nil)
	if task != nil || err == nil || !strings.Contains(err.Error(), "square prover is not configured") {
		t.Fatalf("expected missing-prover error, got task=%#v error=%v", task, err)
	}
}

func TestWorkerExecuteJobRunsZKSquareTask(t *testing.T) {
	prover, verifier := newTestSquareProver(t)
	resp, err := (&WorkerService{squareProver: prover}).ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{
		JobId:     505,
		AttemptId: 7,
		TaskType:  "zk_square_prove",
		Payload:   `{"x":5,"y":25}`,
		TimeoutMs: 10000,
	})
	if err != nil {
		t.Fatalf("ExecuteJob RPC: %v", err)
	}
	if resp.JobId != 505 || resp.AttemptId != 7 || resp.Status != "Succeeded" || resp.Error != "" {
		t.Fatalf("unexpected response: %+v", resp)
	}
	verifySquareOutput(t, verifier, resp.Output, 25)
}
