package main

import (
	"context"
	"flag"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type disabledZKWitness struct{}

func (disabledZKWitness) Get(string) (int64, bool) { panic("disabled task accessed witness") }

func TestDisableZKWorkerRefusal(t *testing.T) {
	fs := flag.NewFlagSet("worker", flag.ContinueOnError)
	disabled := workerDisableZKFlag(fs)
	if *disabled || (&WorkerService{}).zkDisabled {
		t.Fatal("zero/default must keep ZK enabled")
	}
	if err := fs.Parse([]string{"-disable-zk"}); err != nil || !*disabled {
		t.Fatal("disable flag", err)
	}
	limiter, err := runtime.NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("cannot occupy permit")
	}
	defer limiter.Release()
	// A non-nil but uninitialized prover would panic if Prove were reached.
	w := &WorkerService{zkDisabled: true, preimageProver: &zk.PreimageProver{}, witnessStore: disabledZKWitness{}, provingLimiter: limiter}
	req := &runtimepb.ExecuteJobRequest{JobId: 42, AttemptId: 7, TaskType: "zk_preimage_prove", Payload: `{"secret":"PRIVATE_MARKER"}`, TimeoutMs: 1000}
	resp, err := w.ExecuteJob(context.Background(), req)
	if err != nil || resp == nil || resp.Status != "Failed" || resp.JobId != 42 || resp.AttemptId != 7 || resp.Output != "" || resp.Error != "ZK tasks are disabled" {
		t.Fatalf("refusal=%v error=%v", resp, err)
	}
	req.TimeoutMs = 0
	if _, err := w.ExecuteJob(context.Background(), req); status.Code(err) != codes.InvalidArgument {
		t.Fatal("timeout validation precedence changed", err)
	}
	for _, tc := range []struct{ kind, payload string }{{"hash", "hello"}, {"sleep", "1ms"}} {
		resp, err := w.ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{JobId: 43, AttemptId: 8, TaskType: tc.kind, Payload: tc.payload, TimeoutMs: 1000})
		if err != nil || resp.Status != "Succeeded" || resp.JobId != 43 || resp.AttemptId != 8 {
			t.Fatalf("ordinary task: %v %v", resp, err)
		}
	}
	if limiter.TryAcquire() {
		limiter.Release()
		t.Fatal("disabled task released somebody else's permit")
	}
	resp, err = (&WorkerService{}).ExecuteJob(context.Background(), &runtimepb.ExecuteJobRequest{TaskType: "zk_preimage_prove", TimeoutMs: 1000})
	if err != nil || !strings.Contains(resp.Error, "preimage prover is not configured") {
		t.Fatal("zero service no longer uses enabled construction", resp, err)
	}
}

func TestDisableZKDefaultWorkerStillRequiresProver(t *testing.T) {
	files := testpki.New(t).Issue(t, testpki.Worker, nil)
	for _, malformed := range []bool{false, true} {
		path := filepath.Join(t.TempDir(), "proving.key")
		if malformed {
			if err := os.WriteFile(path, []byte("invalid key"), 0600); err != nil {
				t.Fatal(err)
			}
		}
		cfg := workerConfig{ID: "worker-a", ListenAddress: "127.0.0.1:0", AdvertiseAddress: "localhost:50051", CoordinatorAddress: "localhost:50050",
			ProvingKeyPath: path, WitnessFilePath: filepath.Join(t.TempDir(), "missing-witness"), MaxConcurrentProofs: 1, ShutdownTimeout: time.Second,
			Security: rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}}
		err := runWorker(context.Background(), cfg)
		if err == nil || !strings.Contains(err.Error(), "load preimage prover") {
			t.Fatal("enabled startup accepted invalid prover", err)
		}
	}
}
