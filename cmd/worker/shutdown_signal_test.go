package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

type workerSignalOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	ready  chan struct{}
	once   sync.Once
}

func (w *workerSignalOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buffer.Write(p)
	if strings.Contains(w.buffer.String(), "worker registered successfully") {
		w.once.Do(func() { close(w.ready) })
	}
	return n, err
}
func (w *workerSignalOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

func TestWorkerSignalSubprocess(t *testing.T) {
	if os.Getenv("DZKR_TEST_WORKER_CHILD") == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("DZKR_TEST_WORKER_ARGS")), &args); err != nil {
			t.Fatal(err)
		}
		flag.CommandLine = flag.NewFlagSet("worker", flag.ExitOnError)
		os.Args = append([]string{os.Args[0]}, args...)
		main()
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM test requires Unix")
	}
	service := shutdownCoordinator{
		register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
			return &runtimepb.RegisterWorkerResponse{Accepted: true}, nil
		},
		heartbeat: func(context.Context, *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
			return &runtimepb.HeartbeatResponse{Accepted: true}, nil
		},
	}
	address, ca, _, _, _ := startShutdownCoordinator(t, service)
	cfg := workerTestConfig(t, ca, address)
	args := []string{"-id", cfg.ID, "-listen", cfg.ListenAddress, "-advertise", cfg.AdvertiseAddress, "-coordinator", cfg.CoordinatorAddress,
		"-proving-key", cfg.ProvingKeyPath, "-witness-file", cfg.WitnessFilePath, "-metrics-listen", cfg.MetricsAddress, "-shutdown-timeout", "2s",
		"-tls-cert", cfg.Security.CertFile, "-tls-key", cfg.Security.KeyFile, "-tls-ca", cfg.Security.CAFile}
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 12*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestWorkerSignalSubprocess$", "-test.v")
	cmd.Env = append(os.Environ(), "DZKR_TEST_WORKER_CHILD=1", "DZKR_TEST_WORKER_ARGS="+string(encoded))
	output := &workerSignalOutput{ready: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	exited := make(chan struct{})
	go func() { defer close(exited); finished <- cmd.Wait() }()
	defer func() { cancel(); awaitWorker(t, exited) }()
	select {
	case <-output.ready:
	case err := <-finished:
		t.Fatalf("Worker startup: %v\n%s", err, output.String())
	case <-ctx.Done():
		awaitWorker(t, finished)
		t.Fatalf("Worker startup timeout\n%s", output.String())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = cmd.Process.Kill()
		awaitWorker(t, finished)
		t.Fatal(err)
	}
	if err := awaitWorker(t, finished); err != nil {
		t.Fatalf("Worker SIGTERM: %v\n%s", err, output.String())
	}
}
