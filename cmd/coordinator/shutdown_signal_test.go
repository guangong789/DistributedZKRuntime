package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

type coordinatorSignalOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	ready  chan struct{}
	once   sync.Once
}

func (w *coordinatorSignalOutput) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	n, err := w.buffer.Write(p)
	if strings.Contains(w.buffer.String(), "coordinator mTLS listening on ") {
		w.once.Do(func() { close(w.ready) })
	}
	return n, err
}
func (w *coordinatorSignalOutput) String() string {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.buffer.String()
}

// Use the current test executable to run the actual main/signal wiring. No
// external binaries or deployment credentials are required.
func TestCoordinatorSignalSubprocess(t *testing.T) {
	if os.Getenv("DZKR_TEST_COORDINATOR_CHILD") == "1" {
		var args []string
		if err := json.Unmarshal([]byte(os.Getenv("DZKR_TEST_COORDINATOR_ARGS")), &args); err != nil {
			t.Fatal(err)
		}
		flag.CommandLine = flag.NewFlagSet("coordinator", flag.ExitOnError)
		os.Args = append([]string{os.Args[0]}, args...)
		main()
		return
	}
	if runtime.GOOS == "windows" {
		t.Skip("SIGTERM subprocess check requires Unix")
	}
	dir := t.TempDir()
	pk, vk := filepath.Join(dir, "proving.key"), filepath.Join(dir, "verifying.key")
	if err := zk.GeneratePreimageSetup(pk, vk); err != nil {
		t.Fatal(err)
	}
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Coordinator, nil)
	dbPath := filepath.Join(dir, "jobs.db")
	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	want := JobRecord{JobID: 9708, State: JobFailed, AttemptID: 2, WorkerID: "old-worker", TaskType: "hash", Payload: "preserved", TimeoutMs: 1000}
	saveRecoveryRecord(t, store, want)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	args := []string{"-db", dbPath, "-verifying-key", vk, "-listen", "127.0.0.1:0", "-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "2s", "-tls-cert", files.Cert, "-tls-key", files.Key, "-tls-ca", files.CA, "-allow-client", testpki.Client, "-allow-worker", testpki.Worker}
	encoded, err := json.Marshal(args)
	if err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, executable, "-test.run=^TestCoordinatorSignalSubprocess$", "-test.v")
	cmd.Env = append(os.Environ(), "DZKR_TEST_COORDINATOR_CHILD=1", "DZKR_TEST_COORDINATOR_ARGS="+string(encoded))
	output := &coordinatorSignalOutput{ready: make(chan struct{})}
	cmd.Stdout, cmd.Stderr = output, output
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	finished := make(chan error, 1)
	go func() { finished <- cmd.Wait() }()
	select {
	case <-output.ready:
	case err := <-finished:
		t.Fatalf("startup failed: %v\n%s", err, output.String())
	case <-ctx.Done():
		<-finished
		t.Fatalf("startup timed out\n%s", output.String())
	}
	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		_ = cmd.Process.Kill()
		<-finished
		t.Fatal(err)
	}
	if err := <-finished; err != nil {
		t.Fatalf("signal shutdown failed: %v\n%s", err, output.String())
	}
	loadShutdownRecord(t, dbPath, want)
}
