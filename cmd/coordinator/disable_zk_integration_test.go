package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	goruntime "runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type nonZKProcessOutput struct {
	mu     sync.Mutex
	buffer bytes.Buffer
	token  string
	ready  chan struct{}
	once   sync.Once
}

func (o *nonZKProcessOutput) Write(p []byte) (int, error) {
	o.mu.Lock()
	defer o.mu.Unlock()
	n, err := o.buffer.Write(p)
	if start := strings.Index(o.buffer.String(), o.token); start >= 0 && strings.Contains(o.buffer.String()[start:], "\n") {
		o.once.Do(func() { close(o.ready) })
	}
	return n, err
}
func (o *nonZKProcessOutput) text() string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.buffer.String()
}

func startNonZKProcess(t *testing.T, ctx context.Context, bin string, args []string, token string) *nonZKProcessOutput {
	t.Helper()
	out := &nonZKProcessOutput{token: token, ready: make(chan struct{})}
	cmd := exec.CommandContext(ctx, bin, args...)
	cmd.Stdout, cmd.Stderr = out, out
	cmd.WaitDelay = 2 * time.Second
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	var waitErr error
	go func() { waitErr = cmd.Wait(); close(done) }()
	t.Cleanup(func() {
		select {
		case <-done:
			if waitErr != nil {
				t.Errorf("process failed: %v\n%s", waitErr, out.text())
			}
			return
		default:
		}
		_ = cmd.Process.Signal(os.Interrupt)
		select {
		case <-done:
			if waitErr != nil {
				t.Errorf("shutdown failed: %v\n%s", waitErr, out.text())
			}
		case <-time.After(8 * time.Second):
			_ = cmd.Process.Kill()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				t.Error("process was not reaped")
			}
			t.Errorf("shutdown deadline exceeded\n%s", out.text())
		}
	})
	select {
	case <-out.ready:
	case <-done:
		t.Fatalf("startup failed: %v\n%s", waitErr, out.text())
	case <-ctx.Done():
		t.Fatalf("startup deadline: %v\n%s", ctx.Err(), out.text())
	}
	return out
}

func TestDisableZKProductionStartupMTLS(t *testing.T) {
	if goruntime.GOOS == "windows" {
		t.Skip("owned process signal cleanup requires Unix")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancel)
	dir := t.TempDir()
	// Offline build only: no setup, proving, benchmarks or deployment fixtures.
	buildCtx, stopBuild := context.WithTimeout(ctx, 60*time.Second)
	defer stopBuild()
	build := exec.CommandContext(buildCtx, "go", "build", "-mod=readonly", "-p=1", "-o", dir+string(os.PathSeparator), "./cmd/coordinator", "./cmd/worker")
	build.Dir = filepath.Join("..", "..")
	build.Env = append(os.Environ(), "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOMAXPROCS=2")
	build.WaitDelay = 2 * time.Second
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("offline production build: %v\n%s", err, output)
	}
	ca := testpki.New(t)
	coordFiles := ca.Issue(t, testpki.Coordinator, nil)
	workerFiles := ca.Issue(t, testpki.Worker, nil)
	clientFiles := ca.Issue(t, testpki.Client, nil)
	dbPath := filepath.Join(dir, "jobs.db")
	// Terminal ZK must not block a real disabled startup.
	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatal(err)
	}
	terminal := JobRecord{JobID: 99, State: JobSucceeded, AttemptID: 7, WorkerID: "old", TaskType: "zk_preimage_prove", Payload: "preserved", TimeoutMs: 1000}
	if err := store.Save(terminal); err != nil {
		t.Fatal(err)
	}
	recoverable := []JobRecord{
		{JobID: 20, State: JobRunning, AttemptID: 5, WorkerID: "old", TaskType: "hash", Payload: "recover", TimeoutMs: 1000},
		{JobID: 21, State: JobRecovering, AttemptID: 6, WorkerID: "old", TaskType: "sleep", Payload: "1ms", TimeoutMs: 1000},
	}
	for _, record := range recoverable {
		if err := store.Save(record); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	missing := filepath.Join(dir, "no-zk-material")
	coordOut := startNonZKProcess(t, ctx, filepath.Join(dir, "coordinator"), []string{
		"-disable-zk", "-listen", "127.0.0.1:0", "-db", dbPath, "-verifying-key", missing,
		"-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "3s",
		"-tls-cert", coordFiles.Cert, "-tls-key", coordFiles.Key, "-tls-ca", coordFiles.CA,
		"-allow-client", testpki.Client, "-allow-worker", testpki.Worker}, "coordinator mTLS listening on ")
	var coordinatorAddress, metricsURL string
	for _, line := range strings.Split(coordOut.text(), "\n") {
		if strings.HasPrefix(line, "coordinator metrics listening on ") {
			metricsURL = strings.TrimPrefix(line, "coordinator metrics listening on ")
		}
		if strings.HasPrefix(line, "coordinator mTLS listening on ") {
			coordinatorAddress = strings.TrimPrefix(line, "coordinator mTLS listening on ")
		}
	}
	if coordinatorAddress == "" {
		t.Fatal("missing Coordinator bound endpoint")
	}
	// Worker needs its advertised port before startup. Ask the OS for an unused
	// ephemeral port; a competing bind fails startup explicitly rather than retrying.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	workerAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	startNonZKProcess(t, ctx, filepath.Join(dir, "worker"), []string{
		"-disable-zk", "-id", "worker-a", "-listen", workerAddress, "-advertise", workerAddress,
		"-coordinator", coordinatorAddress, "-proving-key", missing, "-witness-file", missing,
		"-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "3s",
		"-tls-cert", workerFiles.Cert, "-tls-key", workerFiles.Key, "-tls-ca", workerFiles.CA}, "worker registered successfully")
	principal, err := rpcauth.ParsePrincipal(testpki.Client)
	if err != nil {
		t.Fatal(err)
	}
	creds, err := (rpcauth.CoordinatorClientConfig{CertFile: clientFiles.Cert, KeyFile: clientFiles.Key, CAFile: clientFiles.CA, ServerIdentity: testpki.Coordinator}).Credentials(coordinatorAddress, principal)
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient(coordinatorAddress, grpc.WithTransportCredentials(creds))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	client := runtimepb.NewCoordinatorServiceClient(conn)
	// Observe recovery completion without competing SQLite reads during writes.
	// These counters are emitted only after the terminal Save has succeeded.
	if !strings.HasPrefix(metricsURL, "http://127.0.0.1:") {
		t.Fatal("missing loopback metrics endpoint")
	}
	recoveryCtx, stopRecovery := context.WithTimeout(ctx, 5*time.Second)
	defer stopRecovery()
	transport := &http.Transport{}
	defer transport.CloseIdleConnections()
	httpClient := &http.Client{Transport: transport, Timeout: time.Second}
	tick := time.NewTicker(10 * time.Millisecond)
	defer tick.Stop()
	for {
		request, err := http.NewRequestWithContext(recoveryCtx, http.MethodGet, metricsURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		response, err := httpClient.Do(request)
		if err != nil {
			t.Fatal(err)
		}
		data, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
		response.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if response.StatusCode != http.StatusOK {
			t.Fatal("metrics status", response.StatusCode)
		}
		if strings.Contains(string(data), `dzkr_jobs_terminal_total{source="recovery",state="succeeded"} 2`) {
			break
		}
		select {
		case <-tick.C:
		case <-recoveryCtx.Done():
			t.Fatal("ordinary startup recovery did not finish", coordOut.text())
		}
	}
	audit, err := sql.Open("sqlite", "file:"+filepath.ToSlash(dbPath)+"?mode=ro")
	if err != nil {
		t.Fatal(err)
	}
	reopened := &SQLiteJobStore{db: audit}
	t.Cleanup(func() { _ = reopened.Close() })
	for _, record := range recoverable {
		record.State = JobSucceeded
		record.AttemptID++
		record.WorkerID = "worker-a"
		assertSQLiteJobRecord(t, reopened, record)
	}
	for i, tc := range []struct{ kind, payload, want string }{{"hash", "hello", ""}, {"sleep", "1ms", "slept for 1ms"}} {
		req := &runtimepb.SubmitJobRequest{JobId: int64(i + 1), TaskType: tc.kind, Payload: tc.payload, TimeoutMs: 1000}
		rpcCtx, stop := context.WithTimeout(ctx, 5*time.Second)
		resp, err := client.SubmitJob(rpcCtx, req)
		stop()
		if err != nil || resp.GetStatus() != "Succeeded" || resp.GetJobId() != req.JobId || resp.GetAttemptId() != 1 || resp.GetError() != "" {
			t.Fatalf("%s response=%v error=%v", tc.kind, resp, err)
		}
		want := tc.want
		if tc.kind == "hash" {
			sum := sha256.Sum256([]byte(tc.payload))
			want = hex.EncodeToString(sum[:])
		}
		if resp.Output != want {
			t.Fatalf("%s output=%q want=%q", tc.kind, resp.Output, want)
		}
		assertSQLiteJobRecord(t, reopened, JobRecord{JobID: req.JobId, State: JobSucceeded, AttemptID: 1, WorkerID: "worker-a", TaskType: tc.kind, Payload: tc.payload, TimeoutMs: 1000})
	}
	rpcCtx, stop := context.WithTimeout(ctx, 5*time.Second)
	defer stop()
	if _, err := client.SubmitJob(rpcCtx, &runtimepb.SubmitJobRequest{JobId: 3, TaskType: "zk_preimage_prove", Payload: "PRIVATE_MARKER", TimeoutMs: 1000}); status.Code(err) != codes.FailedPrecondition {
		t.Fatal("disabled admission", err)
	}
	if _, ok, err := reopened.Load(3); err != nil || ok {
		t.Fatal("rejected job persisted", err)
	}
	assertSQLiteJobRecord(t, reopened, terminal)
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("unexpected ZK artifact creation", err)
	}
	// An enabled Coordinator selecting this disabled real Worker receives a task
	// failure, not a retryable RPC error. No verifying key is needed for Failed.
	coordPrincipal, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	material, err := rpcauth.LoadMaterial(coordFiles.Cert, coordFiles.Key, coordFiles.CA, coordPrincipal)
	if err != nil {
		t.Fatal(err)
	}
	mismatchStore := NewMemoryJobStore()
	enabled := &CoordinatorServer{jobStore: mismatchStore, workerCredentials: func(w WorkerInfo) (credentials.TransportCredentials, error) {
		return material.WorkerCredentials(w.ID, w.Address)
	}}
	if _, err := enabled.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{WorkerId: "worker-a", Address: workerAddress}); err != nil {
		t.Fatal(err)
	}
	resp, err := enabled.SubmitJob(rpcCtx, &runtimepb.SubmitJobRequest{JobId: 4, TaskType: "zk_preimage_prove", Payload: `{"witness_ref":"demo","digest":"1"}`, TimeoutMs: 1000})
	if err != nil || resp.GetStatus() != "Failed" || resp.GetAttemptId() != 1 || resp.GetError() != "ZK tasks are disabled" {
		t.Fatalf("mismatch=%v error=%v", resp, err)
	}
	record, ok, err := mismatchStore.Load(4)
	if err != nil || !ok || record.State != JobFailed || record.AttemptID != 1 || enabled.jobAttempts[4] != 1 {
		t.Fatal("mismatch retried or persisted success", record, err)
	}
	t.Log("production Coordinator + Worker: mTLS hash/sleep succeeded, SQLite AttemptID=1; ZK rejected without artifacts; mismatched Worker failed at AttemptID=1")
}
