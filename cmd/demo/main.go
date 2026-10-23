// Command demo runs a temporary, authenticated hash-only Coordinator/Worker pair.
package main

import (
	"context"
	"errors"
	"fmt"
	"go/version"
	"io"
	"net"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
)

const clientIdentity = "spiffe://distributedzk.local/client/client-1"
const workerIdentity = "spiffe://distributedzk.local/worker/worker-1"

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	ctx, cancel := context.WithTimeout(ctx, 150*time.Second)
	defer cancel()
	if err := runDemo(ctx, os.Stdout); err != nil {
		fmt.Fprintln(os.Stderr, "Demo failed:", err)
		os.Exit(1)
	}
}

func offlineEnv() []string {
	env := make([]string, 0, len(os.Environ())+5)
	for _, item := range os.Environ() {
		name, _, _ := strings.Cut(item, "=")
		switch name {
		case "GOTOOLCHAIN", "GOPROXY", "GOSUMDB", "GOMAXPROCS", "GOFLAGS":
			continue
		}
		env = append(env, item)
	}
	return append(env, "GOTOOLCHAIN=local", "GOPROXY=off", "GOSUMDB=off", "GOMAXPROCS=2", "GOFLAGS=")
}

func prerequisites(ctx context.Context, repo string) error {
	if runtime.GOOS != "linux" {
		return errors.New("this demo requires Linux/WSL and GNU utilities")
	}
	for _, tool := range []string{"go", "bash", "openssl", "git", "realpath", "mktemp", "stat", "cmp", "ln", "chmod", "mkdir", "rm", "grep", "dirname"} {
		if _, err := exec.LookPath(tool); err != nil {
			return fmt.Errorf("missing prerequisite: %s", tool)
		}
	}
	data, err := os.ReadFile(filepath.Join(repo, "go.mod"))
	if err != nil {
		return errors.New("run the demo from the DistributedZKRuntime repository root")
	}
	if !strings.HasPrefix(string(data), "module github.com/guangong789/DistributedZKRuntime\n") {
		return errors.New("unexpected repository module")
	}
	required := ""
	for _, line := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(line, "go ") {
			required = "go" + strings.TrimSpace(strings.TrimPrefix(line, "go "))
		}
	}
	checks := []struct {
		program string
		args    []string
		check   func(string) bool
	}{
		{"go", []string{"env", "GOVERSION"}, func(s string) bool {
			return version.IsValid(required) && version.Compare(strings.TrimSpace(s), required) >= 0
		}},
		{"openssl", []string{"version"}, func(s string) bool { return strings.HasPrefix(s, "OpenSSL 3.") }},
	}
	for _, tool := range []string{"realpath", "mktemp", "stat", "cmp", "ln", "chmod", "mkdir", "rm", "grep", "dirname"} {
		checks = append(checks, struct {
			program string
			args    []string
			check   func(string) bool
		}{tool, []string{"--version"}, func(s string) bool { return strings.Contains(s, "GNU") }})
	}
	for _, check := range checks {
		cmd := exec.CommandContext(ctx, check.program, check.args...)
		cmd.Env = offlineEnv()
		if output, err := cmd.Output(); err != nil || !check.check(string(output)) {
			return fmt.Errorf("prerequisite version check failed: %s", check.program)
		}
	}
	info, err := os.Stat(filepath.Join(repo, "scripts/dev-pki/bootstrap.sh"))
	if err != nil || !info.Mode().IsRegular() {
		return errors.New("development PKI bootstrap script missing")
	}
	return ctx.Err()
}

func stage(ctx context.Context, duration time.Duration, fn func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, duration)
	defer cancel()
	return fn(ctx)
}

func connect(ctx context.Context, address, pki string, coordinator *child) (*grpc.ClientConn, error) {
	principal, err := rpcauth.ParsePrincipal(clientIdentity)
	if err != nil {
		return nil, err
	}
	creds, err := (rpcauth.CoordinatorClientConfig{
		CertFile: filepath.Join(pki, "client-1/cert.pem"), KeyFile: filepath.Join(pki, "client-1/key.pem"),
		CAFile: filepath.Join(pki, "ca/ca.crt"), ServerIdentity: rpcauth.CoordinatorIdentity,
	}).Credentials(address, principal)
	if err != nil {
		return nil, err
	}
	conn, err := grpc.NewClient(address, grpc.WithTransportCredentials(creds), grpc.WithPerRPCCredentials(rpcauth.PeerValidityCredentials{}))
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		select {
		case <-coordinator.done:
			cancel()
		case <-ctx.Done():
		}
	}()
	conn.Connect()
	for {
		state := conn.GetState()
		if state == connectivity.Ready {
			if err := coordinator.alive(); err == nil {
				return conn, nil
			}
			break
		}
		if !conn.WaitForStateChange(ctx, state) {
			break
		}
	}
	conn.Close()
	return nil, fmt.Errorf("authenticated Coordinator readiness failed: %w", errors.Join(ctx.Err(), coordinator.alive()))
}

func runDemo(ctx context.Context, out io.Writer) (result error) {
	fmt.Fprintln(out, "DistributedZKRuntime | Local Distributed Demo\nReal Coordinator + Worker | gRPC/mTLS + SQLite\n\nInfrastructure")
	repo, err := os.Getwd()
	if err != nil {
		return err
	}
	if err := stage(ctx, 10*time.Second, func(ctx context.Context) error { return prerequisites(ctx, repo) }); err != nil {
		return err
	}
	// Never honor a TMPDIR that could place private demo material in the repo.
	workspace, err := os.MkdirTemp("/tmp", "dzkr-demo-")
	if err != nil {
		return err
	}
	// MkdirTemp uses 0700. Resolve /tmp aliases before invoking the PKI script,
	// which deliberately refuses symlink components in credential output paths.
	workspace, err = filepath.EvalSymlinks(workspace)
	if err != nil {
		return err
	}
	var children []*child
	defer func() {
		var cleanupErr error
		for i := len(children) - 1; i >= 0; i-- {
			cleanupErr = errors.Join(cleanupErr, children[i].stop())
		}
		result = errors.Join(result, cleanupErr)
		if result == nil {
			result = os.RemoveAll(workspace)
			if result == nil {
				fmt.Fprintln(out, "\nCleanup\n  [6/6] [OK] Owned children reaped; temporary workspace removed\n\nDemo completed successfully.")
			}
		}
		if result != nil {
			result = fmt.Errorf("%w; isolated diagnostics retained at %s (contains temporary development PKI)", result, workspace)
		}
	}()
	pki := filepath.Join(workspace, "pki")
	if err := stage(ctx, 30*time.Second, func(ctx context.Context) error {
		return runTool(ctx, repo, filepath.Join(workspace, "pki.log"), "bash", filepath.Join(repo, "scripts/dev-pki/bootstrap.sh"), "--out", pki)
	}); err != nil {
		return fmt.Errorf("PKI preparation: %w", err)
	}
	if err := stage(ctx, 60*time.Second, func(ctx context.Context) error {
		return runTool(ctx, repo, filepath.Join(workspace, "build.log"), "go", "build", "-mod=readonly", "-p=1", "-o", workspace+string(os.PathSeparator), "./cmd/coordinator", "./cmd/worker")
	}); err != nil {
		return fmt.Errorf("offline build: %w", err)
	}
	fmt.Fprintln(out, "  [1/6] [OK] Prerequisites, isolated PKI and offline build")
	tlsArgs := func(node string) []string {
		return []string{"-tls-cert", filepath.Join(pki, node, "cert.pem"), "-tls-key", filepath.Join(pki, node, "key.pem"), "-tls-ca", filepath.Join(pki, "ca/ca.crt")}
	}
	db := filepath.Join(workspace, "jobs.db")
	args := append([]string{"-disable-zk=true", "-listen", "127.0.0.1:0", "-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "3s", "-db", db, "-allow-client", clientIdentity, "-allow-worker", workerIdentity}, tlsArgs("coordinator")...)
	c, err := startChild(repo, filepath.Join(workspace, "coordinator.log"), "coordinator mTLS listening on ", filepath.Join(workspace, "coordinator"), args...)
	if err != nil {
		return fmt.Errorf("Coordinator start: %w", err)
	}
	children = append(children, c)
	var conn *grpc.ClientConn
	var address string
	if err := stage(ctx, 10*time.Second, func(ctx context.Context) error {
		line, err := c.waitReady(ctx)
		if err != nil {
			return err
		}
		address = strings.TrimPrefix(line, "coordinator mTLS listening on ")
		host, port, err := net.SplitHostPort(address)
		if err != nil || host != "127.0.0.1" || port == "0" {
			return errors.New("Coordinator reported invalid loopback endpoint")
		}
		conn, err = connect(ctx, address, pki, c)
		return err
	}); err != nil {
		return fmt.Errorf("Coordinator readiness: %w", err)
	}
	defer conn.Close()
	fmt.Fprintln(out, "  [2/6] [OK] Coordinator authenticated gRPC READY")
	// Worker registration requires its advertised port before startup. Reserve
	// an ephemeral port, then release it; any competing bind fails this run.
	reservation, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	workerAddress := reservation.Addr().String()
	if err := reservation.Close(); err != nil {
		return err
	}
	args = append([]string{"-disable-zk=true", "-id", "worker-1", "-listen", workerAddress, "-advertise", workerAddress, "-coordinator", address, "-metrics-listen", "127.0.0.1:0", "-shutdown-timeout", "3s"}, tlsArgs("worker-1")...)
	w, err := startChild(repo, filepath.Join(workspace, "worker.log"), "worker registered successfully", filepath.Join(workspace, "worker"), args...)
	if err != nil {
		return fmt.Errorf("Worker start: %w", err)
	}
	children = append(children, w)
	if err := stage(ctx, 10*time.Second, func(ctx context.Context) error { _, err := w.waitReady(ctx); return err }); err != nil {
		return fmt.Errorf("Worker registration: %w", err)
	}
	fmt.Fprintln(out, "  [3/6] [OK] Worker registered using mTLS")
	req := &runtimepb.SubmitJobRequest{JobId: 1, TaskType: "hash", Payload: "DistributedZKRuntime local demo", TimeoutMs: 5000}
	fmt.Fprintf(out, "\nTask Execution\n  Submitting task: %s\n", req.TaskType)
	var resp *runtimepb.SubmitJobResponse
	if err := stage(ctx, 5*time.Second, func(ctx context.Context) error {
		var err error
		resp, err = runtimepb.NewCoordinatorServiceClient(conn).SubmitJob(ctx, req)
		return err
	}); err != nil {
		return fmt.Errorf("SubmitJob: %w", err)
	}
	if err := validateResponse(req, resp); err != nil {
		return err
	}
	fmt.Fprintf(out, "  [4/6] [OK] Hash task completed\n  JobID: %d | AttemptID: %d | Final Status: %s\n\nVerification\n  [OK] SHA-256 independently verified by demo\n  SHA-256: %s\n", resp.JobId, resp.AttemptId, resp.Status, resp.Output)
	// Stop before opening a read-only SQLite connection; no concurrent writers.
	var shutdownErr error
	for i := len(children) - 1; i >= 0; i-- {
		shutdownErr = errors.Join(shutdownErr, children[i].stop())
	}
	children = nil
	if shutdownErr != nil {
		return fmt.Errorf("service shutdown: %w", shutdownErr)
	}
	if err := stage(ctx, 5*time.Second, func(ctx context.Context) error { return verifySQLite(ctx, db, req) }); err != nil {
		return fmt.Errorf("SQLite verification: %w", err)
	}
	fmt.Fprintln(out, "  [5/6] [OK] SQLite terminal state and full task specification verified")
	return ctx.Err()
}
