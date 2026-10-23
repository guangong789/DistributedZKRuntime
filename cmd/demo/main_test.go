package main

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func hashRequest() *runtimepb.SubmitJobRequest {
	return &runtimepb.SubmitJobRequest{JobId: 1, TaskType: "hash", Payload: "hello", TimeoutMs: 5000}
}

func TestHashResponseEvidence(t *testing.T) {
	req := hashRequest()
	const digest = "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
	if expectedHash(req.Payload) != digest {
		t.Fatal("SHA-256 mismatch")
	}
	good := func() *runtimepb.SubmitJobResponse {
		return &runtimepb.SubmitJobResponse{JobId: 1, AttemptId: 1, Status: "Succeeded", Output: digest}
	}
	if err := validateResponse(req, good()); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"nil", "job", "attempt", "status", "output", "error"} {
		t.Run(field, func(t *testing.T) {
			r := good()
			switch field {
			case "nil":
				r = nil
			case "job":
				r.JobId++
			case "attempt":
				r.AttemptId++
			case "status":
				r.Status = "Failed"
			case "output":
				r.Output = "incorrect"
			case "error":
				r.Error = "failure"
			}
			if validateResponse(req, r) == nil {
				t.Fatal("incomplete success evidence accepted")
			}
		})
	}
}

func TestSQLiteEvidence(t *testing.T) {
	req := hashRequest()
	good := persistedJob{1, 2, 1, "worker-1", "hash", "hello", 5000}
	if err := validateRecord(req, good); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"job", "state", "attempt", "worker", "type", "payload", "timeout"} {
		t.Run(field, func(t *testing.T) {
			r := good
			switch field {
			case "job":
				r.JobID++
			case "state":
				r.State = 1
			case "attempt":
				r.AttemptID++
			case "worker":
				r.WorkerID = "other"
			case "type":
				r.TaskType = "sleep"
			case "payload":
				r.Payload = "other"
			case "timeout":
				r.TimeoutMs++
			}
			if validateRecord(req, r) == nil {
				t.Fatal("inconsistent durable evidence accepted")
			}
		})
	}
	path := filepath.Join(t.TempDir(), "jobs.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	_, err = db.Exec(`CREATE TABLE jobs (job_id INTEGER PRIMARY KEY, state INTEGER, attempt_id INTEGER, worker_id TEXT, task_type TEXT, payload TEXT, timeout_ms INTEGER);
INSERT INTO jobs VALUES (1,2,1,'worker-1','hash','hello',5000)`)
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := verifySQLite(ctx, path, req); err != nil {
		t.Fatal(err)
	}
	req.JobId = 2
	if err := verifySQLite(ctx, path, req); err == nil {
		t.Fatal("missing durable record accepted")
	}
	missing := filepath.Join(t.TempDir(), "missing.db")
	if err := verifySQLite(ctx, missing, req); err == nil {
		t.Fatal("missing DB accepted")
	}
	if _, err := os.Stat(missing); !os.IsNotExist(err) {
		t.Fatal("read-only inspection created DB", err)
	}
}

func TestReadinessEvidence(t *testing.T) {
	r := &readiness{prefix: "service ready ", ready: make(chan struct{})}
	for _, part := range []string{"unrelated\nservice re", "ady 127.0.0.1:1234"} {
		if _, err := r.Write([]byte(part)); err != nil {
			t.Fatal(err)
		}
		select {
		case <-r.ready:
			t.Fatal("accepted incomplete line")
		default:
		}
	}
	if _, err := r.Write([]byte("\n")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-r.ready:
	default:
		t.Fatal("missed complete readiness line")
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); _, _ = r.Write([]byte("service ready again\n")) }()
	}
	wg.Wait()
	if r.value != "service ready 127.0.0.1:1234" {
		t.Fatal("readiness overwritten")
	}
	oversize := &readiness{prefix: "ready", ready: make(chan struct{})}
	if _, err := oversize.Write([]byte(strings.Repeat("x", (64<<10)+1))); err == nil {
		t.Fatal("unbounded readiness buffer")
	}
}

// Only spawned by the lifecycle tests below. It never starts runtime services.
func TestDemoProcessHelper(t *testing.T) {
	mode := os.Getenv("DZKR_DEMO_TEST_HELPER")
	if mode == "" {
		return
	}
	if mode == "early" {
		os.Exit(0)
	}
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	fmt.Print("helper ")
	fmt.Println("ready")
	<-interrupt
	os.Exit(0)
}

func testChild(t *testing.T, mode string) *child {
	t.Helper()
	t.Setenv("DZKR_DEMO_TEST_HELPER", mode)
	exe, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	p, err := startChild(".", filepath.Join(t.TempDir(), "child.log"), "helper ready", exe, "-test.run=^TestDemoProcessHelper$")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		select {
		case <-p.done:
		default:
			if err := p.stop(); err != nil {
				t.Error(err)
			}
		}
	})
	return p
}

func TestOwnedChildReadinessAndCleanup(t *testing.T) {
	p := testChild(t, "wait")
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := p.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	other := testChild(t, "wait")
	if _, err := other.waitReady(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.stop(); err != nil {
		t.Fatal(err)
	}
	if err := other.alive(); err != nil {
		t.Fatal("cleanup affected a different child group", err)
	}
	if err := other.stop(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-p.done:
	default:
		t.Fatal("child not reaped")
	}
	if !p.groupGone() {
		t.Fatal("owned group remains")
	}
	if p.cmd.ProcessState == nil || !p.cmd.ProcessState.Success() {
		t.Fatal("unclean child exit")
	}
}

func TestReadinessEarlyExitAndTimeout(t *testing.T) {
	t.Run("early exit", func(t *testing.T) {
		p := testChild(t, "early")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := p.waitReady(ctx); err == nil {
			t.Fatal("early zero exit accepted")
		}
		if err := p.stop(); err == nil {
			t.Fatal("premature process exit accepted as normal cleanup")
		}
	})
	t.Run("cancellation", func(t *testing.T) {
		p := testChild(t, "wait")
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if _, err := p.waitReady(ctx); err != nil {
			t.Fatal(err)
		}
		// A never-signaled readiness marker avoids sleeps or startup races.
		p.output = &readiness{ready: make(chan struct{})}
		cancelled, stop := context.WithCancel(context.Background())
		stop()
		if _, err := p.waitReady(cancelled); err != context.Canceled {
			t.Fatal("cancellation not propagated", err)
		}
		expired, expire := context.WithDeadline(context.Background(), time.Now().Add(-time.Second))
		defer expire()
		if _, err := p.waitReady(expired); err != context.DeadlineExceeded {
			t.Fatal("readiness timeout not propagated", err)
		}
		if err := p.stop(); err != nil {
			t.Fatal(err)
		}
	})
}

func TestMissingPrerequisite(t *testing.T) {
	t.Setenv("PATH", t.TempDir())
	if err := prerequisites(context.Background(), "."); err == nil || !strings.Contains(err.Error(), "missing prerequisite") {
		t.Fatal("missing tool accepted", err)
	}
}
