package main

import (
	"context"
	"net"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type workerFailureStore struct {
	JobStore
	mu    sync.Mutex
	saves []JobRecord
}

func (s *workerFailureStore) Save(record JobRecord) error {
	if err := s.JobStore.Save(record); err != nil {
		return err
	}
	s.mu.Lock()
	s.saves = append(s.saves, record)
	s.mu.Unlock()
	return nil
}

func (s *workerFailureStore) savedRecords() []JobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]JobRecord(nil), s.saves...)
}

func newWorkerFailureStore(t *testing.T) *workerFailureStore {
	t.Helper()
	db, err := NewSQLiteJobStore(filepath.Join(t.TempDir(), "jobs.db"))
	if err != nil {
		t.Fatalf("open SQLite store: %v", err)
	}
	t.Cleanup(func() {
		if err := db.Close(); err != nil {
			t.Errorf("close SQLite store: %v", err)
		}
	})
	return &workerFailureStore{JobStore: db}
}

func startWorkerFailureServer(
	t *testing.T,
	client runtimepb.CoordinatorServiceClient,
	id string,
	execute func(context.Context, *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error),
	options ...grpc.ServerOption,
) *grpc.Server {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen for %s: %v", id, err)
	}
	server := grpc.NewServer(options...)
	runtimepb.RegisterWorkerServiceServer(server, &testWorker{execute: execute})
	go server.Serve(listener)
	t.Cleanup(server.Stop)

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := client.RegisterWorker(ctx, &runtimepb.RegisterWorkerRequest{
		WorkerId: id, Address: listener.Addr().String(),
	})
	if err != nil || resp == nil || !resp.Accepted {
		t.Fatalf("register %s: response=%+v error=%v", id, resp, err)
	}
	return server
}

func assertWorkerFailureSaves(t *testing.T, store *workerFailureStore, want []JobRecord) {
	t.Helper()
	if got := store.savedRecords(); !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted writes:\n got: %+v\nwant: %+v", got, want)
	}
}

type workerFailureResult struct {
	resp *runtimepb.SubmitJobResponse
	err  error
}

func TestInFlightWorkerStopRetriesPreimageOnSecondWorker(t *testing.T) {
	const jobID int64 = 8101
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
	witnesses := runtime.NewMemoryWitnessStore()
	witnesses.Put("secret-001", 42)
	payload := preimagePublicPayload(zk.ComputePreimageDigest(42))
	store := newWorkerFailureStore(t)
	coordinator := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
	client := startTestCoordinator(t, coordinator)

	firstStarted := make(chan int64, 1)
	firstCanceled := make(chan struct{}, 1)
	var firstCalls, secondCalls atomic.Int32
	firstServer := startWorkerFailureServer(t, client, "worker-1",
		func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			firstCalls.Add(1)
			firstStarted <- req.AttemptId
			<-ctx.Done()
			firstCanceled <- struct{}{}
			return nil, ctx.Err()
		})
	secondStarted := make(chan int64, 1)
	startWorkerFailureServer(t, client, "worker-2",
		func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			secondCalls.Add(1)
			secondStarted <- req.AttemptId
			if req.JobId != jobID || req.AttemptId != 2 || req.TaskType != "zk_preimage_prove" ||
				req.Payload != payload || req.TimeoutMs != preimageTestTimeoutMs {
				t.Errorf("unexpected retry request: %+v", req)
			}
			task := runtime.ZKPreimageTask{Payload: req.Payload, Prover: prover, WitnessStore: witnesses}
			result := runtime.ExecuteJob(ctx, runtime.Job{ID: int(req.JobId), Task: task, Status: runtime.JobRunning})
			errText := ""
			if result.Err != nil {
				errText = result.Err.Error()
			}
			return &runtimepb.ExecuteJobResponse{
				JobId: int64(result.JobID), AttemptId: req.AttemptId,
				Status: result.Status.String(), Output: result.Output, Error: errText,
			}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submitted := make(chan workerFailureResult, 1)
	go func() {
		resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
			JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: preimageTestTimeoutMs,
		})
		submitted <- workerFailureResult{resp: resp, err: err}
	}()
	select {
	case attempt := <-firstStarted:
		if attempt != 1 {
			t.Fatalf("worker-1 received AttemptID %d, want 1", attempt)
		}
	case <-ctx.Done():
		t.Fatal("worker-1 did not receive the in-flight RPC")
	}
	firstServer.Stop()
	select {
	case <-firstCanceled:
	case <-ctx.Done():
		t.Fatal("worker-1 did not observe the stopped transport")
	}
	select {
	case attempt := <-secondStarted:
		if attempt != 2 {
			t.Fatalf("worker-2 received AttemptID %d, want 2", attempt)
		}
	case <-ctx.Done():
		t.Fatal("worker-2 did not receive the retry")
	}

	var accepted *runtimepb.SubmitJobResponse
	select {
	case result := <-submitted:
		accepted = result.resp
		if result.err != nil {
			t.Fatalf("retry submission failed: %v", result.err)
		}
	case <-ctx.Done():
		t.Fatal("retry submission did not finish")
	}
	if accepted == nil || accepted.JobId != jobID || accepted.AttemptId != 2 ||
		accepted.Status != "Succeeded" || accepted.Error != "" {
		t.Fatalf("unexpected accepted response: %+v", accepted)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || verifier.calls.Load() != 1 {
		t.Fatalf("calls: worker-1=%d worker-2=%d verifier=%d, want 1 each",
			firstCalls.Load(), secondCalls.Load(), verifier.calls.Load())
	}
	assertWorkerFailureSaves(t, store, []JobRecord{
		preimageRecord(jobID, 1, "worker-1", JobRunning, payload),
		preimageRecord(jobID, 2, "worker-2", JobRunning, payload),
		preimageRecord(jobID, 2, "worker-2", JobSucceeded, payload),
	})
	record, ok, err := store.Load(jobID)
	if err != nil || !ok || record != preimageRecord(jobID, 2, "worker-2", JobSucceeded, payload) {
		t.Fatalf("durable result=%+v found=%v error=%v", record, ok, err)
	}
	if coordinator.isLeaseCurrent(jobID, 1) {
		t.Fatal("failed Attempt 1 still passes fencing")
	}
}

func TestLatePreimageResultAfterRetryCannotOverwriteSQLite(t *testing.T) {
	const jobID int64 = 8102
	validOutput, digest, realVerifier := preimageProofFixture(t)
	payload := preimagePublicPayload(digest)
	store := newWorkerFailureStore(t)
	verifier := &observedPreimageVerifier{verifier: realVerifier}
	dispatchDone := make(chan int64, 2)
	coordinator := &CoordinatorServer{
		jobStore: store, preimageVerifier: verifier,
		leaseDuration:  250 * time.Millisecond,
		onDispatchDone: func(_ int64, attemptID int64) { dispatchDone <- attemptID },
	}
	client := startTestCoordinator(t, coordinator)

	firstStarted := make(chan int64, 1)
	releaseFirst := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseFirst) }) })
	lateReturned := make(chan *runtimepb.ExecuteJobResponse, 1)
	var firstCalls, secondCalls atomic.Int32
	startWorkerFailureServer(t, client, "worker-1",
		func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			firstCalls.Add(1)
			firstStarted <- req.AttemptId
			// Deliberately ignore attempt cancellation to simulate delayed remote work.
			<-releaseFirst
			return &runtimepb.ExecuteJobResponse{
				JobId: req.JobId, AttemptId: req.AttemptId,
				Status: "Succeeded", Output: validOutput,
			}, nil
		},
		grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			resp, err := handler(ctx, req)
			if err == nil {
				lateReturned <- resp.(*runtimepb.ExecuteJobResponse)
			}
			return resp, err
		}))
	secondStarted := make(chan int64, 1)
	startWorkerFailureServer(t, client, "worker-2",
		func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			secondCalls.Add(1)
			secondStarted <- req.AttemptId
			return &runtimepb.ExecuteJobResponse{
				JobId: req.JobId, AttemptId: req.AttemptId,
				Status: "Succeeded", Output: validOutput,
			}, nil
		})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submitted := make(chan workerFailureResult, 1)
	go func() {
		resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
			JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: preimageTestTimeoutMs,
		})
		submitted <- workerFailureResult{resp: resp, err: err}
	}()
	select {
	case attempt := <-firstStarted:
		if attempt != 1 {
			t.Fatalf("worker-1 received AttemptID %d, want 1", attempt)
		}
	case <-ctx.Done():
		t.Fatal("worker-1 did not start")
	}
	select {
	case attempt := <-secondStarted:
		if attempt != 2 {
			t.Fatalf("worker-2 received AttemptID %d, want 2", attempt)
		}
	case <-ctx.Done():
		t.Fatal("lease expiry did not start Attempt 2")
	}

	var accepted *runtimepb.SubmitJobResponse
	select {
	case result := <-submitted:
		accepted = result.resp
		if result.err != nil {
			t.Fatalf("retry submission failed: %v", result.err)
		}
	case <-ctx.Done():
		t.Fatal("retry submission did not finish")
	}
	if accepted == nil || accepted.AttemptId != 2 || accepted.Status != "Succeeded" ||
		accepted.Output != validOutput || verifier.calls.Load() != 1 {
		t.Fatalf("accepted result=%+v verifier calls=%d", accepted, verifier.calls.Load())
	}
	select {
	case <-lateReturned:
		t.Fatal("worker-1 returned before it was released")
	default:
	}
	// The abandoned dispatch must finish without waiting for the blocked handler.
	completed := make(map[int64]bool)
	for len(completed) < 2 {
		select {
		case attempt := <-dispatchDone:
			if attempt < 1 || attempt > 2 || completed[attempt] {
				t.Fatalf("unexpected dispatch completion: %d", attempt)
			}
			completed[attempt] = true
		case <-ctx.Done():
			t.Fatal("abandoned dispatch did not finish")
		}
	}
	releaseOnce.Do(func() { close(releaseFirst) })
	select {
	case late := <-lateReturned:
		if late.JobId != jobID || late.AttemptId != 1 || late.Status != "Succeeded" ||
			late.Output != validOutput {
			t.Fatalf("unexpected late result: %+v", late)
		}
		if coordinator.isLeaseCurrent(late.JobId, late.AttemptId) {
			t.Fatal("late Attempt 1 passes fencing")
		}
	case <-ctx.Done():
		t.Fatal("worker-1 did not return its delayed success")
	}
	if verifier.calls.Load() != 1 || firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("calls: verifier=%d worker-1=%d worker-2=%d",
			verifier.calls.Load(), firstCalls.Load(), secondCalls.Load())
	}
	assertWorkerFailureSaves(t, store, []JobRecord{
		preimageRecord(jobID, 1, "worker-1", JobRunning, payload),
		preimageRecord(jobID, 2, "worker-2", JobRunning, payload),
		preimageRecord(jobID, 2, "worker-2", JobSucceeded, payload),
	})
	record, ok, err := store.Load(jobID)
	if err != nil || !ok || record != preimageRecord(jobID, 2, "worker-2", JobSucceeded, payload) {
		t.Fatalf("late result changed durable record=%+v found=%v error=%v", record, ok, err)
	}
}

func TestInFlightWorkerStopWithoutBackupNeverPersistsSuccess(t *testing.T) {
	const jobID int64 = 8103
	payload := preimagePublicPayload(zk.ComputePreimageDigest(42))
	store := newWorkerFailureStore(t)
	coordinator := &CoordinatorServer{jobStore: store}
	client := startTestCoordinator(t, coordinator)
	firstStarted := make(chan int64, 1)
	firstCanceled := make(chan struct{}, 1)
	var calls atomic.Int32
	server := startWorkerFailureServer(t, client, "worker-1",
		func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			calls.Add(1)
			firstStarted <- req.AttemptId
			<-ctx.Done()
			firstCanceled <- struct{}{}
			return nil, ctx.Err()
		})

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	submitted := make(chan workerFailureResult, 1)
	go func() {
		resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
			JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: preimageTestTimeoutMs,
		})
		submitted <- workerFailureResult{resp: resp, err: err}
	}()
	select {
	case attempt := <-firstStarted:
		if attempt != 1 {
			t.Fatalf("worker received AttemptID %d, want 1", attempt)
		}
	case <-ctx.Done():
		t.Fatal("worker did not receive the in-flight RPC")
	}
	server.Stop()
	select {
	case <-firstCanceled:
	case <-ctx.Done():
		t.Fatal("worker did not observe transport cancellation")
	}
	select {
	case result := <-submitted:
		if result.resp != nil || status.Code(result.err) != codes.Unavailable {
			t.Fatalf("failed job reported success: response=%+v error=%v", result.resp, result.err)
		}
	case <-ctx.Done():
		t.Fatal("submission did not exhaust retries")
	}
	if calls.Load() != 1 {
		t.Fatalf("stopped worker handled %d calls, want 1", calls.Load())
	}
	assertWorkerFailureSaves(t, store, []JobRecord{
		preimageRecord(jobID, 1, "worker-1", JobRunning, payload),
		preimageRecord(jobID, 2, "worker-1", JobRunning, payload),
	})
	record, ok, err := store.Load(jobID)
	if err != nil || !ok || record != preimageRecord(jobID, 2, "worker-1", JobRunning, payload) {
		t.Fatalf("failed job durable record=%+v found=%v error=%v", record, ok, err)
	}
}
