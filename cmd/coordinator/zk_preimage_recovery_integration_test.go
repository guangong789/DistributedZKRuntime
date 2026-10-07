package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"path/filepath"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

func assertPublicRecoveryPayload(t *testing.T, payload string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &fields); err != nil {
		t.Fatalf("decode persisted ZK payload: %v", err)
	}
	if len(fields) != 2 || fields["witness_ref"] == nil || fields["digest"] == nil {
		t.Fatalf("persisted payload must contain only witness_ref and digest: %v", fields)
	}
	var ref, digest string
	if err := json.Unmarshal(fields["witness_ref"], &ref); err != nil || ref != "secret-001" {
		t.Fatalf("persisted witness_ref=%q error=%v", ref, err)
	}
	if err := json.Unmarshal(fields["digest"], &digest); err != nil || digest == "" {
		t.Fatalf("persisted public digest=%q error=%v", digest, err)
	}
}

func TestPreimageJobRecoversAcrossSQLiteCoordinatorRestart(t *testing.T) {
	const jobID int64 = 8201
	const timeoutMs int64 = 5000
	dir := t.TempDir()
	dbPath := filepath.Join(dir, "jobs.db")
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
	digest := zk.ComputePreimageDigest(42)
	payload := preimagePublicPayload(digest)
	assertPublicRecoveryPayload(t, payload)

	var writesMu sync.Mutex
	var writes []JobRecord
	terminalSaved := make(chan JobRecord, 1)
	recordWrites := func(inner JobStore) func(JobRecord) error {
		return func(record JobRecord) error {
			if err := inner.Save(record); err != nil {
				return err
			}
			writesMu.Lock()
			writes = append(writes, record)
			writesMu.Unlock()
			if record.State == JobSucceeded {
				select {
				case terminalSaved <- record:
				default:
				}
			}
			return nil
		}
	}

	dbA, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open Coordinator A store: %v", err)
	}
	t.Cleanup(func() { _ = dbA.Close() })
	coordinatorA := &CoordinatorServer{
		jobStore: &recoveryPassStore{JobStore: dbA, save: recordWrites(dbA)},
	}
	firstStarted := make(chan int64, 1)
	oldDispatchDone := make(chan struct{}, 1)
	coordinatorA.onDispatchDone = func(id, attempt int64) {
		if id == jobID && attempt == 1 {
			oldDispatchDone <- struct{}{}
		}
	}
	var oldCalls atomic.Int32
	registerTestWorker(t, coordinatorA, "old-worker",
		func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			oldCalls.Add(1)
			firstStarted <- req.AttemptId
			<-ctx.Done()
			return nil, ctx.Err()
		})

	waitCtx, waitCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer waitCancel()
	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	oldSubmission := make(chan error, 1)
	go func() {
		_, err := coordinatorA.SubmitJob(ctxA, &runtimepb.SubmitJobRequest{
			JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: timeoutMs,
		})
		oldSubmission <- err
	}()
	select {
	case attempt := <-firstStarted:
		if attempt != 1 {
			t.Fatalf("Coordinator A dispatched AttemptID %d, want 1", attempt)
		}
	case <-waitCtx.Done():
		t.Fatal("Coordinator A did not dispatch Attempt 1")
	}
	running1 := JobRecord{
		JobID: jobID, State: JobRunning, AttemptID: 1, WorkerID: "old-worker",
		TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: timeoutMs,
	}
	assertRecoveryRecord(t, dbA, running1)
	assertPublicRecoveryPayload(t, running1.Payload)
	cancelA()
	select {
	case err := <-oldSubmission:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Coordinator A submission error=%v, want cancellation", err)
		}
	case <-waitCtx.Done():
		t.Fatal("Coordinator A did not stop")
	}
	select {
	case <-oldDispatchDone:
	case <-waitCtx.Done():
		t.Fatal("Coordinator A dispatch did not finish")
	}
	assertRecoveryRecord(t, dbA, running1)
	if oldCalls.Load() != 1 {
		t.Fatalf("old Worker calls=%d, want 1", oldCalls.Load())
	}
	if err := dbA.Close(); err != nil {
		t.Fatalf("close Coordinator A store: %v", err)
	}
	// Only the SQLite file is carried into a fresh Coordinator B.
	dbB, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open Coordinator B store: %v", err)
	}
	t.Cleanup(func() {
		if err := dbB.Close(); err != nil {
			t.Errorf("close Coordinator B store: %v", err)
		}
	})
	coordinatorB := &CoordinatorServer{
		workers: make(map[string]WorkerInfo), jobAttempts: make(map[int64]int64),
		leases: make(map[int64]JobLease), activeJobs: make(map[int64]struct{}),
		jobStore:         &recoveryPassStore{JobStore: dbB, save: recordWrites(dbB)},
		recoveryReady:    make(chan struct{}, 1),
		preimageVerifier: verifier,
	}
	assertRecoveryRecord(t, dbB, running1)
	if err := coordinatorB.recoverRunningJobs(); err != nil {
		t.Fatalf("mark Running job as Recovering: %v", err)
	}
	recovering1 := running1
	recovering1.State = JobRecovering
	assertRecoveryRecord(t, dbB, recovering1)
	assertPublicRecoveryPayload(t, recovering1.Payload)
	if len(coordinatorB.workers) != 0 || len(coordinatorB.jobAttempts) != 0 ||
		len(coordinatorB.leases) != 0 || len(coordinatorB.activeJobs) != 0 {
		t.Fatal("startup marking created worker, attempt, lease, or ownership state")
	}

	loopCtx, cancelLoop := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() {
		coordinatorB.runRecoveryLoop(loopCtx)
		close(loopDone)
	}()
	t.Cleanup(func() {
		cancelLoop()
		<-loopDone
	})
	client := startTestCoordinator(t, coordinatorB)
	var recoveredCalls atomic.Int32
	proofOutput := make(chan string, 1)
	startWorkerFailureServer(t, client, "recovery-worker",
		func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			recoveredCalls.Add(1)
			if req.JobId != jobID || req.AttemptId != 2 || req.TaskType != "zk_preimage_prove" ||
				req.Payload != payload || req.TimeoutMs != timeoutMs {
				t.Errorf("recovered dispatch lost task specification: %+v", req)
			}
			running2 := running1
			running2.AttemptID, running2.WorkerID = 2, "recovery-worker"
			persisted, ok, err := dbB.Load(jobID)
			if err != nil || !ok || persisted != running2 {
				t.Errorf("Running Attempt 2 was not persisted before dispatch: record=%+v found=%v error=%v", persisted, ok, err)
			}
			task := runtime.ZKPreimageTask{Payload: req.Payload, Prover: prover, WitnessStore: witnesses}
			result := runtime.ExecuteJob(ctx, runtime.Job{ID: int(req.JobId), Task: task, Status: runtime.JobRunning})
			proofOutput <- result.Output
			errText := ""
			if result.Err != nil {
				errText = result.Err.Error()
			}
			return &runtimepb.ExecuteJobResponse{
				JobId: int64(result.JobID), AttemptId: req.AttemptId,
				Status: result.Status.String(), Output: result.Output, Error: errText,
			}, nil
		})
	select {
	case saved := <-terminalSaved:
		if saved.JobID != jobID || saved.State != JobSucceeded || saved.AttemptID != 2 {
			t.Fatalf("unexpected terminal write: %+v", saved)
		}
	case <-waitCtx.Done():
		t.Fatal("Worker registration did not complete ZK recovery")
	}
	cancelLoop()
	select {
	case <-loopDone:
	case <-waitCtx.Done():
		t.Fatal("recovery loop did not stop")
	}

	succeeded2 := running1
	succeeded2.State, succeeded2.AttemptID, succeeded2.WorkerID = JobSucceeded, 2, "recovery-worker"
	assertRecoveryRecord(t, dbB, succeeded2)
	assertPublicRecoveryPayload(t, succeeded2.Payload)
	writesMu.Lock()
	gotWrites := append([]JobRecord(nil), writes...)
	writesMu.Unlock()
	running2 := succeeded2
	running2.State = JobRunning
	wantWrites := []JobRecord{running1, recovering1, running2, succeeded2}
	if !reflect.DeepEqual(gotWrites, wantWrites) {
		t.Fatalf("durable progression:\n got: %+v\nwant: %+v", gotWrites, wantWrites)
	}
	if recoveredCalls.Load() != 1 || verifier.calls.Load() != 1 {
		t.Fatalf("recovered Worker calls=%d verifier calls=%d, want 1 each",
			recoveredCalls.Load(), verifier.calls.Load())
	}
	select {
	case output := <-proofOutput:
		proof, err := base64.StdEncoding.DecodeString(output)
		if err != nil || len(proof) == 0 {
			t.Fatalf("recovered Worker returned invalid proof: bytes=%d error=%v", len(proof), err)
		}
	default:
		t.Fatal("recovered Worker did not produce a proof")
	}
	lease, ok := coordinatorB.getLease(jobID)
	if !ok || lease.AttemptID != 2 || lease.WorkerID != "recovery-worker" ||
		coordinatorB.jobAttempts[jobID] != 2 {
		t.Fatalf("recovered attempt/lease mismatch: lease=%+v found=%v attempts=%v",
			lease, ok, coordinatorB.jobAttempts)
	}
	assertRecoveryOwnershipReleased(t, coordinatorB, jobID)
}

func TestPreimageRecoveryWithoutWorkerLeavesAttemptUnchanged(t *testing.T) {
	const jobID int64 = 8202
	const timeoutMs int64 = 5000
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	payload := preimagePublicPayload(zk.ComputePreimageDigest(42))
	assertPublicRecoveryPayload(t, payload)
	dbA, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open Coordinator A store: %v", err)
	}
	coordinatorA := &CoordinatorServer{jobStore: dbA}
	attempt, err := coordinatorA.startAttempt(
		jobID, "old-worker", time.Minute, "zk_preimage_prove", payload, timeoutMs,
	)
	if err != nil || attempt != 1 {
		t.Fatalf("start old attempt=%d error=%v", attempt, err)
	}
	if err := dbA.Close(); err != nil {
		t.Fatalf("close Coordinator A store: %v", err)
	}
	dbB, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open Coordinator B store: %v", err)
	}
	t.Cleanup(func() {
		if err := dbB.Close(); err != nil {
			t.Errorf("close Coordinator B store: %v", err)
		}
	})
	coordinatorB := &CoordinatorServer{
		workers: make(map[string]WorkerInfo), jobAttempts: make(map[int64]int64),
		leases: make(map[int64]JobLease), activeJobs: make(map[int64]struct{}),
		jobStore: dbB, recoveryReady: make(chan struct{}, 1),
	}
	if err := coordinatorB.recoverRunningJobs(); err != nil {
		t.Fatalf("mark Running job as Recovering: %v", err)
	}
	want := JobRecord{
		JobID: jobID, State: JobRecovering, AttemptID: 1, WorkerID: "old-worker",
		TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: timeoutMs,
	}
	assertRecoveryRecord(t, dbB, want)
	if err := coordinatorB.runRecoveryPass(context.Background()); err != nil {
		t.Fatalf("scan without workers: %v", err)
	}
	assertRecoveryRecord(t, dbB, want)
	assertPublicRecoveryPayload(t, want.Payload)
	if len(coordinatorB.jobAttempts) != 0 || len(coordinatorB.leases) != 0 {
		t.Fatalf("no-worker recovery allocated attempts=%v leases=%v",
			coordinatorB.jobAttempts, coordinatorB.leases)
	}
	if records, err := dbB.ListByState(JobSucceeded); err != nil || len(records) != 0 {
		t.Fatalf("no-worker recovery persisted success: records=%+v error=%v", records, err)
	}
	assertRecoveryOwnershipReleased(t, coordinatorB, jobID)
}
