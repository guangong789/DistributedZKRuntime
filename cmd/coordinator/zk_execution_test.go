package main

import (
	"context"
	"encoding/base64"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const (
	squareTestPayload   = `{"x":5,"y":25}`
	squareTestTimeoutMs = int64(1000)
)

type recordingSquareStore struct {
	*MemoryJobStores
	mu    sync.Mutex
	saves []JobRecord
}

func newRecordingSquareStore() *recordingSquareStore {
	return &recordingSquareStore{MemoryJobStores: NewMemoryJobStore()}
}

func (s *recordingSquareStore) Save(record JobRecord) error {
	if err := s.MemoryJobStores.Save(record); err != nil {
		return err
	}
	s.mu.Lock()
	s.saves = append(s.saves, record)
	s.mu.Unlock()
	return nil
}

func (s *recordingSquareStore) saved() []JobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]JobRecord(nil), s.saves...)
}

func squareRecord(jobID, attemptID int64, workerID string, state JobState) JobRecord {
	return JobRecord{
		JobID: jobID, State: state, AttemptID: attemptID, WorkerID: workerID,
		TaskType: "zk_square_prove", Payload: squareTestPayload, TimeoutMs: squareTestTimeoutMs,
	}
}

func assertSquareSaves(t *testing.T, store *recordingSquareStore, want []JobRecord) {
	t.Helper()
	if got := store.saved(); !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted records=%+v, want %+v", got, want)
	}
}

func submitSquareTestJob(t *testing.T, s *CoordinatorServer, jobID int64) (*runtimepb.SubmitJobResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
		JobId: jobID, TaskType: "zk_square_prove",
		Payload: squareTestPayload, TimeoutMs: squareTestTimeoutMs,
	})
}

func squareProofOutputs(t *testing.T) (valid, wrongPublicInput string, verifier *zk.SquareVerifier) {
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
	verifier, err = zk.LoadSquareVerifier(verifyingKeyPath)
	if err != nil {
		t.Fatalf("load square verifier: %v", err)
	}
	validProof, err := prover.Prove(5, 25)
	if err != nil {
		t.Fatalf("prove valid square: %v", err)
	}
	wrongProof, err := prover.Prove(4, 16)
	if err != nil {
		t.Fatalf("prove square for different public input: %v", err)
	}
	return base64.StdEncoding.EncodeToString(validProof),
		base64.StdEncoding.EncodeToString(wrongProof), verifier
}

type countingSquareVerifier struct {
	calls atomic.Int32
}

func (v *countingSquareVerifier) Verify([]byte, int64) error {
	v.calls.Add(1)
	return nil
}

func TestSubmitZKValidProofPersistsSuccess(t *testing.T) {
	valid, _, verifier := squareProofOutputs(t)
	for _, workerStatus := range []string{"Succeeded", "succeeded"} {
		t.Run(workerStatus, func(t *testing.T) {
			const jobID int64 = 701
			store := newRecordingSquareStore()
			s := &CoordinatorServer{jobStore: store, squareVerifier: verifier}
			var calls atomic.Int32
			registerTestWorker(t, s, "proof-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				if req.JobId != jobID || req.AttemptId != 1 ||
					req.TaskType != "zk_square_prove" || req.Payload != squareTestPayload ||
					req.TimeoutMs != squareTestTimeoutMs {
					t.Errorf("unexpected dispatch: %+v", req)
				}
				return &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId,
					Status: workerStatus, Output: valid,
				}, nil
			})

			resp, err := submitSquareTestJob(t, s, jobID)
			if err != nil || resp == nil || resp.AttemptId != 1 ||
				resp.Status != workerStatus || resp.Output != valid || calls.Load() != 1 {
				t.Fatalf("response=%+v error=%v calls=%d", resp, err, calls.Load())
			}
			assertSquareSaves(t, store, []JobRecord{
				squareRecord(jobID, 1, "proof-worker", JobRunning),
				squareRecord(jobID, 1, "proof-worker", JobSucceeded),
			})
		})
	}
}

func TestSubmitZKInvalidProofRetriesBeforeSuccess(t *testing.T) {
	valid, wrongPublicInput, verifier := squareProofOutputs(t)
	const jobID int64 = 702
	store := newRecordingSquareStore()
	s := &CoordinatorServer{jobStore: store, squareVerifier: verifier}
	var firstCalls, secondCalls atomic.Int32
	registerTestWorker(t, s, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		if req.AttemptId != 1 {
			t.Errorf("first worker got attempt %d", req.AttemptId)
		}
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: wrongPublicInput,
		}, nil
	})
	registerTestWorker(t, s, "second", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		if req.AttemptId != 2 || req.TaskType != "zk_square_prove" ||
			req.Payload != squareTestPayload || req.TimeoutMs != squareTestTimeoutMs {
			t.Errorf("retry got incorrect task: %+v", req)
		}
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: valid,
		}, nil
	})

	resp, err := submitSquareTestJob(t, s, jobID)
	if err != nil || resp == nil || resp.AttemptId != 2 ||
		resp.Status != "Succeeded" || resp.Output != valid {
		t.Fatalf("response=%+v error=%v", resp, err)
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("dispatch counts: first=%d second=%d", firstCalls.Load(), secondCalls.Load())
	}
	assertSquareSaves(t, store, []JobRecord{
		squareRecord(jobID, 1, "first", JobRunning),
		squareRecord(jobID, 2, "second", JobRunning),
		squareRecord(jobID, 2, "second", JobSucceeded),
	})
}

func TestSubmitZKInvalidProofExhaustsRetries(t *testing.T) {
	_, wrongPublicInput, verifier := squareProofOutputs(t)
	const jobID int64 = 703
	store := newRecordingSquareStore()
	s := &CoordinatorServer{jobStore: store, squareVerifier: verifier}
	var calls atomic.Int32
	for _, workerID := range []string{"first", "second"} {
		registerTestWorker(t, s, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			calls.Add(1)
			return &runtimepb.ExecuteJobResponse{
				JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: wrongPublicInput,
			}, nil
		})
	}

	resp, err := submitSquareTestJob(t, s, jobID)
	if resp != nil || status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "verify zk square proof") {
		t.Fatalf("expected verification retry exhaustion, got response=%+v error=%v", resp, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("dispatch count=%d, want 2", calls.Load())
	}
	assertSquareSaves(t, store, []JobRecord{
		squareRecord(jobID, 1, "first", JobRunning),
		squareRecord(jobID, 2, "second", JobRunning),
	})
}

func TestSubmitZKStaleResultIsFencedBeforeVerification(t *testing.T) {
	for _, scenario := range []string{"wrong attempt", "expired lease"} {
		t.Run(scenario, func(t *testing.T) {
			const jobID int64 = 704
			verifier := &countingSquareVerifier{}
			s := &CoordinatorServer{jobStore: NewMemoryJobStore(), squareVerifier: verifier}
			registerTestWorker(t, s, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				resp := &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId,
					Status: "Succeeded", Output: "cHJvb2Y=",
				}
				if scenario == "wrong attempt" {
					resp.AttemptId = 0
				} else {
					s.setLease(req.JobId, req.AttemptId, "first", -time.Second)
				}
				return resp, nil
			})
			registerTestWorker(t, s, "second", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				if verifier.calls.Load() != 0 {
					t.Errorf("stale attempt was verified")
				}
				return &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId,
					Status: "Succeeded", Output: "cHJvb2Y=",
				}, nil
			})

			resp, err := submitSquareTestJob(t, s, jobID)
			if err != nil || resp == nil || resp.AttemptId != 2 || resp.Status != "Succeeded" {
				t.Fatalf("response=%+v error=%v", resp, err)
			}
			if verifier.calls.Load() != 1 {
				t.Fatalf("verification calls=%d, want only current attempt", verifier.calls.Load())
			}
		})
	}
}

func TestSubmitNonZKSuccessSkipsSquareVerification(t *testing.T) {
	for _, taskType := range []string{"hash", "sleep"} {
		t.Run(taskType, func(t *testing.T) {
			const jobID int64 = 705
			verifier := &countingSquareVerifier{}
			store := NewMemoryJobStore()
			s := &CoordinatorServer{jobStore: store, squareVerifier: verifier}
			registerTestWorker(t, s, "ordinary-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				return successfulResult(req), nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
				JobId: jobID, TaskType: taskType, Payload: "input", TimeoutMs: 1000,
			})
			if err != nil || resp == nil || resp.Status != "Succeeded" || resp.AttemptId != 1 {
				t.Fatalf("response=%+v error=%v", resp, err)
			}
			if verifier.calls.Load() != 0 {
				t.Fatalf("non-ZK result invoked verifier %d times", verifier.calls.Load())
			}
			record, ok, err := store.Load(jobID)
			if err != nil || !ok || record.State != JobSucceeded {
				t.Fatalf("record=%+v found=%v error=%v", record, ok, err)
			}
		})
	}
}

func TestSubmitZKWithoutVerifierRejectsSuccess(t *testing.T) {
	const jobID int64 = 706
	store := newRecordingSquareStore()
	s := &CoordinatorServer{jobStore: store}
	var calls atomic.Int32
	for _, workerID := range []string{"first", "second"} {
		registerTestWorker(t, s, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
			calls.Add(1)
			return &runtimepb.ExecuteJobResponse{
				JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: "cHJvb2Y=",
			}, nil
		})
	}

	resp, err := submitSquareTestJob(t, s, jobID)
	if resp != nil || status.Code(err) != codes.Unavailable || !strings.Contains(err.Error(), "square verifier is not configured") {
		t.Fatalf("expected missing-verifier retry exhaustion, got response=%+v error=%v", resp, err)
	}
	if calls.Load() != 2 {
		t.Fatalf("dispatch count=%d, want 2", calls.Load())
	}
	assertSquareSaves(t, store, []JobRecord{
		squareRecord(jobID, 1, "first", JobRunning),
		squareRecord(jobID, 2, "second", JobRunning),
	})
}

func TestSubmitZKTaskFailuresRemainTerminalWithoutVerification(t *testing.T) {
	for _, tc := range []struct {
		status string
		state  JobState
	}{
		{status: "Failed", state: JobFailed},
		{status: "Cancelled", state: JobCancelled},
	} {
		t.Run(tc.status, func(t *testing.T) {
			const jobID int64 = 707
			verifier := &countingSquareVerifier{}
			store := newRecordingSquareStore()
			s := &CoordinatorServer{jobStore: store, squareVerifier: verifier}
			var firstCalls, secondCalls atomic.Int32
			registerTestWorker(t, s, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				firstCalls.Add(1)
				return &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId,
					Status: tc.status, Error: "task outcome",
				}, nil
			})
			registerTestWorker(t, s, "second", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				secondCalls.Add(1)
				return successfulResult(req), nil
			})

			resp, err := submitSquareTestJob(t, s, jobID)
			if err != nil || resp == nil || resp.AttemptId != 1 ||
				resp.Status != tc.status || resp.Error != "task outcome" {
				t.Fatalf("response=%+v error=%v", resp, err)
			}
			if verifier.calls.Load() != 0 || firstCalls.Load() != 1 || secondCalls.Load() != 0 {
				t.Fatalf("unexpected verify/retry counts: verify=%d first=%d second=%d",
					verifier.calls.Load(), firstCalls.Load(), secondCalls.Load())
			}
			assertSquareSaves(t, store, []JobRecord{
				squareRecord(jobID, 1, "first", JobRunning),
				squareRecord(jobID, 1, "first", tc.state),
			})
		})
	}
}
