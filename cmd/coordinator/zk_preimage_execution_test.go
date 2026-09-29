package main

import (
	"context"
	"encoding/base64"
	"fmt"
	"math/big"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const preimageTestTimeoutMs int64 = 1000

type recordingPreimageStore struct {
	*MemoryJobStores
	mu    sync.Mutex
	saves []JobRecord
}

func newRecordingPreimageStore() *recordingPreimageStore {
	return &recordingPreimageStore{MemoryJobStores: NewMemoryJobStore()}
}

func (s *recordingPreimageStore) Save(record JobRecord) error {
	if err := s.MemoryJobStores.Save(record); err != nil {
		return err
	}
	s.mu.Lock()
	s.saves = append(s.saves, record)
	s.mu.Unlock()
	return nil
}

func (s *recordingPreimageStore) savedRecords() []JobRecord {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]JobRecord(nil), s.saves...)
}

type countingPreimageVerifier struct {
	calls atomic.Int32
}

func (v *countingPreimageVerifier) Verify([]byte, fr.Element) error {
	v.calls.Add(1)
	return nil
}

func preimageProofFixture(t *testing.T) (string, fr.Element, *zk.PreimageVerifier) {
	t.Helper()
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
	verifier, err := zk.LoadPreimageVerifier(verifyingKey)
	if err != nil {
		t.Fatalf("load preimage verifier: %v", err)
	}
	digest := zk.ComputePreimageDigest(42)
	proof, err := prover.Prove(42, digest)
	if err != nil {
		t.Fatalf("prove preimage: %v", err)
	}
	return base64.StdEncoding.EncodeToString(proof), digest, verifier
}

func preimagePublicPayload(digest fr.Element) string {
	var value big.Int
	digest.BigInt(&value)
	return fmt.Sprintf(`{"witness_ref":"secret-001","digest":"%s"}`, value.String())
}

func preimageRecord(jobID, attemptID int64, workerID string, state JobState, payload string) JobRecord {
	return JobRecord{
		JobID: jobID, State: state, AttemptID: attemptID, WorkerID: workerID,
		TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: preimageTestTimeoutMs,
	}
}

func submitPreimageJob(t *testing.T, s *CoordinatorServer, jobID int64, payload string) (*runtimepb.SubmitJobResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
		JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: preimageTestTimeoutMs,
	})
}

func assertPreimageSaves(t *testing.T, store *recordingPreimageStore, want []JobRecord) {
	t.Helper()
	if got := store.savedRecords(); !reflect.DeepEqual(got, want) {
		t.Fatalf("persisted records:\n got: %+v\nwant: %+v", got, want)
	}
}

func TestSubmitPreimageAcceptsValidProof(t *testing.T) {
	output, digest, verifier := preimageProofFixture(t)
	payload := preimagePublicPayload(digest)
	store := newRecordingPreimageStore()
	s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
	var calls atomic.Int32
	registerTestWorker(t, s, "prover", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.JobId != 701 || req.AttemptId != 1 || req.TaskType != "zk_preimage_prove" ||
			req.Payload != payload || req.TimeoutMs != preimageTestTimeoutMs {
			t.Errorf("unexpected worker request: %+v", req)
		}
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: output,
		}, nil
	})

	resp, err := submitPreimageJob(t, s, 701, payload)
	if err != nil || resp == nil || resp.JobId != 701 || resp.AttemptId != 1 ||
		resp.Status != "Succeeded" || resp.Output != output || resp.Error != "" {
		t.Fatalf("valid proof rejected: response=%+v error=%v", resp, err)
	}
	if calls.Load() != 1 {
		t.Fatalf("worker called %d times, want 1", calls.Load())
	}
	assertPreimageSaves(t, store, []JobRecord{
		preimageRecord(701, 1, "prover", JobRunning, payload),
		preimageRecord(701, 1, "prover", JobSucceeded, payload),
	})
}

func TestSubmitPreimageRetriesInvalidProof(t *testing.T) {
	validOutput, digest, verifier := preimageProofFixture(t)
	payload := preimagePublicPayload(digest)
	store := newRecordingPreimageStore()
	s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
	var firstCalls, secondCalls atomic.Int32
	registerTestWorker(t, s, "first", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		firstCalls.Add(1)
		if req.AttemptId != 1 {
			t.Errorf("first worker attempt=%d, want 1", req.AttemptId)
		}
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded",
			Output: base64.StdEncoding.EncodeToString([]byte("corrupted proof")),
		}, nil
	})
	registerTestWorker(t, s, "second", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		secondCalls.Add(1)
		if req.AttemptId != 2 || req.Payload != payload {
			t.Errorf("second worker request=%+v", req)
		}
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: validOutput,
		}, nil
	})

	resp, err := submitPreimageJob(t, s, 702, payload)
	if err != nil || resp == nil || resp.AttemptId != 2 || resp.Status != "Succeeded" ||
		firstCalls.Load() != 1 || secondCalls.Load() != 1 {
		t.Fatalf("invalid proof retry: response=%+v error=%v calls=%d/%d",
			resp, err, firstCalls.Load(), secondCalls.Load())
	}
	assertPreimageSaves(t, store, []JobRecord{
		preimageRecord(702, 1, "first", JobRunning, payload),
		preimageRecord(702, 2, "second", JobRunning, payload),
		preimageRecord(702, 2, "second", JobSucceeded, payload),
	})
}

func TestSubmitPreimageExhaustsInvalidProofsWithoutSuccess(t *testing.T) {
	_, digest, verifier := preimageProofFixture(t)
	payload := preimagePublicPayload(digest)
	store := newRecordingPreimageStore()
	s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
	var calls atomic.Int32
	registerTestWorker(t, s, "invalid", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded",
			Output: base64.StdEncoding.EncodeToString([]byte("corrupted proof")),
		}, nil
	})

	resp, err := submitPreimageJob(t, s, 703, payload)
	if resp != nil || status.Code(err) != codes.Unavailable || calls.Load() != 2 {
		t.Fatalf("invalid proofs were accepted or not retried: response=%+v error=%v calls=%d", resp, err, calls.Load())
	}
	assertPreimageSaves(t, store, []JobRecord{
		preimageRecord(703, 1, "invalid", JobRunning, payload),
		preimageRecord(703, 2, "invalid", JobRunning, payload),
	})
}

func TestSubmitPreimageRejectsWrongPublicDigest(t *testing.T) {
	output, _, verifier := preimageProofFixture(t)
	payload := preimagePublicPayload(zk.ComputePreimageDigest(43))
	store := newRecordingPreimageStore()
	s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
	var calls atomic.Int32
	registerTestWorker(t, s, "wrong-digest", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: output,
		}, nil
	})

	resp, err := submitPreimageJob(t, s, 704, payload)
	if resp != nil || status.Code(err) != codes.Unavailable || calls.Load() != 2 {
		t.Fatalf("wrong public digest accepted: response=%+v error=%v calls=%d", resp, err, calls.Load())
	}
	assertPreimageSaves(t, store, []JobRecord{
		preimageRecord(704, 1, "wrong-digest", JobRunning, payload),
		preimageRecord(704, 2, "wrong-digest", JobRunning, payload),
	})
}

func TestSubmitPreimageFencesBeforeVerification(t *testing.T) {
	for _, scenario := range []string{"wrong job", "wrong attempt", "expired lease"} {
		t.Run(scenario, func(t *testing.T) {
			payload := preimagePublicPayload(zk.ComputePreimageDigest(42))
			store := newRecordingPreimageStore()
			verifier := &countingPreimageVerifier{}
			s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
			var calls atomic.Int32
			registerTestWorker(t, s, "fenced", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				resp := &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded",
					Output: base64.StdEncoding.EncodeToString([]byte("proof")),
				}
				switch scenario {
				case "wrong job":
					resp.JobId++
				case "wrong attempt":
					resp.AttemptId++
				case "expired lease":
					s.setLease(req.JobId, req.AttemptId, "fenced", -time.Second)
				}
				return resp, nil
			})

			resp, err := submitPreimageJob(t, s, 705, payload)
			if resp != nil || status.Code(err) != codes.Unavailable || calls.Load() != 2 {
				t.Fatalf("fenced response accepted: response=%+v error=%v calls=%d", resp, err, calls.Load())
			}
			if got := verifier.calls.Load(); got != 0 {
				t.Fatalf("verifier called %d times for fenced responses", got)
			}
			assertPreimageSaves(t, store, []JobRecord{
				preimageRecord(705, 1, "fenced", JobRunning, payload),
				preimageRecord(705, 2, "fenced", JobRunning, payload),
			})
		})
	}
}

func TestSubmitPreimageTerminalFailureSkipsVerifier(t *testing.T) {
	for _, tc := range []struct {
		status string
		state  JobState
	}{
		{"Failed", JobFailed},
		{"Cancelled", JobCancelled},
	} {
		t.Run(tc.status, func(t *testing.T) {
			payload := preimagePublicPayload(zk.ComputePreimageDigest(42))
			store := newRecordingPreimageStore()
			verifier := &countingPreimageVerifier{}
			s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier}
			var calls atomic.Int32
			registerTestWorker(t, s, "terminal", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				return &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId, Status: tc.status, Error: "task error",
				}, nil
			})

			resp, err := submitPreimageJob(t, s, 706, payload)
			if err != nil || resp == nil || resp.Status != tc.status || resp.AttemptId != 1 || calls.Load() != 1 {
				t.Fatalf("terminal task result retried or rejected: response=%+v error=%v calls=%d", resp, err, calls.Load())
			}
			if got := verifier.calls.Load(); got != 0 {
				t.Fatalf("verifier called %d times for %s", got, tc.status)
			}
			assertPreimageSaves(t, store, []JobRecord{
				preimageRecord(706, 1, "terminal", JobRunning, payload),
				preimageRecord(706, 1, "terminal", tc.state, payload),
			})
		})
	}
}

func TestSubmitNonZKSuccessSkipsPreimageVerifier(t *testing.T) {
	verifier := &countingPreimageVerifier{}
	s := &CoordinatorServer{jobStore: NewMemoryJobStore(), preimageVerifier: verifier}
	var calls atomic.Int32
	registerTestWorker(t, s, "hash-worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
		JobId: 707, TaskType: "hash", Payload: "hello", TimeoutMs: preimageTestTimeoutMs,
	})
	if err != nil || resp == nil || resp.Status != "Succeeded" || resp.AttemptId != 1 || calls.Load() != 1 {
		t.Fatalf("non-ZK result changed: response=%+v error=%v calls=%d", resp, err, calls.Load())
	}
	if got := verifier.calls.Load(); got != 0 {
		t.Fatalf("preimage verifier called %d times for hash task", got)
	}
}

func TestSubmitPreimageWithoutVerifierRejectsSuccess(t *testing.T) {
	payload := preimagePublicPayload(zk.ComputePreimageDigest(42))
	store := newRecordingPreimageStore()
	s := &CoordinatorServer{jobStore: store}
	var calls atomic.Int32
	registerTestWorker(t, s, "unverified", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return &runtimepb.ExecuteJobResponse{
			JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded",
			Output: base64.StdEncoding.EncodeToString([]byte("proof")),
		}, nil
	})

	resp, err := submitPreimageJob(t, s, 708, payload)
	if resp != nil || status.Code(err) != codes.Unavailable ||
		!strings.Contains(err.Error(), "preimage verifier is not configured") || calls.Load() != 2 {
		t.Fatalf("missing verifier accepted success: response=%+v error=%v calls=%d", resp, err, calls.Load())
	}
	assertPreimageSaves(t, store, []JobRecord{
		preimageRecord(708, 1, "unverified", JobRunning, payload),
		preimageRecord(708, 2, "unverified", JobRunning, payload),
	})
}
