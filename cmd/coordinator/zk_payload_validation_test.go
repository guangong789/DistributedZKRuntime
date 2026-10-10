package main

import (
	"context"
	"encoding/base64"
	"math/big"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestSubmitRejectsPrivateZKPayloadBeforeSideEffects(t *testing.T) {
	output, digest, verifier := preimageProofFixture(t)
	store := newWorkerFailureStore(t)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier, metrics: observer}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return &runtimepb.ExecuteJobResponse{JobId: r.JobId, AttemptId: r.AttemptId, Status: "Succeeded", Output: output}, nil
	})
	const marker = "PRIVATE_PAYLOAD_MARKER"
	payload := strings.TrimSuffix(preimagePublicPayload(digest), "}") + `,"secret":"` + marker + `"}`
	resp, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 9900, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 1000})
	if resp != nil || status.Code(err) != codes.InvalidArgument {
		t.Fatalf("private input accepted: code=%v status=%s attempt=%d", status.Code(err), resp.GetStatus(), resp.GetAttemptId())
	}
	if strings.Contains(err.Error(), marker) {
		t.Fatal("rejection echoes private input")
	}
	if _, found, loadErr := store.Load(9900); loadErr != nil || found {
		t.Fatal("invalid input created durable record")
	}
	if len(store.savedRecords()) != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || len(s.activeJobs) != 0 || s.nextWorker != 0 || calls.Load() != 0 || observer.count("submitted", "") != 0 || observer.count("execution", "") != 0 {
		t.Fatal("invalid input caused side effects")
	}
}

func TestSubmitStrictZKPayloadBoundary(t *testing.T) {
	store := newWorkerFailureStore(t)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, metrics: observer}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(r), nil
	})
	for _, payload := range []string{
		`{"witness_ref":"ref","digest":"1","private_input":"PRIVATE_VALUE_MARKER"}`,
		`{"witness_ref":"ref","digest":"1","digest":"2"}`,
		`{"Witness_Ref":"ref","digest":"1"}`,
		`{"witness_ref":"ref","digest":null}`,
		`{"witness_ref":"ref","digest":1}`,
		`{"digest":"1"}`, `{"witness_ref":"ref"}`, `null`,
		`{"witness_ref":"ref","digest":"PRIVATE_VALUE_MARKER"}`, strings.Repeat("x", zk.MaxPreimagePayloadBytes+1),
	} {
		resp, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 9902, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: 1000})
		if resp != nil || status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), "PRIVATE_VALUE_MARKER") {
			t.Fatal("malformed input was accepted or echoed")
		}
	}
	if len(store.savedRecords()) != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || len(s.activeJobs) != 0 || s.nextWorker != 0 || calls.Load() != 0 || observer.count("submitted", "") != 0 || observer.count("execution", "") != 0 {
		t.Fatal("malformed input caused submission side effects")
	}
	// Reject before claim, without disturbing a different session's ownership.
	if !s.claimJob(9902) {
		t.Fatal("cannot preclaim")
	}
	defer s.releaseJob(9902)
	if _, err := s.SubmitJob(context.Background(), &runtimepb.SubmitJobRequest{JobId: 9902, TaskType: "zk_preimage_prove", Payload: `{"secret":"PRIVATE_VALUE_MARKER"}`}); status.Code(err) != codes.InvalidArgument {
		t.Fatal("ownership happened before validation")
	}
	if s.claimJob(9902) {
		t.Fatal("rejection released existing owner")
	}
}

func TestSubmitCanonicalZKPayloadPersistsAndRetries(t *testing.T) {
	fixture := newIntegrationZKFixture(t)
	store := newWorkerFailureStore(t)
	s := &CoordinatorServer{jobStore: store, preimageVerifier: fixture.verifier}
	digest := zk.ComputePreimageDigest(42)
	var integer big.Int
	digest.BigInt(&integer)
	legacy := ` {"digest":"+000` + integer.String() + `", "witness_ref":"secret-001"} `
	var failedCalls, proofCalls atomic.Int32
	registerTestWorker(t, s, "busy", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		failedCalls.Add(1)
		if r.Payload != fixture.payload || r.AttemptId != 1 {
			t.Error("raw JSON reached first worker")
		}
		return nil, status.Error(codes.ResourceExhausted, "busy")
	})
	registerTestWorker(t, s, "prover", func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		proofCalls.Add(1)
		if r.Payload != fixture.payload || r.AttemptId != 2 {
			t.Error("retry lost canonical spec")
		}
		return fixture.execute(ctx, r, nil)
	})
	client := startTestCoordinator(t, s)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	resp, err := client.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 9903, TaskType: "zk_preimage_prove", Payload: legacy, TimeoutMs: preimageTestTimeoutMs})
	if err != nil || resp == nil || resp.Status != "Succeeded" || resp.AttemptId != 2 || failedCalls.Load() != 1 || proofCalls.Load() != 1 {
		t.Fatal("valid canonical job did not retry/succeed")
	}
	proof, err := base64.StdEncoding.DecodeString(resp.Output)
	if err != nil || fixture.verifier.Verify(proof, digest) != nil {
		t.Fatal("real returned proof did not verify")
	}
	assertWorkerFailureSaves(t, store, []JobRecord{
		preimageRecord(9903, 1, "busy", JobRunning, fixture.payload), preimageRecord(9903, 2, "prover", JobRunning, fixture.payload), preimageRecord(9903, 2, "prover", JobSucceeded, fixture.payload),
	})
	assertRecoveryOwnershipReleased(t, s, 9903)
}

func TestInternalExecutionRejectsPrivateZKPayload(t *testing.T) {
	store := newWorkerFailureStore(t)
	s := &CoordinatorServer{jobStore: store}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(r), nil
	})
	spec := JobSpec{JobID: 9904, TaskType: "zk_preimage_prove", Payload: `{"witness_ref":"ref","digest":"1","secret":"PRIVATE_VALUE_MARKER"}`}
	if !s.claimJob(spec.JobID) {
		t.Fatal("cannot claim")
	}
	defer s.releaseJob(spec.JobID)
	if _, err := s.executeJobFromSource(context.Background(), spec, metrics.SourceRecovery); status.Code(err) != codes.InvalidArgument {
		t.Fatal("internal shared execution bypassed payload boundary")
	}
	if len(store.savedRecords()) != 0 || calls.Load() != 0 || s.nextWorker != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 {
		t.Fatal("internal validation caused side effects")
	}
}
