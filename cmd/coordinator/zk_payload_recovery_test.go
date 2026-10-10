package main

import (
	"bytes"
	"context"
	"log"
	"math/big"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type payloadPrivacyLog struct {
	mu sync.Mutex
	bytes.Buffer
}

func (b *payloadPrivacyLog) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.Buffer.Write(p)
}
func (b *payloadPrivacyLog) text() string { b.mu.Lock(); defer b.mu.Unlock(); return b.Buffer.String() }

func TestZKPayloadLegacyRecoveryCanonicalizesWithoutResettingBackoff(t *testing.T) {
	fixture := newIntegrationZKFixture(t)
	store := newWorkerFailureStore(t)
	digest := zk.ComputePreimageDigest(42)
	var integer big.Int
	digest.BigInt(&integer)
	raw := ` { "digest" : "+00` + integer.String() + `", "witness_ref" : "secret-001" } `
	record := preimageRecord(9910, 7, "old-worker", JobRecovering, raw)
	saveRecoveryRecord(t, store, record)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, preimageVerifier: fixture.verifier, metrics: observer}
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	var calls atomic.Int32
	var fail atomic.Bool
	fail.Store(true)
	registerTestWorker(t, s, "worker", func(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		if req.Payload != fixture.payload {
			t.Error("legacy raw payload forwarded")
		}
		if fail.Load() {
			return nil, status.Error(codes.ResourceExhausted, "busy")
		}
		return fixture.execute(ctx, req, nil)
	})
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	e := q.entries[record.JobID]
	if e.failures != 1 || e.generation != 9 || !e.nextRetry.Equal(clock.now().Add(time.Second)) {
		t.Fatal("normalization broke first failed session schedule")
	}
	want := preimageRecord(record.JobID, 9, "worker", JobRecovering, fixture.payload)
	assertRecoveryRecord(t, store, want)
	// Same public task, different byte representation: no replacement/cooldown reset.
	representation := want
	representation.Payload = raw
	saveRecoveryRecord(t, store, representation)
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 2 || q.entries[record.JobID].failures != 1 {
		t.Fatal("equivalent legacy representation bypassed cooldown")
	}
	clock.advance(time.Second)
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	e = q.entries[record.JobID]
	if e.failures != 2 || e.generation != 11 || !e.nextRetry.Equal(clock.now().Add(2*time.Second)) {
		t.Fatal("canonicalized own generation lost failure history")
	}
	fail.Store(false)
	clock.advance(2 * time.Second)
	if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
		t.Fatal(err)
	}
	want.State, want.AttemptID = JobSucceeded, 12
	assertRecoveryRecord(t, store, want)
	if calls.Load() != 5 || len(q.entries) != 0 || observer.count("submitted", "") != 0 || observer.count("started", "") != 5 || observer.count("finished", "") != 5 || observer.count("verification", "") != 1 || observer.count("terminal", "") != 1 {
		t.Fatal("legacy recovery changed attempts, verification or metrics")
	}
	for _, saved := range store.savedRecords() {
		if saved.State == JobRunning || saved.State == JobSucceeded {
			if saved.Payload != fixture.payload {
				t.Fatal("running/terminal persistence used raw legacy JSON")
			}
		}
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestZKPayloadInvalidLegacyRecoveryRemainsSafeAndBacksOff(t *testing.T) {
	const marker = "PRIVATE_LEGACY_MARKER"
	store := newWorkerFailureStore(t)
	record := preimageRecord(9911, 7, "old-worker", JobRecovering, `{"witness_ref":"secret-001","digest":"1","secret":"`+marker+`"}`)
	saveRecoveryRecord(t, store, record)
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: store, metrics: observer}
	var calls atomic.Int32
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		calls.Add(1)
		return successfulResult(req), nil
	})
	var captured payloadPrivacyLog
	previous := log.Writer()
	log.SetOutput(&captured)
	defer log.SetOutput(previous)
	clock := newManualRecoveryClock()
	q := testRecoverySchedule(clock)
	for i, delay := range []time.Duration{time.Second, 2 * time.Second, 4 * time.Second, 8 * time.Second, 16 * time.Second, 30 * time.Second} {
		if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
			t.Fatal(err)
		}
		e := q.entries[record.JobID]
		if e.generation != 7 || e.failures != uint32(i+1) || !e.nextRetry.Equal(clock.now().Add(delay)) {
			t.Fatal("invalid legacy recovery did not use bounded session backoff")
		}
		for n := 0; n < 3; n++ {
			if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil {
				t.Fatal(err)
			}
		}
		if observer.count("recovery_failed", "") != i+1 {
			t.Fatal("early checks bypassed invalid legacy cooldown")
		}
		clock.advance(delay)
	}
	assertRecoveryRecord(t, store, record)
	if len(store.savedRecords()) != 1 || calls.Load() != 0 || s.nextWorker != 0 || len(s.jobAttempts) != 0 || len(s.leases) != 0 || len(s.activeJobs) != 0 || observer.count("started", "") != 0 || observer.count("finished", "") != 0 || observer.count("terminal", "") != 0 || observer.count("submitted", "") != 0 {
		t.Fatal("invalid legacy payload was dispatched, resaved or completed")
	}
	if strings.Contains(captured.text(), marker) || strings.Contains(captured.text(), record.Payload) {
		t.Fatal("recovery log disclosed legacy private payload")
	}
	// Direct recovery entry keeps InvalidArgument, releases ownership and echoes
	// no client-controlled values. It never repairs/resaves the unsafe old row.
	if err := s.retryRecoveringJob(context.Background(), record); status.Code(err) != codes.InvalidArgument || strings.Contains(err.Error(), marker) {
		t.Fatal("invalid legacy recovery error is unsafe")
	}
	assertRecoveryOwnershipReleased(t, s, record.JobID)
}

func TestZKPayloadStartupRecoveryHandlesLegacyRows(t *testing.T) {
	digest := zk.ComputePreimageDigest(42)
	var integer big.Int
	digest.BigInt(&integer)
	for _, invalid := range []bool{false, true} {
		t.Run(map[bool]string{false: "valid legacy", true: "unsafe legacy"}[invalid], func(t *testing.T) {
			store := newWorkerFailureStore(t)
			observer := &recordingMetrics{}
			raw := ` {"digest":"+00` + integer.String() + `","witness_ref":"secret-001"} `
			if invalid {
				raw = `{"witness_ref":"secret-001","digest":"1","private_input":"PRIVATE_LEGACY_MARKER"}`
			}
			record := preimageRecord(9912, 1, "old-worker", JobRunning, raw)
			saveRecoveryRecord(t, store, record)
			s := &CoordinatorServer{jobStore: store, metrics: observer}
			if err := s.recoverRunningJobs(); err != nil {
				t.Fatal(err)
			}
			if invalid {
				assertRecoveryRecord(t, store, record)
				if len(store.savedRecords()) != 1 || observer.count("transition", "") != 0 {
					t.Fatal("unsafe legacy row was rewritten")
				}
				q := testRecoverySchedule(newManualRecoveryClock())
				if err := s.runScheduledRecoveryPass(context.Background(), q); err != nil || len(q.entries) != 0 {
					t.Fatal("stranded unsafe Running row was scheduled")
				}
			} else {
				record.State, record.Payload = JobRecovering, preimagePublicPayload(digest)
				assertRecoveryRecord(t, store, record)
				if len(store.savedRecords()) != 2 || observer.count("transition", string(metrics.RecoveryStartup)) != 1 {
					t.Fatal("valid legacy startup marking failed")
				}
			}
			if len(s.jobAttempts) != 0 || len(s.leases) != 0 || len(s.activeJobs) != 0 {
				t.Fatal("startup marking allocated execution state")
			}
		})
	}
}
