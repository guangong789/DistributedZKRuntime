package main

import (
	"context"
	"errors"
	"flag"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
)

// Only the first verification is held, so lease-expiry retry proceeds normally.
// Every released call still verifies a real Groth16 proof.
type acceptanceBarrierVerifier struct {
	verifier PreimageProofVerifier
	entered  chan struct{}
	release  chan struct{}
	calls    atomic.Int32
}

func (v *acceptanceBarrierVerifier) Verify(proof []byte, digest fr.Element) error {
	if v.calls.Add(1) == 1 {
		close(v.entered)
		<-v.release
	}
	return v.verifier.Verify(proof, digest)
}

// The rejection cases enforce final acceptance after potentially slow Verify.
func TestResultAcceptanceBoundaries(t *testing.T) {
	for _, scenario := range []string{
		"lease expires during verification",
		"parent canceled during verification",
		"valid before deadline",
	} {
		t.Run(scenario, func(t *testing.T) {
			const jobID int64 = 9301
			const workerID = "acceptance-worker"
			output, digest, realVerifier := preimageProofFixture(t)
			payload := preimagePublicPayload(digest)
			store := newWorkerFailureStore(t) // Records successful writes to real SQLite.
			verifier := &acceptanceBarrierVerifier{
				verifier: realVerifier, entered: make(chan struct{}), release: make(chan struct{}),
			}
			observer := &recordingMetrics{}
			fs := flag.NewFlagSet("coordinator", flag.ContinueOnError)
			execution := coordinatorExecutionFlags(fs)
			leaseFlag := "5s"
			if scenario == "lease expires during verification" {
				leaseFlag = "250ms"
			}
			if err := fs.Parse([]string{"-attempt-lease", leaseFlag}); err != nil {
				t.Fatal(err)
			}
			if err := execution.validate(); err != nil {
				t.Fatal(err)
			}
			s := &CoordinatorServer{jobStore: store, preimageVerifier: verifier, leaseDuration: execution.AttemptLease, metrics: observer}
			observer.hook = func(event recordedMetric) {
				switch event.name {
				case "started", "finished", "terminal", "verification":
					// Reentering here also catches metrics emitted under s.mu.
					s.getLease(jobID)
				}
			}
			var workerCalls atomic.Int32
			registerTestWorker(t, s, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				workerCalls.Add(1)
				return &runtimepb.ExecuteJobResponse{
					JobId: req.JobId, AttemptId: req.AttemptId, Status: "Succeeded", Output: output,
				}, nil
			})

			parent, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			waitCtx, stopWaiting := context.WithTimeout(context.Background(), 10*time.Second)
			defer stopWaiting()
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(verifier.release) }) }
			finished := make(chan struct{})
			var result workerFailureResult
			t.Cleanup(func() {
				cancel()
				release()
				select {
				case <-finished:
				case <-time.After(5 * time.Second):
					t.Error("submission did not finish during cleanup")
				}
			})
			go func() {
				defer close(finished)
				result.resp, result.err = s.SubmitJob(parent, &runtimepb.SubmitJobRequest{
					JobId: jobID, TaskType: "zk_preimage_prove", Payload: payload, TimeoutMs: preimageTestTimeoutMs,
				})
			}()
			select {
			case <-verifier.entered:
			case <-waitCtx.Done():
				t.Fatal("first response did not reach verification")
			}
			lease, ok := s.getLease(jobID)
			if !ok || lease.AttemptID != 1 || !time.Now().Before(lease.ExpiresAt) {
				t.Fatalf("test requires verification to begin within Attempt 1's real lease: %+v", lease)
			}
			if err := parent.Err(); err != nil {
				t.Fatalf("parent expired before the controlled boundary: %v", err)
			}
			assertWorkerFailureSaves(t, store, []JobRecord{
				preimageRecord(jobID, 1, workerID, JobRunning, payload),
			})

			switch scenario {
			case "lease expires during verification":
				// Wait for the actual stored deadline; do not mutate the lease or
				// approximate expiry with a sleep measured from test startup.
				timer := time.NewTimer(time.Until(lease.ExpiresAt))
				defer timer.Stop()
				select {
				case <-timer.C:
				case <-waitCtx.Done():
					t.Fatal("lease deadline did not arrive")
				}
				if !s.leaseExpired(jobID, time.Now()) || s.isLeaseCurrent(jobID, 1) {
					t.Fatal("Attempt 1 must be expired before releasing Verify")
				}
				if err := parent.Err(); err != nil {
					t.Fatalf("lease expiry must be isolated from parent cancellation: %v", err)
				}
			case "parent canceled during verification":
				cancel()
				<-parent.Done()
				if !errors.Is(parent.Err(), context.Canceled) || !s.isLeaseCurrent(jobID, 1) {
					t.Fatal("test requires parent cancellation while the lease is still valid")
				}
			}
			release()
			select {
			case <-finished:
			case <-waitCtx.Done():
				t.Fatal("submission did not finish after releasing Verify")
			}

			record, found, err := store.Load(jobID)
			if err != nil || !found {
				t.Fatalf("load durable outcome: found=%v error=%v", found, err)
			}
			saves := store.savedRecords()
			t.Logf("durable state=%v (Succeeded=%v), attempt=%d; Verify calls=%d; worker calls=%d; response status=%q, attempt=%d; error=%v",
				record.State, JobSucceeded, record.AttemptID, verifier.calls.Load(), workerCalls.Load(), result.resp.GetStatus(), result.resp.GetAttemptId(), result.err)
			if !s.claimJob(jobID) {
				t.Error("submission retained JobID ownership")
			} else {
				s.releaseJob(jobID)
			}
			switch scenario {
			case "lease expires during verification":
				// A fresh retry may succeed. No write or response may accept the
				// expired first attempt, even if a later write would hide it.
				for _, saved := range saves {
					if saved.AttemptID == 1 && saved.State == JobSucceeded {
						t.Error("accepted expired Attempt 1: SQLite saved JobSucceeded after its lease expired during Verify")
					}
				}
				if result.resp != nil && result.resp.AttemptId == 1 && result.resp.Status == "Succeeded" {
					t.Error("returned success for expired Attempt 1")
				}
				want := preimageRecord(jobID, 2, workerID, JobSucceeded, payload)
				if result.err != nil || result.resp == nil || result.resp.AttemptId != 2 ||
					result.resp.Status != "Succeeded" || record != want || workerCalls.Load() != 2 || verifier.calls.Load() != 2 {
					t.Errorf("lease-expiry retry: record=%+v response=%v error=%v calls=%d Verify=%d",
						record, result.resp, result.err, workerCalls.Load(), verifier.calls.Load())
				}
				assertWorkerFailureSaves(t, store, []JobRecord{
					preimageRecord(jobID, 1, workerID, JobRunning, payload),
					preimageRecord(jobID, 2, workerID, JobRunning, payload), want,
				})
				if observer.count("finished", string(metrics.AttemptFenced)) != 1 ||
					observer.count("rejected", string(metrics.RejectFencing)) != 1 {
					t.Error("final lease rejection must use the existing fencing metrics once")
				}
			case "parent canceled during verification":
				for _, saved := range saves {
					if saved.State == JobSucceeded {
						t.Error("SQLite saved JobSucceeded after parent cancellation was established before Verify returned")
					}
				}
				if !errors.Is(result.err, context.Canceled) || result.resp != nil {
					t.Errorf("canceled submission returned status=%q attempt=%d error=%v, want nil response and context.Canceled",
						result.resp.GetStatus(), result.resp.GetAttemptId(), result.err)
				}
				if workerCalls.Load() != 1 || verifier.calls.Load() != 1 {
					t.Error("parent cancellation started an extra attempt")
				}
				assertWorkerFailureSaves(t, store, []JobRecord{
					preimageRecord(jobID, 1, workerID, JobRunning, payload),
					preimageRecord(jobID, 1, workerID, JobCancelled, payload),
				})
				if record.State != JobCancelled || observer.count("terminal", string(metrics.TerminalCancelled)) != 1 {
					t.Error("normal-submit cancellation must durably persist and report Cancelled")
				}
				if observer.count("finished", string(metrics.AttemptCancelled)) != 1 ||
					observer.count("rejected", "") != 0 {
					t.Error("parent cancellation must finish once as cancelled, without fencing/retry")
				}
			case "valid before deadline":
				if parent.Err() != nil || !s.isLeaseCurrent(jobID, 1) {
					t.Fatal("positive control exceeded its parent deadline or lease")
				}
				if result.err != nil || result.resp == nil || result.resp.JobId != jobID ||
					result.resp.AttemptId != 1 || result.resp.Status != "Succeeded" || result.resp.Output != output {
					t.Fatalf("valid proof not accepted: response=%v error=%v", result.resp, result.err)
				}
				want := preimageRecord(jobID, 1, workerID, JobSucceeded, payload)
				if record != want || workerCalls.Load() != 1 || verifier.calls.Load() != 1 {
					t.Errorf("valid acceptance record=%+v, want %+v with one dispatch and verification", record, want)
				}
				assertWorkerFailureSaves(t, store, []JobRecord{
					preimageRecord(jobID, 1, workerID, JobRunning, payload), want,
				})
			}
			wantAttempts, wantTerminals, wantAccepted := 1, 1, 1
			if scenario == "lease expires during verification" {
				wantAttempts = 2
			} else if scenario == "parent canceled during verification" {
				wantAccepted = 0
			}
			if observer.count("submitted", "") != 1 || observer.count("started", "") != wantAttempts ||
				observer.count("finished", "") != wantAttempts || observer.count("verification", "") != wantAttempts ||
				observer.count("terminal", "") != wantTerminals ||
				observer.count("finished", string(metrics.AttemptAccepted)) != wantAccepted {
				t.Errorf("acceptance events doubled, missing, or emitted for invalid success: %+v", observer.snapshot())
			}
		})
	}
}

// Non-ZK terminal outcomes use the same final gate without a verification step.
// Exercise the gate directly so invalidation occurs immediately before Save,
// rather than being intercepted by the earlier RPC-response check.
func TestPersistTerminalResultRejectsInvalidNonZKResult(t *testing.T) {
	for _, terminal := range []struct {
		name  string
		state JobState
	}{
		{"Succeeded", JobSucceeded},
		{"Failed", JobFailed},
		{"Cancelled", JobCancelled},
	} {
		for _, invalidation := range []string{"expired", "superseded", "cancelled and expired"} {
			t.Run(terminal.name+"/"+invalidation, func(t *testing.T) {
				inner := NewMemoryJobStore()
				saves := 0
				s := &CoordinatorServer{jobStore: &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
					saves++
					return inner.Save(record)
				}}}
				const jobID int64 = 9302
				if !s.claimJob(jobID) {
					t.Fatal("cannot claim job")
				}
				defer s.releaseJob(jobID)
				attempt, err := s.startAttempt(jobID, "worker", time.Hour, "hash", "hello", 1000)
				if err != nil {
					t.Fatal(err)
				}
				ctx, cancel := context.WithCancel(context.Background())
				defer cancel()
				if invalidation == "superseded" {
					if _, err := s.startAttempt(jobID, "new-worker", time.Hour, "hash", "hello", 1000); err != nil {
						t.Fatal(err)
					}
				} else {
					s.setLease(jobID, attempt, "worker", -time.Second)
					if invalidation == "cancelled and expired" {
						cancel()
					}
				}
				before, ok, err := inner.Load(jobID)
				if err != nil || !ok {
					t.Fatalf("load current record: found=%v error=%v", ok, err)
				}
				record := JobRecord{JobID: jobID, State: terminal.state, AttemptID: attempt, WorkerID: "worker",
					TaskType: "hash", Payload: "hello", TimeoutMs: 1000}
				beforeSaves := saves
				eligible, err := s.persistTerminalResult(ctx, record)
				if eligible || (invalidation == "cancelled and expired" && !errors.Is(err, context.Canceled)) ||
					(invalidation != "cancelled and expired" && err != nil) {
					t.Fatalf("invalid result: eligible=%v error=%v", eligible, err)
				}
				after, _, err := inner.Load(jobID)
				if err != nil || after != before || saves != beforeSaves {
					t.Fatalf("invalid terminal changed persistence: before=%+v after=%+v saves=%d/%d error=%v",
						before, after, beforeSaves, saves, err)
				}
			})
		}
	}
}
