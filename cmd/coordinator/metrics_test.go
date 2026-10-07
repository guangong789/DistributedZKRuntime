package main

import (
	"context"
	"encoding/base64"
	"errors"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type recordedMetric struct {
	name     string
	source   metrics.ExecutionSource
	label    string
	duration time.Duration
}

type recordingMetrics struct {
	metrics.NoopMetrics
	mu     sync.Mutex
	events []recordedMetric
	// Set before serving. The hook runs outside the recorder lock.
	hook func(recordedMetric)
}

func TestMetricsStartAttemptAfterPublication(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(map[bool]string{false: "published", true: "save failure"}[fail], func(t *testing.T) {
			observer := &recordingMetrics{}
			inner := NewMemoryJobStore()
			s := &CoordinatorServer{metrics: observer}
			s.jobStore = &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
				if fail {
					return errors.New("injected Running Save failure")
				}
				return inner.Save(record)
			}}
			observer.hook = func(event recordedMetric) {
				record, ok, err := inner.Load(908)
				lease, leased := s.getLease(908)
				s.mu.Lock()
				published := s.jobAttempts[908]
				s.mu.Unlock()
				if err != nil || !ok || record.State != JobRunning || !leased ||
					lease.AttemptID != record.AttemptID || published != record.AttemptID {
					t.Error("AttemptStarted occurred before persistence/publication")
				}
			}
			attempt, err := s.startAttempt(908, "worker", time.Minute, "hash", "hello", 1000)
			if fail {
				if err == nil || observer.count("started", "") != 0 || attempt != 0 {
					t.Fatalf("failed save emitted start: attempt=%d error=%v events=%v", attempt, err, observer.snapshot())
				}
			} else if err != nil || attempt != 1 || observer.count("started", "") != 1 {
				t.Fatalf("published start: attempt=%d error=%v events=%v", attempt, err, observer.snapshot())
			}
		})
	}
}

func (m *recordingMetrics) record(e recordedMetric) {
	m.mu.Lock()
	m.events = append(m.events, e)
	m.mu.Unlock()
	if m.hook != nil {
		m.hook(e)
	}
}
func (m *recordingMetrics) snapshot() []recordedMetric {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]recordedMetric(nil), m.events...)
}
func (m *recordingMetrics) JobSubmitted(s metrics.ExecutionSource) {
	m.record(recordedMetric{name: "submitted", source: s})
}
func (m *recordingMetrics) JobTerminal(s metrics.ExecutionSource, state metrics.TerminalState) {
	m.record(recordedMetric{name: "terminal", source: s, label: string(state)})
}
func (m *recordingMetrics) ObserveJobExecution(s metrics.ExecutionSource, d time.Duration) {
	m.record(recordedMetric{name: "execution", source: s, duration: d})
}
func (m *recordingMetrics) AttemptStarted(s metrics.ExecutionSource) {
	m.record(recordedMetric{name: "started", source: s})
}
func (m *recordingMetrics) AttemptFinished(s metrics.ExecutionSource, o metrics.AttemptOutcome, d time.Duration) {
	m.record(recordedMetric{name: "finished", source: s, label: string(o), duration: d})
}
func (m *recordingMetrics) AttemptRejected(s metrics.ExecutionSource, r metrics.AttemptRejectReason) {
	m.record(recordedMetric{name: "rejected", source: s, label: string(r)})
}
func (m *recordingMetrics) WorkerStateChanged(from, to metrics.WorkerState, cause metrics.WorkerStateChangeCause) {
	m.record(recordedMetric{name: "worker", label: string(from) + "/" + string(to) + "/" + string(cause)})
}
func (m *recordingMetrics) RecoveryPass(o metrics.RecoveryPassOutcome, d time.Duration) {
	m.record(recordedMetric{name: "pass", label: string(o), duration: d})
}
func (m *recordingMetrics) RecoveryTransition(c metrics.RecoveryTransitionCause) {
	m.record(recordedMetric{name: "transition", label: string(c)})
}
func (m *recordingMetrics) RecoveryExecutionFailed(r metrics.RecoveryFailureReason) {
	m.record(recordedMetric{name: "recovery_failed", label: string(r)})
}
func (m *recordingMetrics) RecoveryRepairFailed(s metrics.RecoveryRepairStage) {
	m.record(recordedMetric{name: "repair_failed", label: string(s)})
}
func (m *recordingMetrics) ObserveZKVerification(d time.Duration) {
	m.record(recordedMetric{name: "verification", duration: d})
}
func (m *recordingMetrics) count(name, label string) int {
	count := 0
	for _, e := range m.snapshot() {
		if e.name == name && (label == "" || e.label == label) {
			count++
		}
	}
	return count
}

func TestMetricsPersistenceOrdering(t *testing.T) {
	for _, scenario := range []string{"Succeeded", "Failed", "Cancelled", "running save failure", "terminal save failure", "nil store"} {
		t.Run(scenario, func(t *testing.T) {
			inner := NewMemoryJobStore()
			observer := &recordingMetrics{}
			s := &CoordinatorServer{metrics: observer}
			s.jobStore = &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
				if (scenario == "running save failure" && record.State == JobRunning) ||
					(scenario == "terminal save failure" && record.State != JobRunning) {
					return errors.New("injected save failure")
				}
				if err := inner.Save(record); err != nil {
					return err
				}
				name := "running_saved"
				if record.State != JobRunning {
					name = "terminal_saved"
				}
				observer.record(recordedMetric{name: name})
				return nil
			}}
			if scenario == "nil store" {
				s.jobStore = nil
			}
			observer.hook = func(e recordedMetric) {
				if e.name == "started" {
					record, ok, err := inner.Load(901)
					lease, hasLease := s.getLease(901)
					s.mu.Lock()
					attempt := s.jobAttempts[901]
					s.mu.Unlock()
					if err != nil || !ok || record.State != JobRunning ||
						!hasLease || lease.AttemptID != record.AttemptID || attempt != record.AttemptID {
						t.Errorf("started event preceded durable/in-memory publication")
					}
				}
				if e.name == "terminal" {
					record, ok, err := inner.Load(901)
					if err != nil || !ok || record.State == JobRunning {
						t.Errorf("terminal event preceded terminal persistence")
					}
				}
			}
			var calls atomic.Int32
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				calls.Add(1)
				resp := successfulResult(req)
				if scenario == "Failed" || scenario == "Cancelled" {
					resp.Status = scenario
				}
				return resp, nil
			})
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: 901, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
			want := []string{"submitted", "running_saved", "started", "terminal_saved", "terminal", "finished", "execution"}
			switch scenario {
			case "running save failure":
				want = []string{"submitted", "execution"}
				if status.Code(err) != codes.Internal || resp != nil || calls.Load() != 0 {
					t.Fatalf("running save failure: response=%v error=%v calls=%d", resp, err, calls.Load())
				}
			case "terminal save failure":
				want = []string{"submitted", "running_saved", "started", "finished", "execution"}
				if status.Code(err) != codes.Internal || resp != nil ||
					observer.count("finished", string(metrics.AttemptPersistenceError)) != 1 {
					t.Fatalf("terminal save failure: response=%v error=%v", resp, err)
				}
			case "nil store":
				want = []string{"submitted", "execution"}
				if err != nil || resp == nil || resp.Status != "Succeeded" {
					t.Fatalf("nil-store behavior changed: response=%v error=%v", resp, err)
				}
			default:
				if err != nil || resp == nil || resp.Status != scenario {
					t.Fatalf("terminal response=%v error=%v", resp, err)
				}
			}
			var names []string
			for _, e := range observer.snapshot() {
				if e.name == "worker" {
					continue
				}
				names = append(names, e.name)
				if e.name == "finished" || e.name == "execution" {
					if e.source != metrics.SourceSubmit || e.duration < 0 {
						t.Errorf("invalid duration/source: %+v", e)
					}
				}
			}
			if !reflect.DeepEqual(names, want) {
				t.Fatalf("events=%v, want %v", names, want)
			}
		})
	}
}

type metricVerifier struct {
	verify func([]byte, fr.Element) error
}

func (v metricVerifier) Verify(proof []byte, digest fr.Element) error {
	return v.verify(proof, digest)
}

func TestMetricsZKVerificationOrdering(t *testing.T) {
	validOutput, digest, realVerifier := preimageProofFixture(t)
	payload := preimagePublicPayload(digest)
	for _, scenario := range []string{"valid", "fenced", "invalid proof"} {
		t.Run(scenario, func(t *testing.T) {
			observer := &recordingMetrics{}
			inner := NewMemoryJobStore()
			s := &CoordinatorServer{metrics: observer}
			s.jobStore = &recoveryPassStore{JobStore: inner, save: func(record JobRecord) error {
				if err := inner.Save(record); err != nil {
					return err
				}
				if record.State == JobSucceeded {
					observer.record(recordedMetric{name: "terminal_saved"})
				}
				return nil
			}}
			s.preimageVerifier = metricVerifier{verify: func(proof []byte, digest fr.Element) error {
				observer.record(recordedMetric{name: "verifier_called"})
				return realVerifier.Verify(proof, digest)
			}}
			registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				resp := successfulResult(req)
				resp.Output = validOutput
				if scenario == "fenced" {
					resp.JobId++
				} else if scenario == "invalid proof" {
					resp.Output = base64.StdEncoding.EncodeToString([]byte("corrupted proof"))
				}
				return resp, nil
			})
			resp, err := submitPreimageJob(t, s, 902, payload)
			if scenario == "valid" {
				if err != nil || resp == nil || resp.Status != "Succeeded" {
					t.Fatalf("valid proof response=%v error=%v", resp, err)
				}
				var order []string
				for _, e := range observer.snapshot() {
					switch e.name {
					case "verifier_called", "verification", "terminal_saved", "terminal":
						order = append(order, e.name)
					}
				}
				want := []string{"verifier_called", "verification", "terminal_saved", "terminal"}
				if !reflect.DeepEqual(order, want) || observer.count("started", "") != 1 ||
					observer.count("finished", string(metrics.AttemptAccepted)) != 1 {
					t.Fatalf("valid proof metric ordering=%v", order)
				}
				return
			}
			if resp != nil || status.Code(err) != codes.Unavailable || observer.count("submitted", "") != 1 ||
				observer.count("started", "") != 2 || observer.count("finished", "") != 2 ||
				observer.count("terminal", "") != 0 {
				t.Fatalf("rejected proof response=%v error=%v events=%+v", resp, err, observer.snapshot())
			}
			if scenario == "fenced" {
				if observer.count("rejected", string(metrics.RejectFencing)) != 2 ||
					observer.count("verification", "") != 0 || observer.count("verifier_called", "") != 0 {
					t.Fatalf("fenced responses invoked verification: %+v", observer.snapshot())
				}
			} else if observer.count("rejected", string(metrics.RejectProof)) != 2 ||
				observer.count("verification", "") != 2 ||
				observer.count("finished", string(metrics.AttemptProofRejected)) != 2 {
				t.Fatalf("invalid proof metrics=%+v", observer.snapshot())
			}
		})
	}
}

func TestMetricsWorkerTransitionsOnlyOnChanges(t *testing.T) {
	observer := &recordingMetrics{}
	s := &CoordinatorServer{metrics: observer}
	// Observer callbacks must not run while the registry mutex is held.
	observer.hook = func(e recordedMetric) {
		s.mu.Lock()
		s.mu.Unlock()
	}
	register := func() {
		t.Helper()
		if resp, err := s.RegisterWorker(context.Background(), &runtimepb.RegisterWorkerRequest{WorkerId: "worker", Address: "localhost:1234"}); err != nil || !resp.Accepted {
			t.Fatalf("registration=%v error=%v", resp, err)
		}
	}
	expire := func() {
		s.mu.Lock()
		worker := s.workers["worker"]
		worker.LastSeen = time.Now().Add(-time.Hour)
		s.workers[worker.ID] = worker
		s.mu.Unlock()
		s.checkWorkerLiveness(time.Second)
		s.checkWorkerLiveness(time.Second)
	}
	register()
	register()
	for i := 0; i < 3; i++ {
		if resp, err := s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: "worker"}); err != nil || !resp.Accepted {
			t.Fatalf("heartbeat=%v error=%v", resp, err)
		}
	}
	expire()
	if observer.count("worker", "alive/dead/heartbeat_timeout") != 1 ||
		observer.count("worker", "") != 2 {
		t.Fatalf("duplicate transition/timeout events=%+v", observer.snapshot())
	}
	_, _ = s.Heartbeat(context.Background(), &runtimepb.HeartbeatRequest{WorkerId: "worker"})
	expire()
	register()
	if observer.count("worker", "dead/alive/heartbeat") != 1 ||
		observer.count("worker", "dead/alive/registration") != 1 ||
		observer.count("worker", "alive/dead/heartbeat_timeout") != 2 {
		t.Fatalf("revival events=%+v", observer.snapshot())
	}
}

func TestMetricsConcurrentSubmissions(t *testing.T) {
	const jobs = 20
	observer := &recordingMetrics{}
	s := &CoordinatorServer{jobStore: NewMemoryJobStore(), metrics: observer}
	registerTestWorker(t, s, "worker", func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		return successfulResult(req), nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := 0; i < jobs; i++ {
		wg.Add(1)
		go func(id int64) {
			defer wg.Done()
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{JobId: id, TaskType: "hash", Payload: "hello", TimeoutMs: 1000})
			if err != nil || resp == nil || resp.Status != "Succeeded" {
				t.Errorf("concurrent submission: response=%v error=%v", resp, err)
			}
			_, _ = s.Heartbeat(ctx, &runtimepb.HeartbeatRequest{WorkerId: "worker"})
		}(int64(i + 1))
	}
	wg.Wait()
	for _, name := range []string{"submitted", "started", "finished", "terminal", "execution"} {
		if got := observer.count(name, ""); got != jobs {
			t.Errorf("%s count=%d, want %d", name, got, jobs)
		}
	}
}

func TestMetricsAttemptFinishedOnceOnAbandonment(t *testing.T) {
	for _, tc := range []struct {
		name    string
		outcome metrics.AttemptOutcome
		starts  int
		code    codes.Code
	}{
		{"rpc", metrics.AttemptRPCError, 2, codes.Unavailable},
		{"lease", metrics.AttemptLeaseExpired, 2, codes.Unavailable},
		{"cancel", metrics.AttemptCancelled, 1, codes.Canceled},
		{"invalid status", metrics.AttemptInvalidStatus, 1, codes.Internal},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &recordingMetrics{}
			s := &CoordinatorServer{jobStore: NewMemoryJobStore(), metrics: observer}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if tc.name == "lease" {
				s.leaseDuration = 100 * time.Millisecond
			}
			dispatchDone := make(chan struct{}, 2)
			s.onDispatchDone = func(_, _ int64) { dispatchDone <- struct{}{} }
			registerTestWorker(t, s, "worker", func(attemptCtx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
				switch tc.name {
				case "lease":
					<-attemptCtx.Done()
					return nil, attemptCtx.Err()
				case "cancel":
					cancel()
					return nil, status.Error(codes.Unavailable, "worker unavailable")
				case "invalid status":
					resp := successfulResult(req)
					resp.Status = "unexpected"
					return resp, nil
				default:
					return nil, status.Error(codes.Unavailable, "injected RPC error")
				}
			})
			resp, err := s.SubmitJob(ctx, &runtimepb.SubmitJobRequest{
				JobId: 909, TaskType: "hash", Payload: "hello", TimeoutMs: 1000,
			})
			gotCode := status.Code(err)
			if errors.Is(err, context.Canceled) {
				gotCode = codes.Canceled
			}
			if resp != nil || gotCode != tc.code || observer.count("started", "") != tc.starts ||
				observer.count("finished", "") != tc.starts ||
				observer.count("finished", string(tc.outcome)) != tc.starts ||
				observer.count("terminal", "") != 0 {
				t.Fatalf("abandonment response=%v error=%v events=%+v", resp, err, observer.snapshot())
			}
			waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer waitCancel()
			for i := 0; i < tc.starts; i++ {
				select {
				case <-dispatchDone:
				case <-waitCtx.Done():
					t.Fatal("abandoned dispatch did not complete")
				}
			}
			if observer.count("finished", "") != tc.starts {
				t.Fatal("late dispatch doubled an attempt finish")
			}
		})
	}
}
