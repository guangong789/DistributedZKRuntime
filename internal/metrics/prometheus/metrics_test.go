package prometheus

import (
	"errors"
	"fmt"
	"math"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
)

type sample struct {
	counter float64
	count   uint64
	sum     float64
	buckets []float64
}

func readSample(t *testing.T, registry *prom.Registry, name string, labels map[string]string) sample {
	t.Helper()
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if family.GetName() != name {
			continue
		}
		for _, metric := range family.GetMetric() {
			match := len(metric.GetLabel()) == len(labels)
			for _, label := range metric.GetLabel() {
				want, ok := labels[label.GetName()]
				match = match && ok && want == label.GetValue()
			}
			if !match {
				continue
			}
			result := sample{}
			if counter := metric.GetCounter(); counter != nil {
				result.counter = counter.GetValue()
			}
			if histogram := metric.GetHistogram(); histogram != nil {
				result.count, result.sum = histogram.GetSampleCount(), histogram.GetSampleSum()
				for _, bucket := range histogram.GetBucket() {
					result.buckets = append(result.buckets, bucket.GetUpperBound())
				}
			}
			return result
		}
	}
	t.Fatalf("missing metric %s with labels %v", name, labels)
	return sample{}
}

func newTestBackend(t *testing.T) (*Metrics, *prom.Registry) {
	t.Helper()
	registry := prom.NewPedanticRegistry()
	backend, err := New(registry)
	if err != nil {
		t.Fatal(err)
	}
	return backend, registry
}

func TestSemanticCountersAndLabels(t *testing.T) {
	backend, registry := newTestBackend(t)
	cases := []struct {
		name   string
		labels map[string]string
		emit   func()
	}{
		{"dzkr_jobs_submitted_total", map[string]string{"source": "submit"},
			func() { backend.JobSubmitted(metrics.SourceSubmit) }},
		{"dzkr_jobs_terminal_total", map[string]string{"source": "submit", "state": "succeeded"},
			func() { backend.JobTerminal(metrics.SourceSubmit, metrics.TerminalSucceeded) }},
		{"dzkr_jobs_terminal_total", map[string]string{"source": "recovery", "state": "failed"},
			func() { backend.JobTerminal(metrics.SourceRecovery, metrics.TerminalFailed) }},
		{"dzkr_jobs_terminal_total", map[string]string{"source": "recovery", "state": "cancelled"},
			func() { backend.JobTerminal(metrics.SourceRecovery, metrics.TerminalCancelled) }},
		{"dzkr_attempts_started_total", map[string]string{"source": "recovery"},
			func() { backend.AttemptStarted(metrics.SourceRecovery) }},
		{"dzkr_attempts_finished_total", map[string]string{"source": "submit", "outcome": "rpc_error"},
			func() { backend.AttemptFinished(metrics.SourceSubmit, metrics.AttemptRPCError, time.Millisecond) }},
		{"dzkr_attempts_rejected_total", map[string]string{"source": "submit", "reason": "fencing"},
			func() { backend.AttemptRejected(metrics.SourceSubmit, metrics.RejectFencing) }},
		{"dzkr_attempts_rejected_total", map[string]string{"source": "submit", "reason": "proof"},
			func() { backend.AttemptRejected(metrics.SourceSubmit, metrics.RejectProof) }},
		{"dzkr_worker_state_transitions_total", map[string]string{"from": "alive", "to": "dead", "cause": "heartbeat_timeout"},
			func() { backend.WorkerStateChanged(metrics.WorkerAlive, metrics.WorkerDead, metrics.WorkerTimeout) }},
		{"dzkr_worker_state_transitions_total", map[string]string{"from": "dead", "to": "alive", "cause": "heartbeat"},
			func() { backend.WorkerStateChanged(metrics.WorkerDead, metrics.WorkerAlive, metrics.WorkerHeartbeat) }},
		{"dzkr_worker_state_transitions_total", map[string]string{"from": "unregistered", "to": "alive", "cause": "registration"},
			func() {
				backend.WorkerStateChanged(metrics.WorkerUnregistered, metrics.WorkerAlive, metrics.WorkerRegistration)
			}},
		{"dzkr_recovery_passes_total", map[string]string{"outcome": "completed_with_errors"},
			func() { backend.RecoveryPass(metrics.PassCompletedWithErrors, time.Millisecond) }},
		{"dzkr_recovery_transitions_total", map[string]string{"cause": "startup"},
			func() { backend.RecoveryTransition(metrics.RecoveryStartup) }},
		{"dzkr_recovery_transitions_total", map[string]string{"cause": "execution_failure"},
			func() { backend.RecoveryTransition(metrics.RecoveryRepair) }},
		{"dzkr_recovery_execution_failures_total", map[string]string{"reason": "unavailable"},
			func() { backend.RecoveryExecutionFailed(metrics.FailureUnavailable) }},
		{"dzkr_recovery_repair_failures_total", map[string]string{"stage": "load"},
			func() { backend.RecoveryRepairFailed(metrics.RepairLoad) }},
		{"dzkr_recovery_repair_failures_total", map[string]string{"stage": "save"},
			func() { backend.RecoveryRepairFailed(metrics.RepairSave) }},
	}
	for _, tc := range cases {
		t.Run(tc.name+"/"+fmt.Sprint(tc.labels), func(t *testing.T) {
			tc.emit()
			tc.emit()
			if got := readSample(t, registry, tc.name, tc.labels).counter; got != 2 {
				t.Fatalf("counter=%v, want 2", got)
			}
		})
	}
}

func TestDurationHistogramsUseSecondsAndBuckets(t *testing.T) {
	backend, registry := newTestBackend(t)
	cases := []struct {
		name   string
		labels map[string]string
		emit   func(time.Duration)
	}{
		{"dzkr_job_execution_duration_seconds", map[string]string{"source": "submit"},
			func(d time.Duration) { backend.ObserveJobExecution(metrics.SourceSubmit, d) }},
		{"dzkr_attempt_duration_seconds", map[string]string{"source": "recovery", "outcome": "accepted"},
			func(d time.Duration) { backend.AttemptFinished(metrics.SourceRecovery, metrics.AttemptAccepted, d) }},
		{"dzkr_recovery_pass_duration_seconds", map[string]string{"outcome": "completed"},
			func(d time.Duration) { backend.RecoveryPass(metrics.PassCompleted, d) }},
		{"dzkr_zk_proving_duration_seconds", nil, backend.ObserveZKProving},
		{"dzkr_zk_verification_duration_seconds", nil, backend.ObserveZKVerification},
	}
	for _, tc := range cases {
		tc.emit(250 * time.Millisecond)
		tc.emit(750 * time.Millisecond)
		got := readSample(t, registry, tc.name, tc.labels)
		if got.count != 2 || got.sum != 1 {
			t.Errorf("%s: count=%d sum=%v, want 2 and 1 second", tc.name, got.count, got.sum)
		}
		wantBuckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
		if !reflect.DeepEqual(got.buckets, wantBuckets) {
			t.Errorf("%s: buckets=%v, want %v", tc.name, got.buckets, wantBuckets)
		}
	}
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	for _, family := range families {
		if kind := family.GetType().String(); kind != "COUNTER" && kind != "HISTOGRAM" {
			t.Errorf("unexpected family type %s for %s", kind, family.GetName())
		}
	}
}

func TestIndependentRegistriesAndDuplicateRegistrationError(t *testing.T) {
	first, registryA := newTestBackend(t)
	second, registryB := newTestBackend(t)
	first.JobSubmitted(metrics.SourceSubmit)
	second.JobSubmitted(metrics.SourceSubmit)
	second.JobSubmitted(metrics.SourceSubmit)
	if readSample(t, registryA, "dzkr_jobs_submitted_total", map[string]string{"source": "submit"}).counter != 1 ||
		readSample(t, registryB, "dzkr_jobs_submitted_total", map[string]string{"source": "submit"}).counter != 2 {
		t.Fatal("independent backends shared counters")
	}
	duplicate, err := New(registryA)
	var alreadyRegistered prom.AlreadyRegisteredError
	if duplicate != nil || !errors.As(err, &alreadyRegistered) {
		t.Fatalf("duplicate registration: backend=%v error=%v", duplicate, err)
	}
	first.JobSubmitted(metrics.SourceSubmit)
	if readSample(t, registryA, "dzkr_jobs_submitted_total", map[string]string{"source": "submit"}).counter != 2 {
		t.Fatal("duplicate registration altered the original backend")
	}
	if backend, err := New(nil); err == nil || backend != nil {
		t.Fatal("constructor silently used the default registerer")
	}
}

func TestRegistrationConflictDoesNotLeavePartialBackend(t *testing.T) {
	registry := prom.NewRegistry()
	conflict := prom.NewCounter(prom.CounterOpts{Name: "dzkr_jobs_submitted_total", Help: "Conflicting collector."})
	if err := registry.Register(conflict); err != nil {
		t.Fatal(err)
	}
	if backend, err := New(registry); backend != nil || err == nil {
		t.Fatal("expected conflicting registration error")
	}
	families, err := registry.Gather()
	if err != nil || len(families) != 1 || families[0].GetName() != "dzkr_jobs_submitted_total" {
		t.Fatalf("partial registration: families=%v error=%v", families, err)
	}
}

func TestInvalidEnumsCannotCreateUnboundedLabels(t *testing.T) {
	backend, registry := newTestBackend(t)
	bad := "private-unbounded-value"
	backend.JobSubmitted(metrics.ExecutionSource(bad))
	backend.JobTerminal(metrics.SourceSubmit, metrics.TerminalState(bad))
	backend.ObserveJobExecution(metrics.ExecutionSource(bad), time.Second)
	backend.AttemptStarted(metrics.ExecutionSource(bad))
	backend.AttemptFinished(metrics.SourceSubmit, metrics.AttemptOutcome(bad), time.Second)
	backend.AttemptRejected(metrics.SourceSubmit, metrics.AttemptRejectReason(bad))
	backend.WorkerStateChanged(metrics.WorkerState(bad), metrics.WorkerAlive, metrics.WorkerRegistration)
	backend.WorkerStateChanged(metrics.WorkerDead, metrics.WorkerState(bad), metrics.WorkerHeartbeat)
	backend.WorkerStateChanged(metrics.WorkerAlive, metrics.WorkerDead, metrics.WorkerStateChangeCause(bad))
	backend.RecoveryPass(metrics.RecoveryPassOutcome(bad), time.Second)
	backend.RecoveryTransition(metrics.RecoveryTransitionCause(bad))
	backend.RecoveryExecutionFailed(metrics.RecoveryFailureReason(bad))
	backend.RecoveryRepairFailed(metrics.RecoveryRepairStage(bad))
	backend.ObserveJobExecution(metrics.SourceSubmit, -time.Second)
	backend.ObserveZKProving(-time.Second)
	backend.ObserveZKVerification(-time.Second)
	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	// Only the two unlabeled histograms exist before any valid observations.
	if len(families) != 2 {
		t.Fatalf("invalid input created metric series: %v", families)
	}
	for _, family := range families {
		for _, metric := range family.GetMetric() {
			if len(metric.GetLabel()) != 0 || metric.GetHistogram().GetSampleCount() != 0 {
				t.Fatalf("invalid label/duration escaped validation: %v", metric)
			}
		}
	}
}

func TestConcurrentEmissionAndGather(t *testing.T) {
	backend, registry := newTestBackend(t)
	const goroutines, calls = 16, 80
	var wg sync.WaitGroup
	for i := 0; i < goroutines; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < calls; j++ {
				backend.JobSubmitted(metrics.SourceSubmit)
				backend.JobTerminal(metrics.SourceSubmit, metrics.TerminalSucceeded)
				backend.ObserveJobExecution(metrics.SourceSubmit, time.Millisecond)
				backend.AttemptStarted(metrics.SourceSubmit)
				backend.AttemptFinished(metrics.SourceSubmit, metrics.AttemptAccepted, time.Millisecond)
				backend.AttemptRejected(metrics.SourceSubmit, metrics.RejectFencing)
				backend.WorkerStateChanged(metrics.WorkerAlive, metrics.WorkerDead, metrics.WorkerTimeout)
				backend.RecoveryPass(metrics.PassCompleted, time.Millisecond)
				backend.RecoveryTransition(metrics.RecoveryRepair)
				backend.RecoveryExecutionFailed(metrics.FailureUnavailable)
				backend.RecoveryRepairFailed(metrics.RepairSave)
				backend.ObserveZKProving(time.Millisecond)
				backend.ObserveZKVerification(time.Millisecond)
			}
		}()
	}
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 50; i++ {
			if _, err := registry.Gather(); err != nil {
				t.Errorf("concurrent Gather: %v", err)
			}
		}
	}()
	wg.Wait()
	want := float64(goroutines * calls)
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{"dzkr_jobs_submitted_total", map[string]string{"source": "submit"}},
		{"dzkr_jobs_terminal_total", map[string]string{"source": "submit", "state": "succeeded"}},
		{"dzkr_attempts_started_total", map[string]string{"source": "submit"}},
		{"dzkr_attempts_finished_total", map[string]string{"source": "submit", "outcome": "accepted"}},
		{"dzkr_attempts_rejected_total", map[string]string{"source": "submit", "reason": "fencing"}},
		{"dzkr_worker_state_transitions_total", map[string]string{"from": "alive", "to": "dead", "cause": "heartbeat_timeout"}},
		{"dzkr_recovery_passes_total", map[string]string{"outcome": "completed"}},
		{"dzkr_recovery_transitions_total", map[string]string{"cause": "execution_failure"}},
		{"dzkr_recovery_execution_failures_total", map[string]string{"reason": "unavailable"}},
		{"dzkr_recovery_repair_failures_total", map[string]string{"stage": "save"}},
	} {
		if got := readSample(t, registry, tc.name, tc.labels).counter; got != want {
			t.Errorf("%s=%v, want %v", tc.name, got, want)
		}
	}
	for _, tc := range []struct {
		name   string
		labels map[string]string
	}{
		{"dzkr_job_execution_duration_seconds", map[string]string{"source": "submit"}},
		{"dzkr_attempt_duration_seconds", map[string]string{"source": "submit", "outcome": "accepted"}},
		{"dzkr_recovery_pass_duration_seconds", map[string]string{"outcome": "completed"}},
		{"dzkr_zk_proving_duration_seconds", nil},
		{"dzkr_zk_verification_duration_seconds", nil},
	} {
		got := readSample(t, registry, tc.name, tc.labels)
		if got.count != uint64(want) || math.Abs(got.sum-want/1000) > 1e-9 {
			t.Errorf("%s: count=%d sum=%v, want %v and %v", tc.name, got.count, got.sum, want, want/1000)
		}
	}
}
