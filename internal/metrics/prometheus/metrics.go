// Package prometheus adapts semantic runtime events to Prometheus collectors.
package prometheus

import (
	"fmt"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prom "github.com/prometheus/client_golang/prometheus"
)

// Metrics must be constructed with New. Collectors and label schemas are fixed
// after construction; the Prometheus client synchronizes concurrent emissions.
type Metrics struct {
	jobsSubmitted     *prom.CounterVec
	jobsTerminal      *prom.CounterVec
	attemptsStarted   *prom.CounterVec
	attemptsFinished  *prom.CounterVec
	attemptsRejected  *prom.CounterVec
	workerTransitions *prom.CounterVec
	recoveryPasses    *prom.CounterVec
	recoveryChanges   *prom.CounterVec
	recoveryFailures  *prom.CounterVec
	repairFailures    *prom.CounterVec
	jobDuration       *prom.HistogramVec
	attemptDuration   *prom.HistogramVec
	recoveryDuration  *prom.HistogramVec
	provingDuration   prom.Histogram
	verifyDuration    prom.Histogram
	collectors        []prom.Collector
}

var _ metrics.Metrics = (*Metrics)(nil)
var _ prom.Collector = (*Metrics)(nil)

// New registers the backend as one collector, so registration failure cannot
// leave a partially registered backend. No default/global registry is used.
func New(registerer prom.Registerer) (*Metrics, error) {
	if registerer == nil {
		return nil, fmt.Errorf("Prometheus registerer is required")
	}
	counter := func(name, help string, labels ...string) *prom.CounterVec {
		return prom.NewCounterVec(prom.CounterOpts{Namespace: "dzkr", Name: name, Help: help}, labels)
	}
	// Milliseconds through one minute cover proof work, the 5-second lease,
	// and longer execution/recovery sessions. Prometheus adds the +Inf bucket.
	buckets := []float64{0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60}
	histogramOptions := func(name, help string) prom.HistogramOpts {
		return prom.HistogramOpts{Namespace: "dzkr", Name: name, Help: help, Buckets: buckets}
	}
	m := &Metrics{
		jobsSubmitted:     counter("jobs_submitted_total", "Accepted job submissions.", "source"),
		jobsTerminal:      counter("jobs_terminal_total", "Durably accepted terminal job results.", "source", "state"),
		attemptsStarted:   counter("attempts_started_total", "Durably published attempts.", "source"),
		attemptsFinished:  counter("attempts_finished_total", "Accepted or abandoned attempts.", "source", "outcome"),
		attemptsRejected:  counter("attempts_rejected_total", "Attempts rejected by fencing or proof validation.", "source", "reason"),
		workerTransitions: counter("worker_state_transitions_total", "Actual worker registry state changes.", "from", "to", "cause"),
		recoveryPasses:    counter("recovery_passes_total", "Completed or interrupted recovery scans.", "outcome"),
		recoveryChanges:   counter("recovery_transitions_total", "Durable Running to Recovering transitions.", "cause"),
		recoveryFailures:  counter("recovery_execution_failures_total", "Recovery execution failures.", "reason"),
		repairFailures:    counter("recovery_repair_failures_total", "Recovery repair persistence failures.", "stage"),
		jobDuration:       prom.NewHistogramVec(histogramOptions("job_execution_duration_seconds", "Execution session duration, excluding earlier processes."), []string{"source"}),
		attemptDuration:   prom.NewHistogramVec(histogramOptions("attempt_duration_seconds", "Duration until an attempt is accepted or abandoned."), []string{"source", "outcome"}),
		recoveryDuration:  prom.NewHistogramVec(histogramOptions("recovery_pass_duration_seconds", "Recovery scan duration."), []string{"outcome"}),
		provingDuration:   prom.NewHistogram(histogramOptions("zk_proving_duration_seconds", "Time spent in actual preimage proving calls.")),
		verifyDuration:    prom.NewHistogram(histogramOptions("zk_verification_duration_seconds", "Time spent in actual proof verification calls.")),
	}
	m.collectors = []prom.Collector{
		m.jobsSubmitted, m.jobsTerminal, m.attemptsStarted, m.attemptsFinished, m.attemptsRejected,
		m.workerTransitions, m.recoveryPasses, m.recoveryChanges, m.recoveryFailures, m.repairFailures,
		m.jobDuration, m.attemptDuration, m.recoveryDuration, m.provingDuration, m.verifyDuration,
	}
	if err := registerer.Register(m); err != nil {
		return nil, fmt.Errorf("register runtime metrics: %w", err)
	}
	return m, nil
}

func (m *Metrics) Describe(ch chan<- *prom.Desc) {
	for _, collector := range m.collectors {
		collector.Describe(ch)
	}
}

func (m *Metrics) Collect(ch chan<- prom.Metric) {
	for _, collector := range m.collectors {
		collector.Collect(ch)
	}
}

// Typed string enums can still be explicitly cast from arbitrary strings.
// Reject unknown values before creating any series to enforce bounded labels.
func oneOf[T comparable](value T, allowed ...T) bool {
	for _, candidate := range allowed {
		if value == candidate {
			return true
		}
	}
	return false
}

func validSource(source metrics.ExecutionSource) bool {
	return oneOf(source, metrics.SourceSubmit, metrics.SourceRecovery)
}

func (m *Metrics) JobSubmitted(source metrics.ExecutionSource) {
	if validSource(source) {
		m.jobsSubmitted.WithLabelValues(string(source)).Inc()
	}
}

func (m *Metrics) JobTerminal(source metrics.ExecutionSource, state metrics.TerminalState) {
	if validSource(source) && oneOf(state, metrics.TerminalSucceeded, metrics.TerminalFailed, metrics.TerminalCancelled) {
		m.jobsTerminal.WithLabelValues(string(source), string(state)).Inc()
	}
}

func (m *Metrics) ObserveJobExecution(source metrics.ExecutionSource, duration time.Duration) {
	if validSource(source) && duration >= 0 {
		m.jobDuration.WithLabelValues(string(source)).Observe(duration.Seconds())
	}
}

func (m *Metrics) AttemptStarted(source metrics.ExecutionSource) {
	if validSource(source) {
		m.attemptsStarted.WithLabelValues(string(source)).Inc()
	}
}

func (m *Metrics) AttemptFinished(source metrics.ExecutionSource, outcome metrics.AttemptOutcome, duration time.Duration) {
	if !validSource(source) || !oneOf(outcome, metrics.AttemptAccepted, metrics.AttemptRPCError,
		metrics.AttemptLeaseExpired, metrics.AttemptCancelled, metrics.AttemptFenced,
		metrics.AttemptProofRejected, metrics.AttemptTaskFailed, metrics.AttemptTaskCancelled,
		metrics.AttemptInvalidStatus, metrics.AttemptPersistenceError) {
		return
	}
	m.attemptsFinished.WithLabelValues(string(source), string(outcome)).Inc()
	if duration >= 0 {
		m.attemptDuration.WithLabelValues(string(source), string(outcome)).Observe(duration.Seconds())
	}
}

func (m *Metrics) AttemptRejected(source metrics.ExecutionSource, reason metrics.AttemptRejectReason) {
	if validSource(source) && oneOf(reason, metrics.RejectFencing, metrics.RejectProof) {
		m.attemptsRejected.WithLabelValues(string(source), string(reason)).Inc()
	}
}

func (m *Metrics) WorkerStateChanged(from, to metrics.WorkerState, cause metrics.WorkerStateChangeCause) {
	validState := func(state metrics.WorkerState) bool {
		return oneOf(state, metrics.WorkerUnregistered, metrics.WorkerAlive, metrics.WorkerDead)
	}
	if validState(from) && validState(to) &&
		oneOf(cause, metrics.WorkerRegistration, metrics.WorkerHeartbeat, metrics.WorkerTimeout) {
		m.workerTransitions.WithLabelValues(string(from), string(to), string(cause)).Inc()
	}
}

func (m *Metrics) RecoveryPass(outcome metrics.RecoveryPassOutcome, duration time.Duration) {
	if !oneOf(outcome, metrics.PassCompleted, metrics.PassCompletedWithErrors, metrics.PassCancelled,
		metrics.PassStoreError, metrics.PassSkipped) {
		return
	}
	m.recoveryPasses.WithLabelValues(string(outcome)).Inc()
	if duration >= 0 {
		m.recoveryDuration.WithLabelValues(string(outcome)).Observe(duration.Seconds())
	}
}

func (m *Metrics) RecoveryTransition(cause metrics.RecoveryTransitionCause) {
	if oneOf(cause, metrics.RecoveryStartup, metrics.RecoveryRepair) {
		m.recoveryChanges.WithLabelValues(string(cause)).Inc()
	}
}

func (m *Metrics) RecoveryExecutionFailed(reason metrics.RecoveryFailureReason) {
	if oneOf(reason, metrics.FailureCancelled, metrics.FailureDeadline, metrics.FailureUnavailable,
		metrics.FailureInternal, metrics.FailureOther) {
		m.recoveryFailures.WithLabelValues(string(reason)).Inc()
	}
}

func (m *Metrics) RecoveryRepairFailed(stage metrics.RecoveryRepairStage) {
	if oneOf(stage, metrics.RepairLoad, metrics.RepairSave) {
		m.repairFailures.WithLabelValues(string(stage)).Inc()
	}
}

func (m *Metrics) ObserveZKProving(duration time.Duration) {
	if duration >= 0 {
		m.provingDuration.Observe(duration.Seconds())
	}
}

func (m *Metrics) ObserveZKVerification(duration time.Duration) {
	if duration >= 0 {
		m.verifyDuration.Observe(duration.Seconds())
	}
}
