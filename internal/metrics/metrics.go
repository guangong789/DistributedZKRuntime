// Package metrics defines backend-independent runtime observations.
package metrics

import "time"

type ExecutionSource string

const (
	SourceSubmit   ExecutionSource = "submit"
	SourceRecovery ExecutionSource = "recovery"
)

type TerminalState string

const (
	TerminalSucceeded TerminalState = "succeeded"
	TerminalFailed    TerminalState = "failed"
	TerminalCancelled TerminalState = "cancelled"
)

type AttemptOutcome string

const (
	AttemptAccepted         AttemptOutcome = "accepted"
	AttemptRPCError         AttemptOutcome = "rpc_error"
	AttemptLeaseExpired     AttemptOutcome = "lease_expired"
	AttemptCancelled        AttemptOutcome = "cancelled"
	AttemptFenced           AttemptOutcome = "fenced"
	AttemptProofRejected    AttemptOutcome = "proof_rejected"
	AttemptTaskFailed       AttemptOutcome = "task_failed"
	AttemptTaskCancelled    AttemptOutcome = "task_cancelled"
	AttemptInvalidStatus    AttemptOutcome = "invalid_status"
	AttemptPersistenceError AttemptOutcome = "persistence_error"
)

type AttemptRejectReason string

const (
	RejectFencing AttemptRejectReason = "fencing"
	RejectProof   AttemptRejectReason = "proof"
)

type WorkerState string

const (
	WorkerUnregistered WorkerState = "unregistered"
	WorkerAlive        WorkerState = "alive"
	WorkerDead         WorkerState = "dead"
)

type WorkerStateChangeCause string

const (
	WorkerRegistration WorkerStateChangeCause = "registration"
	WorkerHeartbeat    WorkerStateChangeCause = "heartbeat"
	WorkerTimeout      WorkerStateChangeCause = "heartbeat_timeout"
)

type RecoveryPassOutcome string

const (
	PassCompleted           RecoveryPassOutcome = "completed"
	PassCompletedWithErrors RecoveryPassOutcome = "completed_with_errors"
	PassCancelled           RecoveryPassOutcome = "cancelled"
	PassStoreError          RecoveryPassOutcome = "store_error"
	PassSkipped             RecoveryPassOutcome = "skipped"
)

type RecoveryTransitionCause string

const (
	RecoveryStartup RecoveryTransitionCause = "startup"
	RecoveryRepair  RecoveryTransitionCause = "execution_failure"
)

type RecoveryFailureReason string

const (
	FailureCancelled   RecoveryFailureReason = "cancelled"
	FailureDeadline    RecoveryFailureReason = "deadline"
	FailureUnavailable RecoveryFailureReason = "unavailable"
	FailureInternal    RecoveryFailureReason = "internal"
	FailureOther       RecoveryFailureReason = "other"
)

type RecoveryRepairStage string

const (
	RepairLoad RecoveryRepairStage = "load"
	RepairSave RecoveryRepairStage = "save"
)

// Metrics may be called concurrently. Implementations must be concurrency-safe,
// fast, non-blocking, and must not panic. Callbacks have no error return and must
// not influence runtime decisions. Arguments deliberately exclude identities,
// task payloads, witnesses, proofs, and raw error messages.
//
// AttemptFinished includes the attempt duration; RecoveryPass includes the pass
// duration. A timeout is represented by WorkerStateChanged with WorkerTimeout,
// so a backend can derive both a transition and a timeout count from one event.
type Metrics interface {
	JobSubmitted(ExecutionSource)
	JobTerminal(ExecutionSource, TerminalState)
	ObserveJobExecution(ExecutionSource, time.Duration)
	AttemptStarted(ExecutionSource)
	AttemptFinished(ExecutionSource, AttemptOutcome, time.Duration)
	AttemptRejected(ExecutionSource, AttemptRejectReason)
	WorkerStateChanged(WorkerState, WorkerState, WorkerStateChangeCause)
	RecoveryPass(RecoveryPassOutcome, time.Duration)
	RecoveryTransition(RecoveryTransitionCause)
	RecoveryExecutionFailed(RecoveryFailureReason)
	RecoveryRepairFailed(RecoveryRepairStage)
	ObserveZKProving(time.Duration)
	ObserveZKVerification(time.Duration)
}

type NoopMetrics struct{}

func (NoopMetrics) JobSubmitted(ExecutionSource)                                   {}
func (NoopMetrics) JobTerminal(ExecutionSource, TerminalState)                     {}
func (NoopMetrics) ObserveJobExecution(ExecutionSource, time.Duration)             {}
func (NoopMetrics) AttemptStarted(ExecutionSource)                                 {}
func (NoopMetrics) AttemptFinished(ExecutionSource, AttemptOutcome, time.Duration) {}
func (NoopMetrics) AttemptRejected(ExecutionSource, AttemptRejectReason)           {}
func (NoopMetrics) WorkerStateChanged(WorkerState, WorkerState, WorkerStateChangeCause) {
}
func (NoopMetrics) RecoveryPass(RecoveryPassOutcome, time.Duration) {}
func (NoopMetrics) RecoveryTransition(RecoveryTransitionCause)      {}
func (NoopMetrics) RecoveryExecutionFailed(RecoveryFailureReason)   {}
func (NoopMetrics) RecoveryRepairFailed(RecoveryRepairStage)        {}
func (NoopMetrics) ObserveZKProving(time.Duration)                  {}
func (NoopMetrics) ObserveZKVerification(time.Duration)             {}

// Resolve supplies the default without mutating shared runtime configuration.
// Configure a non-nil implementation before serving; a typed nil is not a valid
// backend implementation.
func Resolve(observer Metrics) Metrics {
	if observer == nil {
		return NoopMetrics{}
	}
	return observer
}
