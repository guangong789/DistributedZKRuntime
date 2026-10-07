package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestResolveNoopSupportsConcurrentCalls(t *testing.T) {
	observer := Resolve(nil)
	if _, ok := observer.(NoopMetrics); !ok {
		t.Fatalf("nil metrics resolved to %T", observer)
	}
	if Resolve(observer) != observer {
		t.Fatal("Resolve replaced a configured observer")
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			observer.JobSubmitted(SourceSubmit)
			observer.JobTerminal(SourceRecovery, TerminalSucceeded)
			observer.ObserveJobExecution(SourceSubmit, time.Second)
			observer.AttemptStarted(SourceRecovery)
			observer.AttemptFinished(SourceRecovery, AttemptAccepted, time.Millisecond)
			observer.AttemptRejected(SourceSubmit, RejectFencing)
			observer.WorkerStateChanged(WorkerAlive, WorkerDead, WorkerTimeout)
			observer.RecoveryPass(PassCompleted, time.Second)
			observer.RecoveryTransition(RecoveryRepair)
			observer.RecoveryExecutionFailed(FailureUnavailable)
			observer.RecoveryRepairFailed(RepairSave)
			observer.ObserveZKProving(time.Millisecond)
			observer.ObserveZKVerification(time.Millisecond)
		}()
	}
	wg.Wait()
}
