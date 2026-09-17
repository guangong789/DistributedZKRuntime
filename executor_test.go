package main

import (
	"context"
	"errors"
	"testing"
	"time"
)

type blockingTask struct {
	started chan struct{}
	release chan struct{}
}

func (t blockingTask) Execute(ctx context.Context) (string, error) {
	close(t.started)

	select {
	case <-t.release:
		return "done", nil

	case <-ctx.Done():
		return "", ctx.Err()
	}
}

func TestExecutorCompletesJob(t *testing.T) {
	executor := NewExecutor(1, 4)

	executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})

	go executor.Shutdown()

	result := <-executor.Results()

	if result.JobID != 1 {
		t.Fatalf("expected job ID 1, got %d", result.JobID)
	}

	if result.Status != JobSucceeded {
		t.Fatalf(
			"expected status %d, got %d",
			JobSucceeded,
			result.Status,
		)
	}
}

func TestExecutorJobTimeout(t *testing.T) {
	executor := NewExecutor(1, 4)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 50 * time.Millisecond,
		Task: SleepTask{
			Duration: 500 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	go executor.Shutdown()

	result := <-executor.Results()

	if result.Status != JobCancelled {
		t.Fatalf(
			"expected cancelled status, got %d",
			result.Status,
		)
	}

	if !errors.Is(result.Err, context.DeadlineExceeded) {
		t.Fatalf(
			"expected deadline exceeded, got %v",
			result.Err,
		)
	}
}

func TestExecutorShutdownCompletes(t *testing.T) {
	executor := NewExecutor(2, 4)

	executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})

	done := make(chan struct{})

	go func() {
		executor.Shutdown()
		close(done)
	}()

	go func() {
		for range executor.Results() {
		}
	}()

	select {
	case <-done:
		// success

	case <-time.After(5 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

func TestExecutorDoubleShutdown(t *testing.T) {
	executor := NewExecutor(1, 1)

	executor.Shutdown()
	executor.Shutdown()
}

func TestExecutorSubmitAfterShutdown(t *testing.T) {
	executor := NewExecutor(1, 1)

	executor.Shutdown()

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})

	if err == nil {
		t.Fatal("expected submit error after shutdown")
	}
}

func TestBlockedSubmitUnblocksOnShutdown(t *testing.T) {
	executor := NewExecutor(1, 1)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = executor.Submit(Job{
		ID:      2,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	submitDone := make(chan error, 1)

	go func() {
		err := executor.Submit(Job{
			ID:      3,
			Timeout: 5 * time.Second,
			Task: SleepTask{
				Duration: 100 * time.Millisecond,
			},
		})

		submitDone <- err
	}()

	// 必须消费 results，否则 worker 会卡在发送结果。
	go func() {
		for range executor.Results() {
		}
	}()

	go executor.Shutdown()

	select {
	case err := <-submitDone:
		if err != ErrExecutorNotRunning {
			t.Fatalf(
				"expected ErrExecutorNotRunning, got %v",
				err,
			)
		}

	case <-time.After(1 * time.Second):
		t.Fatal("blocked Submit did not unblock after shutdown")
	}
}

func TestConcurrentShutdownWaitsForCompletion(t *testing.T) {
	executor := NewExecutor(1, 1)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		for range executor.Results() {
		}
	}()

	done1 := make(chan struct{})
	done2 := make(chan struct{})

	go func() {
		executor.Shutdown()
		close(done1)
	}()

	go func() {
		executor.Shutdown()
		close(done2)
	}()

	select {
	case <-done1:
	case <-time.After(5 * time.Second):
		t.Fatal("first shutdown timed out")
	}

	select {
	case <-done2:
	case <-time.After(5 * time.Second):
		t.Fatal("second shutdown timed out")
	}
}

func TestExecutorTaskFailure(t *testing.T) {
	executor := NewExecutor(1, 4)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: time.Second,
		Task: HashTask{
			Input: "",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	go executor.Shutdown()

	result := <-executor.Results()

	if result.Status != JobFailed {
		t.Fatalf(
			"expected failed status, got %d",
			result.Status,
		)
	}

	if result.Err == nil {
		t.Fatal("expected task error, got nil")
	}
}

func TestInvalidJobTransitionRejected(t *testing.T) {
	executor := NewExecutor(1, 1)

	executor.jobRegistry[1] = Job{
		ID:     1,
		Status: JobSucceeded,
	}

	err := executor.updateJobStatus(
		1,
		JobRunning,
	)

	if !errors.Is(err, ErrInvalidJobTransition) {
		t.Fatalf(
			"expected invalid transition error, got %v",
			err,
		)
	}

	job, ok := executor.GetJob(1)
	if !ok {
		t.Fatal("expected job to exist")
	}

	if job.Status != JobSucceeded {
		t.Fatalf(
			"expected status to remain Succeeded, got %v",
			job.Status,
		)
	}
}

func TestExecutorRejectsDuplicateJobID(t *testing.T) {
	executor := NewExecutor(1, 4)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = executor.Submit(Job{
		ID:      1,
		Timeout: time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})

	if !errors.Is(err, ErrDuplicateJobID) {
		t.Fatalf(
			"expected duplicate job ID error, got %v",
			err,
		)
	}

	go func() {
		for range executor.Results() {
		}
	}()

	executor.Shutdown()
}

func TestRejectedSubmitRemovedFromRegistry(t *testing.T) {
	executor := NewExecutor(1, 1)

	started := make(chan struct{})
	release := make(chan struct{})

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 5 * time.Second,
		Task: blockingTask{
			started: started,
			release: release,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// 确保 worker 已经被 Job 1 占住。
	<-started

	// queue capacity = 1，所以 Job 2 会填满 queue。
	err = executor.Submit(Job{
		ID:      2,
		Timeout: time.Second,
		Task: SleepTask{
			Duration: 10 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	submitDone := make(chan error, 1)

	// Job 3 现在会因为 queue 满而阻塞。
	go func() {
		submitDone <- executor.Submit(Job{
			ID:      3,
			Timeout: time.Second,
			Task: SleepTask{
				Duration: 10 * time.Millisecond,
			},
		})
	}()

	// 消费结果，防止 worker 后面卡在 results send。
	go func() {
		for range executor.Results() {
		}
	}()

	shutdownDone := make(chan struct{})

	go func() {
		executor.Shutdown()
		close(shutdownDone)
	}()

	err = <-submitDone

	if !errors.Is(err, ErrExecutorNotRunning) {
		t.Fatalf(
			"expected executor not running error, got %v",
			err,
		)
	}

	if _, ok := executor.GetJob(3); ok {
		t.Fatal("rejected job remained in registry")
	}

	// 让 Job 1 真正结束。
	close(release)

	select {
	case <-shutdownDone:
	case <-time.After(2 * time.Second):
		t.Fatal("shutdown timed out")
	}
}

func TestConcurrentDuplicateJobID(t *testing.T) {
	executor := NewExecutor(1, 4)

	start := make(chan struct{})

	results := make(chan error, 2)

	submit := func() {
		<-start

		results <- executor.Submit(Job{
			ID:      42,
			Timeout: time.Second,
			Task: SleepTask{
				Duration: 100 * time.Millisecond,
			},
		})
	}

	go submit()
	go submit()

	close(start)

	err1 := <-results
	err2 := <-results

	successCount := 0
	duplicateCount := 0

	for _, err := range []error{err1, err2} {
		switch {
		case err == nil:
			successCount++

		case errors.Is(err, ErrDuplicateJobID):
			duplicateCount++

		default:
			t.Fatalf("unexpected submit error: %v", err)
		}
	}

	if successCount != 1 {
		t.Fatalf(
			"expected exactly one successful submit, got %d",
			successCount,
		)
	}

	if duplicateCount != 1 {
		t.Fatalf(
			"expected exactly one duplicate rejection, got %d",
			duplicateCount,
		)
	}

	go func() {
		for range executor.Results() {
		}
	}()

	executor.Shutdown()
}

func TestWorkerReportsStateErrorAndContinues(t *testing.T) {
	executor := NewExecutor(1, 4)

	executor.jobs <- Job{
		ID:      1,
		Timeout: time.Second,
		Task: SleepTask{
			Duration: 50 * time.Millisecond,
		},
		Status: JobPending,
	}

	err := executor.Submit(Job{
		ID:      2,
		Timeout: time.Second,
		Task: SleepTask{
			Duration: 50 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	go executor.Shutdown()

	result1 := <-executor.Results()
	result2 := <-executor.Results()

	results := map[int]Result{
		result1.JobID: result1,
		result2.JobID: result2,
	}

	job1Result := results[1]

	if job1Result.Status != JobFailed {
		t.Fatalf(
			"expected job 1 failed, got %v",
			job1Result.Status,
		)
	}

	if !errors.Is(job1Result.Err, ErrJobNotFound) {
		t.Fatalf(
			"expected ErrJobNotFound, got %v",
			job1Result.Err,
		)
	}

	job2Result := results[2]

	if job2Result.Status != JobSucceeded {
		t.Fatalf(
			"expected job 2 succeeded, got %v",
			job2Result.Status,
		)
	}
}

func TestGracefulShutdownLetsRunningJobFinish(t *testing.T) {
	executor := NewExecutor(1, 1)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: time.Second,
		Task: SleepTask{
			Duration: 100 * time.Millisecond,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	shutdownDone := make(chan struct{})

	go func() {
		executor.Shutdown()
		close(shutdownDone)
	}()

	result := <-executor.Results()

	if result.Status != JobSucceeded {
		t.Fatalf(
			"expected graceful shutdown to let job succeed, got %v",
			result.Status,
		)
	}

	<-shutdownDone
}

func TestForceShutdownCancelsRunningJob(t *testing.T) {
	executor := NewExecutor(1, 1)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 10 * time.Second,
		Task: SleepTask{
			Duration: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	resultCh := make(chan Result, 1)

	go func() {
		result, ok := <-executor.Results()
		if ok {
			resultCh <- result
		}
	}()

	time.Sleep(50 * time.Millisecond)

	done := make(chan struct{})

	go func() {
		executor.ForceShutdown()
		close(done)
	}()

	select {
	case result := <-resultCh:
		if result.Status != JobCancelled {
			t.Fatalf(
				"expected cancelled status, got %v",
				result.Status,
			)
		}

		if !errors.Is(result.Err, context.Canceled) {
			t.Fatalf(
				"expected context.Canceled, got %v",
				result.Err,
			)
		}

	case <-time.After(time.Second):
		t.Fatal("expected cancelled result")
	}

	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("force shutdown timed out")
	}
}

func TestExecutorRunsJobsConcurrently(t *testing.T) {
	executor := NewExecutor(2, 4)

	start := time.Now()

	for i := 1; i <= 2; i++ {
		err := executor.Submit(Job{
			ID:      i,
			Timeout: time.Second,
			Task: SleepTask{
				Duration: 200 * time.Millisecond,
			},
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	go executor.Shutdown()

	for range executor.Results() {
	}

	elapsed := time.Since(start)

	if elapsed >= 350*time.Millisecond {
		t.Fatalf(
			"expected jobs to run concurrently, took %v",
			elapsed,
		)
	}
}

func TestForceShutdownCancelsPendingJobs(t *testing.T) {
	executor := NewExecutor(1, 4)

	err := executor.Submit(Job{
		ID:      1,
		Timeout: 10 * time.Second,
		Task: SleepTask{
			Duration: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	err = executor.Submit(Job{
		ID:      2,
		Timeout: 10 * time.Second,
		Task: SleepTask{
			Duration: 5 * time.Second,
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Give worker 1 a chance to start Job 1.
	time.Sleep(50 * time.Millisecond)

	executor.ForceShutdown()

	job2, ok := executor.GetJob(2)
	if !ok {
		t.Fatal("expected job 2 to exist")
	}

	if job2.Status != JobCancelled {
		t.Fatalf(
			"expected pending job to be cancelled, got %v",
			job2.Status,
		)
	}
}