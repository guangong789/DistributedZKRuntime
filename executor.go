package main

import (
	"context"
	"errors"
	"sync"
)

type Executor struct {
	jobs    chan Job
	results chan Result

	ctx    context.Context
	cancel context.CancelFunc

	wg       sync.WaitGroup
	submitWG sync.WaitGroup

	mu    sync.Mutex
	state ExecutorState

	jobMu       sync.Mutex
	jobRegistry map[int]Job

	stopCh chan struct{}
	doneCh chan struct{}
}

func NewExecutor(workerCount int, queueSize int) *Executor {
	ctx, cancel := context.WithCancel(context.Background())

	e := &Executor{
		jobs:    make(chan Job, queueSize),
		results: make(chan Result),

		ctx:    ctx,
		cancel: cancel,

		state: ExecutorRunning,

		jobRegistry: make(map[int]Job),

		stopCh: make(chan struct{}),
		doneCh: make(chan struct{}),
	}

	for i := 1; i <= workerCount; i++ {
		e.wg.Add(1)

		go e.worker(i)
	}

	return e
}

func executeJob(ctx context.Context, job Job) Result {
	output, err := job.Task.Execute(ctx)

	if err != nil {
		status := JobFailed

		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			status = JobCancelled
		}

		return Result{
			JobID:  job.ID,
			Status: status,
			Output: "",
			Err:    err,
		}
	}

	return Result{
		JobID:  job.ID,
		Status: JobSucceeded,
		Output: output,
		Err:    nil,
	}
}

func (e *Executor) updateJobStatus(jobID int, newStatus JobStatus) error {
	e.jobMu.Lock()
	defer e.jobMu.Unlock()

	job, ok := e.jobRegistry[jobID]
	if !ok {
		return ErrJobNotFound
	}

	if !validTransition(job.Status, newStatus) {
		return ErrInvalidJobTransition
	}

	job.Status = newStatus
	e.jobRegistry[jobID] = job

	return nil
}

func (e *Executor) GetJob(jobID int) (Job, bool) {
	e.jobMu.Lock()
	defer e.jobMu.Unlock()

	job, ok := e.jobRegistry[jobID]
	return job, ok
}

func failedResult(jobID int, err error) Result {
	return Result{
		JobID:  jobID,
		Status: JobFailed,
		Output: "",
		Err:    err,
	}
}

func (e *Executor) sendResult(result Result) bool {
	select {
	case e.results <- result:
		return true
	default:
	}

	select {
	case e.results <- result:
		return true

	case <-e.ctx.Done():
		return false
	}
}

func (e *Executor) worker(id int) {
	defer e.wg.Done()

	for {
		select {
		case job, ok := <-e.jobs:
			if !ok {
				return
			}

			if err := e.updateJobStatus(job.ID, JobRunning); err != nil {
				if !e.sendResult(failedResult(job.ID, err)) {
					return
				}

				continue
			}

			jobCtx, cancel := context.WithTimeout(e.ctx, job.Timeout)
			result := executeJob(jobCtx, job)
			cancel()

			if err := e.updateJobStatus(job.ID, result.Status); err != nil {
				if !e.sendResult(failedResult(job.ID, err)) {
					return
				}

				continue
			}

			if !e.sendResult(result) {
				return
			}

		case <-e.ctx.Done():
			return
		}
	}
}

func (e *Executor) Submit(job Job) error {
	if job.Task == nil {
		return ErrInvalidJob
	}

	if job.Timeout <= 0 {
		return ErrInvalidJob
	}

	e.mu.Lock()

	if e.state != ExecutorRunning {
		e.mu.Unlock()
		return ErrExecutorNotRunning
	}

	e.submitWG.Add(1)
	e.mu.Unlock()

	defer e.submitWG.Done()

	job.Status = JobPending

	e.jobMu.Lock()

	if _, exists := e.jobRegistry[job.ID]; exists {
		e.jobMu.Unlock()
		return ErrDuplicateJobID
	}

	e.jobRegistry[job.ID] = job
	e.jobMu.Unlock()

	select {
	case e.jobs <- job:
		return nil

	case <-e.stopCh:
		e.jobMu.Lock()
		delete(e.jobRegistry, job.ID)
		e.jobMu.Unlock()

		return ErrExecutorNotRunning
	}
}

func (e *Executor) Results() <-chan Result {
	return e.results
}

func (e *Executor) cancelPendingJobs() {
	e.jobMu.Lock()
	defer e.jobMu.Unlock()

	for id, job := range e.jobRegistry {
		if job.Status != JobPending {
			continue
		}

		job.Status = JobCancelled
		e.jobRegistry[id] = job
	}
}

func (e *Executor) Shutdown() {
	e.mu.Lock()

	if e.state != ExecutorRunning {
		doneCh := e.doneCh
		e.mu.Unlock()

		<-doneCh
		return
	}

	e.state = ExecutorShuttingDown
	close(e.stopCh)

	e.mu.Unlock()

	e.submitWG.Wait()
	close(e.jobs)

	e.wg.Wait()
	close(e.results)

	e.mu.Lock()
	e.state = ExecutorStopped
	close(e.doneCh)
	e.mu.Unlock()
}

func (e *Executor) ForceShutdown() {
	e.cancel()
	e.Shutdown()
	e.cancelPendingJobs()
}
