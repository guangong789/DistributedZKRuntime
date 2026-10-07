package main

import (
	"context"
	"fmt"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type dispatchResult struct {
	resp *runtimepb.ExecuteJobResponse
	err  error
}

func dispatchJob(
	ctx context.Context,
	worker WorkerInfo,
	req *runtimepb.ExecuteJobRequest,
) (*runtimepb.ExecuteJobResponse, error) {
	conn, err := grpc.NewClient(
		worker.Address,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	client := runtimepb.NewWorkerServiceClient(conn)

	return client.ExecuteJob(ctx, req)
}

// executeJob runs attempts for an immutable specification.
// The caller must hold claimJob ownership for spec.JobID until this returns.
// Ownership stays at the entry point; this method neither claims nor releases it.
func (s *CoordinatorServer) executeJob(
	ctx context.Context,
	spec JobSpec,
) (*runtimepb.SubmitJobResponse, error) {
	return s.executeJobFromSource(ctx, spec, metrics.SourceSubmit)
}

func (s *CoordinatorServer) executeJobFromSource(
	ctx context.Context,
	spec JobSpec,
	source metrics.ExecutionSource,
) (*runtimepb.SubmitJobResponse, error) {
	observer := metrics.Resolve(s.metrics)
	const maxAttempts = 2
	leaseDuration := s.leaseDuration
	if leaseDuration <= 0 {
		leaseDuration = 5 * time.Second
	}

	var lastErr error

	for i := 0; i < maxAttempts; i++ {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		worker, ok := s.getNextWorker()
		if !ok {
			break
		}

		attemptID, err := s.startAttemptFromSource(
			spec.JobID,
			worker.ID,
			leaseDuration,
			spec.TaskType,
			spec.Payload,
			spec.TimeoutMs,
			source,
		)
		if err != nil {
			return nil, status.Errorf(
				codes.Internal,
				"failed to start attempt for job %d: %v",
				spec.JobID,
				err,
			)
		}

		attemptStart := time.Now()
		// Durable lifecycle events are omitted in the legacy nil-store mode.
		finishAttempt := func(outcome metrics.AttemptOutcome) {
			if s.jobStore != nil {
				observer.AttemptFinished(source, outcome, time.Since(attemptStart))
			}
		}
		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		resultCh := make(chan dispatchResult, 1)

		go func() {
			resp, err := dispatchJob(attemptCtx, worker, &runtimepb.ExecuteJobRequest{
				JobId:     spec.JobID,
				TaskType:  spec.TaskType,
				Payload:   spec.Payload,
				TimeoutMs: spec.TimeoutMs,
				AttemptId: attemptID,
			},
			)

			resultCh <- dispatchResult{
				resp: resp,
				err:  err,
			}
			if s.onDispatchDone != nil {
				s.onDispatchDone(spec.JobID, attemptID)
			}
		}()

		leaseTimer := time.NewTimer(leaseDuration)

		select {
		case result := <-resultCh:
			leaseTimer.Stop()
			cancelAttempt()

			if err := ctx.Err(); err != nil {
				finishAttempt(metrics.AttemptCancelled)
				return nil, err
			}

			if result.err != nil {
				lastErr = result.err
				finishAttempt(metrics.AttemptRPCError)

				fmt.Printf(
					"attempt failed: job=%d attempt=%d worker=%s err=%v\n",
					spec.JobID,
					attemptID,
					worker.ID,
					result.err,
				)

				continue
			}

			resp := result.resp

			if resp == nil ||
				resp.JobId != spec.JobID ||
				resp.AttemptId != attemptID ||
				!s.isLeaseCurrent(spec.JobID, attemptID) {

				lastErr = fmt.Errorf(
					"stale, expired, or mismatched result for job %d attempt %d",
					spec.JobID,
					attemptID,
				)

				observer.AttemptRejected(source, metrics.RejectFencing)
				finishAttempt(metrics.AttemptFenced)
				continue
			}

			finalState, err := jobStateFromStatus(resp.Status)
			if err != nil {
				finishAttempt(metrics.AttemptInvalidStatus)
				return nil, status.Errorf(
					codes.Internal,
					"invalid worker status for job %d attempt %d: %v",
					spec.JobID,
					attemptID,
					err,
				)
			}

			if spec.TaskType == "zk_preimage_prove" && finalState == JobSucceeded {
				if err := verifyZKPreimageResultWithMetrics(
					s.preimageVerifier,
					spec.Payload,
					resp.Output,
					observer,
				); err != nil {
					lastErr = fmt.Errorf(
						"preimage proof verification failed for job %d attempt %d: %w",
						spec.JobID,
						attemptID,
						err,
					)

					observer.AttemptRejected(source, metrics.RejectProof)
					finishAttempt(metrics.AttemptProofRejected)
					fmt.Printf(
						"attempt rejected: job=%d attempt=%d worker=%s err=%v\n",
						spec.JobID,
						attemptID,
						worker.ID,
						lastErr,
					)

					continue
				}
			}

			record := JobRecord{
				JobID:     spec.JobID,
				State:     finalState,
				AttemptID: attemptID,
				WorkerID:  worker.ID,
				TaskType:  spec.TaskType,
				Payload:   spec.Payload,
				TimeoutMs: spec.TimeoutMs,
			}

			if s.jobStore != nil {
				if err := s.jobStore.Save(record); err != nil {
					finishAttempt(metrics.AttemptPersistenceError)
					return nil, status.Errorf(
						codes.Internal,
						"failed to persist final state for job %d attempt %d: %v",
						spec.JobID,
						attemptID,
						err,
					)
				}
			}

			terminalState, outcome := metrics.TerminalSucceeded, metrics.AttemptAccepted
			switch finalState {
			case JobFailed:
				terminalState, outcome = metrics.TerminalFailed, metrics.AttemptTaskFailed
			case JobCancelled:
				terminalState, outcome = metrics.TerminalCancelled, metrics.AttemptTaskCancelled
			}
			if s.jobStore != nil {
				observer.JobTerminal(source, terminalState)
			}
			finishAttempt(outcome)
			return &runtimepb.SubmitJobResponse{
				JobId:     resp.JobId,
				Status:    resp.Status,
				Output:    resp.Output,
				Error:     resp.Error,
				AttemptId: resp.AttemptId,
			}, nil

		case <-leaseTimer.C:
			cancelAttempt()
			finishAttempt(metrics.AttemptLeaseExpired)

			lastErr = fmt.Errorf(
				"lease expired for job %d attempt %d",
				spec.JobID,
				attemptID,
			)

			fmt.Printf(
				"lease expired: job=%d attempt=%d worker=%s\n",
				spec.JobID,
				attemptID,
				worker.ID,
			)

			continue

		case <-ctx.Done():
			leaseTimer.Stop()
			cancelAttempt()
			finishAttempt(metrics.AttemptCancelled)

			return nil, ctx.Err()
		}
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if lastErr != nil {
		return nil, status.Errorf(codes.Unavailable, "job submission failed: %v", lastErr)
	}
	return nil, status.Error(codes.Unavailable, "no alive workers available")
}
