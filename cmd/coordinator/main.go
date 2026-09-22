package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"sync"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type CoordinatorServer struct {
	runtimepb.UnimplementedCoordinatorServiceServer

	// mu protects the worker registry, round-robin cursor, attempts, and leases.
	mu          sync.Mutex
	workers     map[string]WorkerInfo
	workerOrder []string
	nextWorker  int

	jobAttempts map[int64]int64
	leases      map[int64]JobLease

	activeJobs map[int64]struct{}

	// Set before serving requests; non-positive values use the 5-second default.
	leaseDuration time.Duration

	// Optional completion observer, called after dispatch publishes its result.
	// Configure before serving; callbacks may run concurrently.
	onDispatchDone func(jobID, attemptID int64)
}

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

func (s *CoordinatorServer) SubmitJob(
	ctx context.Context,
	req *runtimepb.SubmitJobRequest,
) (*runtimepb.SubmitJobResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	if !s.claimJob(req.JobId) {
		return nil, status.Errorf(
			codes.AlreadyExists,
			"job %d is already being submitted",
			req.JobId,
		)
	}

	defer s.releaseJob(req.JobId)

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

		attemptID := s.startAttempt(req.JobId, worker.ID, leaseDuration)
		attemptCtx, cancelAttempt := context.WithCancel(ctx)
		resultCh := make(chan dispatchResult, 1)

		go func() {
			resp, err := dispatchJob(attemptCtx, worker, &runtimepb.ExecuteJobRequest{
				JobId:     req.JobId,
				TaskType:  req.TaskType,
				Payload:   req.Payload,
				TimeoutMs: req.TimeoutMs,
				AttemptId: attemptID,
			},
			)

			resultCh <- dispatchResult{
				resp: resp,
				err:  err,
			}
			if s.onDispatchDone != nil {
				s.onDispatchDone(req.JobId, attemptID)
			}
		}()

		leaseTimer := time.NewTimer(leaseDuration)

		select {
		case result := <-resultCh:
			leaseTimer.Stop()
			cancelAttempt()

			if err := ctx.Err(); err != nil {
				return nil, err
			}

			if result.err != nil {
				lastErr = result.err

				fmt.Printf(
					"attempt failed: job=%d attempt=%d worker=%s err=%v\n",
					req.JobId,
					attemptID,
					worker.ID,
					result.err,
				)

				continue
			}

			resp := result.resp

			if resp == nil ||
				resp.JobId != req.JobId ||
				resp.AttemptId != attemptID ||
				!s.isLeaseCurrent(req.JobId, attemptID) {

				lastErr = fmt.Errorf(
					"stale, expired, or mismatched result for job %d attempt %d",
					req.JobId,
					attemptID,
				)

				continue
			}

			return &runtimepb.SubmitJobResponse{
				JobId:     resp.JobId,
				Status:    resp.Status,
				Output:    resp.Output,
				Error:     resp.Error,
				AttemptId: resp.AttemptId,
			}, nil

		case <-leaseTimer.C:
			cancelAttempt()

			lastErr = fmt.Errorf(
				"lease expired for job %d attempt %d",
				req.JobId,
				attemptID,
			)

			fmt.Printf(
				"lease expired: job=%d attempt=%d worker=%s\n",
				req.JobId,
				attemptID,
				worker.ID,
			)

			continue

		case <-ctx.Done():
			leaseTimer.Stop()
			cancelAttempt()

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

func main() {
	listener, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()

	coordinator := &CoordinatorServer{
		workers:     make(map[string]WorkerInfo),
		jobAttempts: make(map[int64]int64),
		leases:      make(map[int64]JobLease),
	}

	runtimepb.RegisterCoordinatorServiceServer(
		server,
		coordinator,
	)

	fmt.Println("coordinator listening on :50050")

	go coordinator.runLivenessMonitor(
		time.Second,
		3*time.Second,
	)

	if err := server.Serve(listener); err != nil {
		log.Fatal(err)
	}
}
