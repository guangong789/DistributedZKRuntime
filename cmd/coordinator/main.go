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
	const maxAttempts = 2
	const leaseDuration = 5 * time.Second

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
		resp, err := dispatchJob(attemptCtx, worker, &runtimepb.ExecuteJobRequest{
			JobId:     req.JobId,
			TaskType:  req.TaskType,
			Payload:   req.Payload,
			TimeoutMs: req.TimeoutMs,
			AttemptId: attemptID,
		})
		cancelAttempt()

		// Only cancel the child above; each retry inherits the live parent.
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err != nil {
			lastErr = err
			fmt.Printf("attempt failed: job=%d attempt=%d worker=%s err=%v\n",
				req.JobId, attemptID, worker.ID, err)
			continue
		}

		// A response must belong to this dispatch, not merely to any current lease.
		if resp == nil || resp.JobId != req.JobId || resp.AttemptId != attemptID ||
			!s.isLeaseCurrent(req.JobId, attemptID) {
			lastErr = fmt.Errorf("stale, expired, or mismatched result for job %d attempt %d", req.JobId, attemptID)
			continue
		}

		return &runtimepb.SubmitJobResponse{
			JobId:     resp.JobId,
			Status:    resp.Status,
			Output:    resp.Output,
			Error:     resp.Error,
			AttemptId: resp.AttemptId,
		}, nil
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
