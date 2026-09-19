package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
)

type WorkerService struct {
	runtimepb.UnimplementedWorkerServiceServer
}

func buildTask(taskType string, payload string) (runtime.Task, error) {
	switch taskType {
	case "hash":
		return runtime.HashTask{
			Input: payload,
		}, nil

	case "sleep":
		duration, err := time.ParseDuration(payload)
		if err != nil {
			return nil, err
		}

		return runtime.SleepTask{
			Duration: duration,
		}, nil

	default:
		return nil, fmt.Errorf("unknown task type: %s", taskType)
	}
}

func errorString(err error) string {
	if err == nil {
		return ""
	}

	return err.Error()
}

func (s *WorkerService) ExecuteJob(
	ctx context.Context,
	req *runtimepb.ExecuteJobRequest,
) (*runtimepb.ExecuteJobResponse, error) {
	task, err := buildTask(req.TaskType, req.Payload)
	if err != nil {
		return &runtimepb.ExecuteJobResponse{
			JobId:     req.JobId,
			AttemptId: req.AttemptId,
			Status:    "failed",
			Output:    "",
			Error:     err.Error(),
		}, nil
	}

	timeout := time.Duration(req.TimeoutMs) * time.Millisecond

	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	job := runtime.Job{
		ID:      int(req.JobId),
		Timeout: timeout,
		Task:    task,
		Status:  runtime.JobRunning,
	}

	result := runtime.ExecuteJob(jobCtx, job)

	fmt.Printf(
		"executing job: id=%d attempt=%d type=%s\n",
		req.JobId,
		req.AttemptId,
		req.TaskType,
	)

	return &runtimepb.ExecuteJobResponse{
		JobId:     int64(result.JobID),
		AttemptId: req.AttemptId,
		Status:    result.Status.String(),
		Output:    result.Output,
		Error:     errorString(result.Err),
	}, nil
}

func registerWithCoordinator(
	workerID string,
	workerAddress string,
	coordinatorAddress string,
) error {
	conn, err := grpc.NewClient(
		coordinatorAddress,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := runtimepb.NewCoordinatorServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		context.Background(),
		3*time.Second,
	)
	defer cancel()

	resp, err := client.RegisterWorker(
		ctx,
		&runtimepb.RegisterWorkerRequest{
			WorkerId: workerID,
			Address:  workerAddress,
		},
	)
	if err != nil {
		return err
	}

	if !resp.Accepted {
		return fmt.Errorf(
			"registration rejected: %s",
			resp.Error,
		)
	}

	return nil
}

func runHeartbeat(
	workerID string,
	coordinatorAddress string,
) {
	conn, err := grpc.NewClient(
		coordinatorAddress,
		grpc.WithTransportCredentials(
			insecure.NewCredentials(),
		),
	)
	if err != nil {
		log.Panicln("heartbeat connection failed:", err)
		return
	}
	defer conn.Close()

	client := runtimepb.NewCoordinatorServiceClient(conn)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for range ticker.C {
		ctx, cancel := context.WithTimeout(
			context.Background(),
			500*time.Millisecond,
		)

		resp, err := client.Heartbeat(
			ctx,
			&runtimepb.HeartbeatRequest{
				WorkerId: workerID,
			},
		)

		cancel()

		if err != nil {
			log.Println("heartbeat failed:", err)
			continue
		}
		if !resp.Accepted {
			log.Println("heartbeat rejected")
			continue
		}

		fmt.Println("heartbeat sent")
	}
}

func main() {
	workerID := flag.String(
		"id",
		"worker-1",
		"worker ID",
	)

	listenAddress := flag.String(
		"listen",
		":50051",
		"worker listen address",
	)

	advertiseAddress := flag.String(
		"advertise",
		"localhost:50051",
		"worker advertised address",
	)

	coordinatorAddress := flag.String(
		"coordinator",
		"localhost:50050",
		"coordinator address",
	)

	flag.Parse()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()

	runtimepb.RegisterWorkerServiceServer(
		server,
		&WorkerService{},
	)

	go runHeartbeat(
		*workerID,
		*coordinatorAddress,
	)

	go func() {
		fmt.Printf("worker %s listening on %s\n", *workerID, *listenAddress)

		if err := server.Serve(listener); err != nil {
			log.Fatal(err)
		}
	}()

	if err := registerWithCoordinator(
		*workerID,
		*advertiseAddress,
		*coordinatorAddress,
	); err != nil {
		log.Fatal(err)
	}

	fmt.Println("worker registered successfully")

	select {}
}
