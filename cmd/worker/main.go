package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	workerheartbeat "github.com/guangong789/DistributedZKRuntime/internal/workerheartbeat"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
)

type WorkerService struct {
	runtimepb.UnimplementedWorkerServiceServer

	preimageProver *zk.PreimageProver
	witnessStore   runtime.WitnessStore
	// Configure before serving; implementations must support concurrent calls.
	metrics metrics.Metrics
}

func buildTask(
	taskType string,
	payload string,
	preimageProver *zk.PreimageProver,
	witnessStore runtime.WitnessStore,
) (runtime.Task, error) {
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

	case "zk_preimage_prove":
		if preimageProver == nil {
			return nil, fmt.Errorf("preimage prover is not configured")
		}

		if witnessStore == nil {
			return nil, fmt.Errorf("witness store is not configured")
		}

		return runtime.ZKPreimageTask{
			Payload:      payload,
			Prover:       preimageProver,
			WitnessStore: witnessStore,
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
	task, err := buildTask(
		req.TaskType,
		req.Payload,
		s.preimageProver,
		s.witnessStore,
	)
	if err != nil {
		return &runtimepb.ExecuteJobResponse{
			JobId:     req.JobId,
			AttemptId: req.AttemptId,
			Status:    "failed",
			Output:    "",
			Error:     err.Error(),
		}, nil
	}

	if preimageTask, ok := task.(runtime.ZKPreimageTask); ok {
		preimageTask.Metrics = metrics.Resolve(s.metrics)
		task = preimageTask
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
	workerAddress string,
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

		err := workerheartbeat.Send(ctx, client, workerID, workerAddress)
		cancel()

		if err != nil {
			log.Println("heartbeat failed:", err)
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

	provingKeyPath := flag.String(
		"proving-key",
		"zk-artifacts/preimage/proving.key",
		"path to preimage Groth16 proving key",
	)

	witnessFilePath := flag.String(
		"witness-file",
		"private/witnesses.json",
		"path to local private witness file",
	)

	metricsAddress := flag.String("metrics-listen", "127.0.0.1:9091", "metrics HTTP listen address (required; loopback by default)")
	flag.Parse()

	// Load the proving capability once at worker startup.
	preimageProver, err := zk.LoadPreimageProver(*provingKeyPath)
	if err != nil {
		log.Fatal(err)
	}

	witnessStore, err := runtime.LoadMemoryWitnessStore(*witnessFilePath)
	if err != nil {
		log.Fatalf("failed to load witness store: %v", err)
	}

	registry := prom.NewRegistry()
	observer, err := prommetrics.New(registry)
	if err != nil {
		log.Fatalf("initialize metrics: %v", err)
	}
	workerService := &WorkerService{
		preimageProver: preimageProver,
		witnessStore:   witnessStore,
		metrics:        observer,
	}
	metricsServer, err := prommetrics.StartHTTP(*metricsAddress, registry)
	if err != nil {
		log.Fatalf("start metrics HTTP server: %v", err)
	}
	fmt.Printf("worker metrics listening on http://%s/metrics\n", metricsServer.Addr())
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		if err := metricsServer.Shutdown(shutdownCtx); err != nil {
			log.Printf("metrics HTTP shutdown: %v", err)
		}
	}()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	listener, err := net.Listen("tcp", *listenAddress)
	if err != nil {
		log.Fatalf("listen on %s: %v", *listenAddress, err)
	}

	server := grpc.NewServer()

	runtimepb.RegisterWorkerServiceServer(
		server,
		workerService,
	)

	// Start serving before registering with the coordinator so that
	// the worker is reachable as soon as the coordinator schedules it.
	go func() {
		fmt.Printf(
			"worker %s listening on %s\n",
			*workerID,
			*listenAddress,
		)

		if err := server.Serve(listener); err != nil {
			log.Fatalf("worker gRPC server: %v", err)
		}
	}()

	// Register once during startup.
	if err := registerWithCoordinator(
		*workerID,
		*advertiseAddress,
		*coordinatorAddress,
	); err != nil {
		log.Fatalf("register with coordinator: %v", err)
	}

	fmt.Println("worker registered successfully")

	// Heartbeats start after the initial registration succeeds.
	go runHeartbeat(
		*workerID,
		*advertiseAddress,
		*coordinatorAddress,
	)

	<-ctx.Done()
	server.Stop()
}
