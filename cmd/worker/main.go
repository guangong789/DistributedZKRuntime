package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	workerheartbeat "github.com/guangong789/DistributedZKRuntime/internal/workerheartbeat"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"

	runtime "github.com/guangong789/DistributedZKRuntime/internal/runtime"
)

type WorkerService struct {
	runtimepb.UnimplementedWorkerServiceServer

	preimageProver *zk.PreimageProver
	witnessStore   runtime.WitnessStore
	// Configure one shared limiter before serving ZK requests.
	provingLimiter *runtime.ProvingLimiter
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
	timeout, err := runtime.TaskTimeoutFromMilliseconds(req.TimeoutMs)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, err.Error())
	}
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
		if s.provingLimiter == nil {
			return nil, status.Error(codes.FailedPrecondition, "proving limiter is not configured")
		}
		preimageTask.Limiter = s.provingLimiter
		preimageTask.Metrics = metrics.Resolve(s.metrics)
		task = preimageTask
	}

	jobCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	job := runtime.Job{
		ID:      int(req.JobId),
		Timeout: timeout,
		Task:    task,
		Status:  runtime.JobRunning,
	}

	result := runtime.ExecuteJob(jobCtx, job)
	if errors.Is(result.Err, runtime.ErrProvingBusy) {
		return nil, status.Error(codes.ResourceExhausted, result.Err.Error())
	}

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
	parent context.Context,
	workerID string,
	workerAddress string,
	coordinatorAddress string,
	transport credentials.TransportCredentials,
) error {
	if err := parent.Err(); err != nil {
		return err
	}
	conn, err := newCoordinatorConnection(coordinatorAddress, transport)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := runtimepb.NewCoordinatorServiceClient(conn)

	ctx, cancel := context.WithTimeout(
		parent,
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
	parent context.Context,
	workerID string,
	workerAddress string,
	coordinatorAddress string,
	transport credentials.TransportCredentials,
) error {
	if err := parent.Err(); err != nil {
		return err
	}
	conn, err := newCoordinatorConnection(coordinatorAddress, transport)
	if err != nil {
		return fmt.Errorf("heartbeat connection: %w", err)
	}
	defer conn.Close()

	client := runtimepb.NewCoordinatorServiceClient(conn)

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-parent.Done():
			return parent.Err()
		case <-ticker.C:
		}
		if err := parent.Err(); err != nil {
			return err
		}
		ctx, cancel := context.WithTimeout(
			parent,
			500*time.Millisecond,
		)

		err := workerheartbeat.Send(ctx, client, workerID, workerAddress)
		cancel()

		if err != nil && parent.Err() == nil {
			log.Println("heartbeat failed:", err)
		}
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
	maxConcurrentProofs := flag.Int("max-concurrent-proofs", 1, "maximum simultaneous preimage proving calls (positive)")
	shutdownTimeout := workerShutdownFlag(flag.CommandLine)
	security := workerSecurityFlags(flag.CommandLine)
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	cfg := workerConfig{ID: *workerID, ListenAddress: *listenAddress, AdvertiseAddress: *advertiseAddress,
		CoordinatorAddress: *coordinatorAddress, ProvingKeyPath: *provingKeyPath, WitnessFilePath: *witnessFilePath,
		MetricsAddress: *metricsAddress, MaxConcurrentProofs: *maxConcurrentProofs, ShutdownTimeout: *shutdownTimeout, Security: *security}
	if err := runWorker(ctx, cfg); err != nil {
		// Helpers report incomplete shutdown; only the process entry point exits.
		log.Print(err)
		os.Exit(1)
	}
}

func workerShutdownFlag(fs *flag.FlagSet) *time.Duration {
	return fs.Duration("shutdown-timeout", defaultWorkerShutdownTimeout, "positive total shutdown budget, independent of task deadlines; expiration forces process exit")
}

type workerConfig struct {
	ID, ListenAddress, AdvertiseAddress, CoordinatorAddress string
	ProvingKeyPath, WitnessFilePath, MetricsAddress         string
	MaxConcurrentProofs                                     int
	ShutdownTimeout                                         time.Duration
	Security                                                rpcauth.CoordinatorClientConfig
}

func runWorker(ctx context.Context, cfg workerConfig) (runErr error) {
	if cfg.ShutdownTimeout <= 0 {
		return errors.New("-shutdown-timeout must be positive")
	}
	if ctx.Err() != nil {
		return nil
	}
	p := newWorkerProcess(cfg.ShutdownTimeout)
	defer func() { runErr = errors.Join(runErr, p.shutdown()) }()
	ready, backgroundDone := make(chan struct{}), make(chan struct{})
	p.resourcesReady, p.backgroundDone = ready, backgroundDone
	backgroundResult := make(chan error, 1)
	go func() {
		defer close(backgroundDone)
		var once sync.Once
		publish := func() { once.Do(func() { close(ready) }) }
		defer publish() // Also publish partial resources on any startup error.
		backgroundResult <- initializeWorker(p, cfg, publish)
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-backgroundResult:
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("worker background loop stopped unexpectedly")
		}
		return err
	case err := <-p.serveResult:
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("worker gRPC server stopped unexpectedly")
		}
		return fmt.Errorf("worker gRPC server: %w", err)
	}
}

// One owned producer initializes resources, registers, and runs heartbeat. It
// publishes resource fields before registration so Stop need not wait for an
// in-flight registration or its connection Close. No resources change afterward.
func initializeWorker(p *workerProcess, cfg workerConfig, publish func()) error {
	ctx := p.lifecycle.ctx
	if err := ctx.Err(); err != nil {
		return err
	}
	transport, err := loadWorkerTransport(cfg.ID, cfg.CoordinatorAddress, cfg.Security)
	if err != nil {
		return fmt.Errorf("worker mTLS configuration: %w", err)
	}
	limiter, err := runtime.NewProvingLimiter(cfg.MaxConcurrentProofs)
	if err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	prover, err := zk.LoadPreimageProver(cfg.ProvingKeyPath)
	if err != nil {
		return fmt.Errorf("load preimage prover: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	witnesses, err := runtime.LoadMemoryWitnessStore(cfg.WitnessFilePath)
	if err != nil {
		return fmt.Errorf("load witness store: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	registry := prom.NewRegistry()
	observer, err := prommetrics.New(registry)
	if err != nil {
		return fmt.Errorf("initialize metrics: %w", err)
	}
	service := &WorkerService{preimageProver: prover, witnessStore: witnesses, provingLimiter: limiter, metrics: observer}
	metricsServer, err := prommetrics.StartHTTP(cfg.MetricsAddress, registry)
	if err != nil {
		return fmt.Errorf("start metrics HTTP server: %w", err)
	}
	p.shutdownMetrics = metricsServer.Shutdown
	fmt.Printf("worker metrics listening on http://%s/metrics\n", metricsServer.Addr())
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", cfg.ListenAddress)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", cfg.ListenAddress, err)
	}
	p.listener = listener
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := transport.newServer(service, p.lifecycle)
	if err != nil {
		return fmt.Errorf("worker mTLS server configuration: %w", err)
	}
	// Serve before registration so recovery can immediately schedule this Worker.
	p.start(server, listener)
	publish()
	fmt.Printf("worker %s listening on %s\n", cfg.ID, listener.Addr())
	if err := registerWithCoordinator(ctx, cfg.ID, cfg.AdvertiseAddress, cfg.CoordinatorAddress, transport.client); err != nil {
		return fmt.Errorf("register with coordinator: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	fmt.Println("worker registered successfully")
	return runHeartbeat(ctx, cfg.ID, cfg.AdvertiseAddress, cfg.CoordinatorAddress, transport.client)
}
