package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	prommetrics "github.com/guangong789/DistributedZKRuntime/internal/metrics/prometheus"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	prom "github.com/prometheus/client_golang/prometheus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

type CoordinatorServer struct {
	runtimepb.UnimplementedCoordinatorServiceServer

	// mu protects the worker registry, round-robin cursor, attempts, leases, and active jobs.
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

	jobStore  JobStore
	lifecycle *coordinatorLifecycle
	// Joined only after all admitted handlers and recovery producers have exited.
	dispatchWG sync.WaitGroup
	// Configure before serving. Worker readiness notifications coalesce.
	recoveryReady chan struct{}

	preimageVerifier PreimageProofVerifier
	// Configure before serving; implementations must support concurrent calls.
	metrics metrics.Metrics
	// Configure before serving. Nil fails closed; isolated tests may explicitly
	// inject their own transport. Production binds TLS to each selected Worker.
	workerCredentials func(WorkerInfo) (credentials.TransportCredentials, error)
}

func (s *CoordinatorServer) SubmitJob(
	ctx context.Context,
	req *runtimepb.SubmitJobRequest,
) (*runtimepb.SubmitJobResponse, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	spec, err := validateJobSpec(jobSpecFromRequest(req))
	if err != nil {
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

	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	defer func() { observer.ObserveJobExecution(metrics.SourceSubmit, time.Since(start)) }()
	observer.JobSubmitted(metrics.SourceSubmit)
	return s.executeJob(ctx, spec)
}

func main() {
	dbPath := flag.String(
		"db",
		"runtime.db",
		"path to coordinator SQLite database",
	)

	verifyingKeyPath := flag.String(
		"verifying-key",
		"zk-artifacts/preimage/verifying.key",
		"path to preimage verifying key",
	)

	metricsAddress := flag.String("metrics-listen", "127.0.0.1:9090", "metrics HTTP listen address (required; loopback by default)")
	execution := coordinatorExecutionFlags(flag.CommandLine)
	security := coordinatorSecurityFlags(flag.CommandLine)
	flag.Parse()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if err := runCoordinator(ctx, *execution, *security, *dbPath, *verifyingKeyPath, *metricsAddress); err != nil {
		// Incomplete shutdown must not run deferred resource closure. Unfinished
		// Running records retain the existing crash-recovery semantics on restart.
		log.Print(err)
		os.Exit(1)
	}
}

// runCoordinator owns startup and runtime resources. All exits, including startup
// and Serve failures, use the same shutdown protocol; it never calls os.Exit.
func runCoordinator(ctx context.Context, execution coordinatorExecutionConfig, security coordinatorSecurityConfig, dbPath, verifyingKeyPath, metricsAddress string) (runErr error) {
	if err := execution.validate(); err != nil {
		return fmt.Errorf("coordinator execution configuration: %w", err)
	}
	if ctx.Err() != nil {
		return nil
	}
	transport, err := security.load()
	if err != nil {
		return fmt.Errorf("coordinator mTLS configuration: %w", err)
	}
	coordinator := &CoordinatorServer{
		workers: make(map[string]WorkerInfo), jobAttempts: make(map[int64]int64),
		leases: make(map[int64]JobLease), activeJobs: make(map[int64]struct{}),
		leaseDuration: execution.AttemptLease, recoveryReady: make(chan struct{}, 1),
		workerCredentials: transport.workerCredentials,
	}
	process := newCoordinatorProcess(coordinator, execution.ShutdownTimeout)
	defer func() { runErr = errors.Join(runErr, process.shutdown()) }()
	// Startup is an owned producer too: a signal can select the shutdown
	// deadline even if a non-cancelable startup store operation is still blocked.
	startupDone := make(chan struct{})
	process.startupDone = startupDone
	initialized := make(chan error, 1)
	go func() {
		defer close(startupDone)
		initialized <- initializeCoordinator(process, execution, transport, dbPath, verifyingKeyPath, metricsAddress)
	}()
	select {
	case <-ctx.Done():
		return nil
	case err := <-initialized:
		if err != nil {
			return err
		}
	}
	select {
	case <-ctx.Done():
		return nil
	case err := <-process.serveResult:
		if ctx.Err() != nil {
			return nil
		}
		if err == nil {
			return errors.New("coordinator gRPC server stopped unexpectedly")
		}
		return fmt.Errorf("coordinator gRPC server: %w", err)
	}
}

func initializeCoordinator(process *coordinatorProcess, execution coordinatorExecutionConfig, transport *coordinatorTransport, dbPath, verifyingKeyPath, metricsAddress string) error {
	coordinator := process.service
	ctx := coordinator.lifecycle.ctx
	if err := ctx.Err(); err != nil {
		return err
	}
	store, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		return fmt.Errorf("open job store: %w", err)
	}
	process.closeStore = store.Close
	coordinator.jobStore = store
	if err := ctx.Err(); err != nil {
		return err
	}
	preimageVerifier, err := zk.LoadPreimageVerifier(verifyingKeyPath)
	if err != nil {
		return fmt.Errorf("load preimage verifier: %w", err)
	}
	coordinator.preimageVerifier = preimageVerifier
	if err := ctx.Err(); err != nil {
		return err
	}
	registry := prom.NewRegistry()
	observer, err := prommetrics.New(registry)
	if err != nil {
		return fmt.Errorf("initialize metrics: %w", err)
	}
	coordinator.metrics = observer
	metricsServer, err := prommetrics.StartHTTP(metricsAddress, registry)
	if err != nil {
		return fmt.Errorf("start metrics HTTP server: %w", err)
	}
	process.shutdownMetrics = metricsServer.Shutdown
	fmt.Printf("coordinator metrics listening on http://%s/metrics\n", metricsServer.Addr())
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := coordinator.recoverRunningJobs(); err != nil {
		return fmt.Errorf("recover running jobs: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	listener, err := execution.listen()
	if err != nil {
		return fmt.Errorf("coordinator listen: %w", err)
	}
	process.listener = listener
	if err := ctx.Err(); err != nil {
		return err
	}
	process.start(transport.newServer(coordinator), listener)
	fmt.Printf("coordinator mTLS listening on %s\n", listener.Addr())
	return nil
}
