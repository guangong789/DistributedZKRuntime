package main

import (
	"context"
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
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
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

	jobStore JobStore
	// Configure before serving. Registrations coalesce readiness notifications.
	recoveryReady chan struct{}

	preimageVerifier PreimageProofVerifier
	// Configure before serving; implementations must support concurrent calls.
	metrics metrics.Metrics
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

	observer := metrics.Resolve(s.metrics)
	start := time.Now()
	defer func() { observer.ObserveJobExecution(metrics.SourceSubmit, time.Since(start)) }()
	observer.JobSubmitted(metrics.SourceSubmit)
	return s.executeJob(ctx, jobSpecFromRequest(req))
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

	flag.Parse()

	store, err := NewSQLiteJobStore(*dbPath)
	if err != nil {
		log.Fatalf("failed to open job store: %v", err)
	}
	defer store.Close()

	preimageVerifier, err := zk.LoadPreimageVerifier(*verifyingKeyPath)
	if err != nil {
		log.Fatalf("failed to load preimage verifier: %v", err)
	}

	coordinator := &CoordinatorServer{
		workers:          make(map[string]WorkerInfo),
		jobAttempts:      make(map[int64]int64),
		leases:           make(map[int64]JobLease),
		activeJobs:       make(map[int64]struct{}),
		jobStore:         store,
		leaseDuration:    5 * time.Second,
		recoveryReady:    make(chan struct{}, 1),
		preimageVerifier: preimageVerifier,
	}

	if err := coordinator.recoverRunningJobs(); err != nil {
		log.Fatalf("failed to recover running jobs: %v", err)
	}

	listener, err := net.Listen("tcp", ":50050")
	if err != nil {
		log.Fatal(err)
	}

	server := grpc.NewServer()

	runtimepb.RegisterCoordinatorServiceServer(
		server,
		coordinator,
	)

	fmt.Println("coordinator listening on :50050")

	go coordinator.runLivenessMonitor(
		time.Second,
		3*time.Second,
	)

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	recoveryDone := make(chan struct{})
	go func() {
		defer close(recoveryDone)
		coordinator.runRecoveryLoop(ctx)
	}()
	go func() {
		<-ctx.Done()
		server.Stop()
	}()

	err = server.Serve(listener)
	cancel()
	// Recovery must stop using the store before it is closed.
	<-recoveryDone
	if err != nil {
		log.Fatal(err)
	}
}
