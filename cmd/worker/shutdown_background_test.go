package main

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/guangong789/DistributedZKRuntime/internal/rpcauth"
	"github.com/guangong789/DistributedZKRuntime/internal/testutil/testpki"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/stats"
)

type shutdownCoordinator struct {
	runtimepb.UnimplementedCoordinatorServiceServer
	register  func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error)
	heartbeat func(context.Context, *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error)
}

func (s shutdownCoordinator) RegisterWorker(ctx context.Context, r *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
	return s.register(ctx, r)
}
func (s shutdownCoordinator) Heartbeat(ctx context.Context, r *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
	return s.heartbeat(ctx, r)
}

// Server-side ConnEnd observes actual closure of the outgoing Worker connection.
type shutdownConnections struct{ ended chan struct{} }

func (s *shutdownConnections) TagRPC(ctx context.Context, _ *stats.RPCTagInfo) context.Context {
	return ctx
}
func (s *shutdownConnections) HandleRPC(context.Context, stats.RPCStats) {}
func (s *shutdownConnections) TagConn(ctx context.Context, _ *stats.ConnTagInfo) context.Context {
	return ctx
}
func (s *shutdownConnections) HandleConn(_ context.Context, event stats.ConnStats) {
	if _, ok := event.(*stats.ConnEnd); ok {
		s.ended <- struct{}{}
	}
}

func startShutdownCoordinator(t *testing.T, service runtimepb.CoordinatorServiceServer) (string, *testpki.CA, *shutdownConnections, *grpc.Server, <-chan struct{}) {
	t.Helper()
	ca := testpki.New(t)
	files := ca.Issue(t, testpki.Coordinator, nil)
	coordinator, _ := rpcauth.ParsePrincipal(testpki.Coordinator)
	worker, _ := rpcauth.ParsePrincipal(testpki.Worker)
	material, err := rpcauth.LoadMaterial(files.Cert, files.Key, files.CA, coordinator)
	if err != nil {
		t.Fatal(err)
	}
	policy, err := rpcauth.NewPolicy(coordinator, nil, []rpcauth.Principal{worker})
	if err != nil {
		t.Fatal(err)
	}
	config, err := rpcauth.NewServerTLS(material, policy)
	if err != nil {
		t.Fatal(err)
	}
	connections := &shutdownConnections{ended: make(chan struct{}, 8)}
	server := grpc.NewServer(grpc.Creds(credentials.NewTLS(config)), grpc.UnaryInterceptor(policy.UnaryInterceptor()), grpc.StatsHandler(connections))
	runtimepb.RegisterCoordinatorServiceServer(server, service)
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() { defer close(done); _ = server.Serve(listener) }()
	t.Cleanup(func() { server.Stop(); awaitWorker(t, done) })
	return listener.Addr().String(), ca, connections, server, done
}

func shutdownClientCredentials(t *testing.T, ca *testpki.CA, address string) credentials.TransportCredentials {
	t.Helper()
	files := ca.Issue(t, testpki.Worker, nil)
	transport, err := workerCoordinatorCredentials("worker-a", address, rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator})
	if err != nil {
		t.Fatal(err)
	}
	return transport
}

func TestWorkerHeartbeatCancellationClosesConnection(t *testing.T) {
	entered, returned := make(chan struct{}), make(chan struct{})
	var calls atomic.Int32
	service := shutdownCoordinator{heartbeat: func(ctx context.Context, r *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
		defer close(returned)
		calls.Add(1)
		if r.WorkerId != "worker-a" {
			t.Error("heartbeat changed identity")
		}
		if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 500*time.Millisecond {
			t.Error("heartbeat timeout changed")
		}
		close(entered)
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	address, ca, connections, _, _ := startShutdownCoordinator(t, service)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	transport := shutdownClientCredentials(t, ca, address)
	go func() { result <- runHeartbeat(ctx, "worker-a", "localhost:50051", address, transport) }()
	awaitWorker(t, entered)
	cancel()
	if err := awaitWorker(t, result); !errors.Is(err, context.Canceled) {
		t.Fatalf("heartbeat cancellation=%v", err)
	}
	awaitWorker(t, returned)
	awaitWorker(t, connections.ended)
	if calls.Load() != 1 {
		t.Fatal("heartbeat continued after cancellation")
	}
}

func workerTestConfig(t *testing.T, ca *testpki.CA, address string) workerConfig {
	t.Helper()
	dir := t.TempDir()
	pk, vk := filepath.Join(dir, "proving.key"), filepath.Join(dir, "verifying.key")
	if err := zk.GeneratePreimageSetup(pk, vk); err != nil {
		t.Fatal(err)
	}
	witness := filepath.Join(dir, "witnesses.json")
	if err := os.WriteFile(witness, []byte(`{"secret-001":42}`), 0600); err != nil {
		t.Fatal(err)
	}
	files := ca.Issue(t, testpki.Worker, nil)
	return workerConfig{ID: "worker-a", ListenAddress: "127.0.0.1:0", AdvertiseAddress: "localhost:50051", CoordinatorAddress: address,
		ProvingKeyPath: pk, WitnessFilePath: witness, MetricsAddress: "127.0.0.1:0", MaxConcurrentProofs: 1, ShutdownTimeout: 2 * time.Second,
		Security: rpcauth.CoordinatorClientConfig{CertFile: files.Cert, KeyFile: files.Key, CAFile: files.CA, ServerIdentity: testpki.Coordinator}}
}

func TestWorkerShutdownDuringStartupRegistration(t *testing.T) {
	for _, lateSuccess := range []bool{false, true} {
		t.Run(map[bool]string{false: "RPC cancellation", true: "late registration success"}[lateSuccess], func(t *testing.T) {
			entered, returned := make(chan struct{}), make(chan struct{})
			var heartbeatCalls atomic.Int32
			service := shutdownCoordinator{register: func(ctx context.Context, r *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
				defer close(returned)
				if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > 3*time.Second {
					t.Error("registration timeout changed")
				}
				if r.WorkerId != "worker-a" || r.Address != "localhost:50051" {
					t.Error("registration spec changed")
				}
				close(entered)
				<-ctx.Done()
				if lateSuccess {
					return &runtimepb.RegisterWorkerResponse{Accepted: true}, nil
				}
				return nil, ctx.Err()
			}, heartbeat: func(context.Context, *runtimepb.HeartbeatRequest) (*runtimepb.HeartbeatResponse, error) {
				heartbeatCalls.Add(1)
				return &runtimepb.HeartbeatResponse{Accepted: true}, nil
			}}
			address, ca, connections, _, _ := startShutdownCoordinator(t, service)
			cfg := workerTestConfig(t, ca, address)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			result := make(chan error, 1)
			go func() { result <- runWorker(ctx, cfg) }()
			awaitWorker(t, entered)
			cancel()
			if err := awaitWorker(t, result); err != nil {
				t.Fatal(err)
			}
			awaitWorker(t, returned)
			awaitWorker(t, connections.ended)
			if heartbeatCalls.Load() != 0 {
				t.Fatal("started heartbeat after startup cancellation")
			}
		})
	}
}

func TestWorkerShutdownConcurrentCoordinatorCancellation(t *testing.T) {
	entered, observed, release := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var once sync.Once
	service := shutdownExecutionService{execute: func(ctx context.Context, r *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		close(entered)
		<-ctx.Done()
		close(observed)
		<-release
		return nil, ctx.Err()
	}}
	p, client := shutdownWorker(t, service, time.Second)
	t.Cleanup(func() { once.Do(func() { close(release) }) })
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	response := make(chan error, 1)
	go func() { _, err := client.ExecuteJob(ctx, &runtimepb.ExecuteJobRequest{JobId: 12003}); response <- err }()
	awaitWorker(t, entered)
	start := make(chan struct{})
	stopped := make(chan error, 1)
	go func() { <-start; cancel() }()
	go func() { <-start; stopped <- p.shutdown() }()
	close(start)
	awaitWorker(t, observed)
	select {
	case <-p.lifecycle.done:
		t.Fatal("transport cancellation lost handler tracking")
	default:
	}
	once.Do(func() { close(release) })
	if err := awaitWorker(t, stopped); err != nil {
		t.Fatal(err)
	}
	if err := awaitWorker(t, response); err == nil {
		t.Fatal("canceled Coordinator RPC succeeded")
	}
}

func TestWorkerStartupErrorsAndPartialCleanup(t *testing.T) {
	service := shutdownCoordinator{register: func(context.Context, *runtimepb.RegisterWorkerRequest) (*runtimepb.RegisterWorkerResponse, error) {
		return &runtimepb.RegisterWorkerResponse{Accepted: false, Error: "denied"}, nil
	}}
	address, ca, connections, _, _ := startShutdownCoordinator(t, service)
	cfg := workerTestConfig(t, ca, address)
	if err := runWorker(context.Background(), cfg); err == nil {
		t.Fatal("registration rejection hidden")
	}
	awaitWorker(t, connections.ended)
	// These paths fail after metrics startup and must still return through cleanup.
	cfg.ListenAddress = "invalid listen endpoint"
	if err := runWorker(context.Background(), cfg); err == nil {
		t.Fatal("listener startup failure hidden")
	}
}
