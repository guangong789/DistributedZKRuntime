package main

import (
	"context"
	"net"
	"testing"
	"time"

	workerheartbeat "github.com/guangong789/DistributedZKRuntime/internal/workerheartbeat"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type recordingCoordinatorClient struct {
	runtimepb.CoordinatorServiceClient
	heartbeatAccepted []bool
	registrations     []*runtimepb.RegisterWorkerRequest
}

func (c *recordingCoordinatorClient) Heartbeat(
	ctx context.Context,
	req *runtimepb.HeartbeatRequest,
	opts ...grpc.CallOption,
) (*runtimepb.HeartbeatResponse, error) {
	resp, err := c.CoordinatorServiceClient.Heartbeat(ctx, req, opts...)
	if err == nil {
		c.heartbeatAccepted = append(c.heartbeatAccepted, resp.Accepted)
	}
	return resp, err
}

func (c *recordingCoordinatorClient) RegisterWorker(
	ctx context.Context,
	req *runtimepb.RegisterWorkerRequest,
	opts ...grpc.CallOption,
) (*runtimepb.RegisterWorkerResponse, error) {
	c.registrations = append(c.registrations, req)
	return c.CoordinatorServiceClient.RegisterWorker(ctx, req, opts...)
}

func startTestCoordinator(
	t *testing.T,
	coordinator *CoordinatorServer,
) runtimepb.CoordinatorServiceClient {
	t.Helper()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	server := grpc.NewServer()
	runtimepb.RegisterCoordinatorServiceServer(server, coordinator)

	go func() {
		if err := server.Serve(listener); err != nil {
			t.Logf("coordinator test server stopped: %v", err)
		}
	}()

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		server.Stop()
		t.Fatal(err)
	}

	t.Cleanup(func() {
		_ = conn.Close()
		server.Stop()
	})

	return runtimepb.NewCoordinatorServiceClient(conn)
}

func coordinatorWorkerSnapshot(
	coordinator *CoordinatorServer,
	workerID string,
) (WorkerInfo, bool, []string) {
	coordinator.mu.Lock()
	defer coordinator.mu.Unlock()

	worker, ok := coordinator.workers[workerID]
	order := append([]string(nil), coordinator.workerOrder...)
	return worker, ok, order
}

func TestWorkerRecoversAfterCoordinatorStateLoss(t *testing.T) {
	const workerID = "worker-recovery"
	const workerAddress = "127.0.0.1:51051"

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	originalCoordinator := &CoordinatorServer{}
	originalClient := startTestCoordinator(t, originalCoordinator)

	registration, err := originalClient.RegisterWorker(
		ctx,
		&runtimepb.RegisterWorkerRequest{
			WorkerId: workerID,
			Address:  workerAddress,
		},
	)
	if err != nil || !registration.Accepted {
		t.Fatalf("initial registration: response=%v error=%v", registration, err)
	}

	originalWorker, ok, originalOrder := coordinatorWorkerSnapshot(
		originalCoordinator,
		workerID,
	)
	if !ok || originalWorker.ID != workerID ||
		originalWorker.Address != workerAddress ||
		originalWorker.Status != WorkerAlive ||
		len(originalOrder) != 1 || originalOrder[0] != workerID {
		t.Fatalf(
			"initial coordinator state: worker=%+v found=%v order=%v",
			originalWorker,
			ok,
			originalOrder,
		)
	}

	// A fresh service instance models coordinator restart: the old in-memory
	// registry is absent, while the worker retains its ID and advertised address.
	restartedCoordinator := &CoordinatorServer{}
	recoveryClient := &recordingCoordinatorClient{
		CoordinatorServiceClient: startTestCoordinator(t, restartedCoordinator),
	}

	if err := workerheartbeat.Send(ctx, recoveryClient, workerID, workerAddress); err != nil {
		t.Fatalf("heartbeat recovery: %v", err)
	}
	if len(recoveryClient.heartbeatAccepted) != 1 || recoveryClient.heartbeatAccepted[0] {
		t.Fatalf("unknown-worker heartbeat responses: %v", recoveryClient.heartbeatAccepted)
	}
	if len(recoveryClient.registrations) != 1 ||
		recoveryClient.registrations[0].WorkerId != workerID ||
		recoveryClient.registrations[0].Address != workerAddress {
		t.Fatalf("re-registration requests: %v", recoveryClient.registrations)
	}

	recoveredWorker, ok, recoveredOrder := coordinatorWorkerSnapshot(
		restartedCoordinator,
		workerID,
	)
	if !ok || recoveredWorker.ID != workerID ||
		recoveredWorker.Address != workerAddress ||
		recoveredWorker.Status != WorkerAlive ||
		len(recoveredOrder) != 1 || recoveredOrder[0] != workerID {
		t.Fatalf(
			"recovered coordinator state: worker=%+v found=%v order=%v",
			recoveredWorker,
			ok,
			recoveredOrder,
		)
	}

	beforeHeartbeat := time.Now()
	if err := workerheartbeat.Send(ctx, recoveryClient, workerID, workerAddress); err != nil {
		t.Fatalf("post-recovery heartbeat: %v", err)
	}
	if len(recoveryClient.heartbeatAccepted) != 2 ||
		!recoveryClient.heartbeatAccepted[1] ||
		len(recoveryClient.registrations) != 1 {
		t.Fatalf(
			"post-recovery calls: heartbeats=%v registrations=%v",
			recoveryClient.heartbeatAccepted,
			recoveryClient.registrations,
		)
	}

	refreshedWorker, ok, finalOrder := coordinatorWorkerSnapshot(
		restartedCoordinator,
		workerID,
	)
	if !ok || refreshedWorker.LastSeen.Before(beforeHeartbeat) {
		t.Fatalf(
			"heartbeat did not refresh LastSeen: worker=%+v before=%v",
			refreshedWorker,
			beforeHeartbeat,
		)
	}
	if refreshedWorker.ID != workerID ||
		refreshedWorker.Address != workerAddress ||
		refreshedWorker.Status != WorkerAlive {
		t.Fatalf("post-heartbeat worker changed: %+v", refreshedWorker)
	}
	if len(finalOrder) != 1 || finalOrder[0] != workerID {
		t.Fatalf("recovery duplicated worker order: %v", finalOrder)
	}

	selected, ok := restartedCoordinator.getNextWorker()
	if !ok || selected.ID != workerID ||
		selected.Address != workerAddress || selected.Status != WorkerAlive {
		t.Fatalf("recovered worker is not schedulable: %+v found=%v", selected, ok)
	}
}
