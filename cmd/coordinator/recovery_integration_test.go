package main

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"sync"
	"sync/atomic"
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

func TestSQLiteRestartRecoversJobAfterWorkerReregistration(t *testing.T) {
	const jobID int64 = 801
	const workerID = "restarted-worker"
	request := &runtimepb.SubmitJobRequest{
		JobId: jobID, TaskType: "hash", Payload: "proof-input", TimeoutMs: 1500,
	}
	dbPath := filepath.Join(t.TempDir(), "jobs.db")
	storeA, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open first SQLite store: %v", err)
	}
	t.Cleanup(func() { _ = storeA.Close() })
	coordinatorA := &CoordinatorServer{jobStore: storeA}
	oldStarted := make(chan struct{})
	releaseOld := make(chan struct{})
	oldReturned := make(chan struct{})
	oldDispatchDone := make(chan struct{}, 1)
	var releaseOnce sync.Once
	var firstCalls, secondCalls atomic.Int32
	coordinatorA.onDispatchDone = func(id, attempt int64) {
		if id == jobID && attempt == 1 {
			oldDispatchDone <- struct{}{}
		}
	}
	registerTestWorker(t, coordinatorA, workerID, func(_ context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
		switch req.AttemptId {
		case 1:
			if firstCalls.Add(1) == 1 {
				close(oldStarted)
			}
			// Model a remote worker that finishes after its original coordinator
			// has canceled the RPC and a new coordinator accepts Attempt 2.
			<-releaseOld
			close(oldReturned)
			return successfulResult(req), nil
		case 2:
			secondCalls.Add(1)
			if req.JobId != jobID || req.TaskType != request.TaskType ||
				req.Payload != request.Payload || req.TimeoutMs != request.TimeoutMs {
				t.Errorf("recovered worker request lost task specification: %+v", req)
			}
			return successfulResult(req), nil
		default:
			t.Errorf("unexpected worker attempt: %+v", req)
			return successfulResult(req), nil
		}
	})
	// Cleanup runs before the test worker server stops, including after t.Fatal.
	t.Cleanup(func() { releaseOnce.Do(func() { close(releaseOld) }) })
	worker, ok, _ := coordinatorWorkerSnapshot(coordinatorA, workerID)
	if !ok {
		t.Fatal("worker was not registered with Coordinator A")
	}

	ctxA, cancelA := context.WithCancel(context.Background())
	defer cancelA()
	submitDone := make(chan error, 1)
	go func() {
		_, err := coordinatorA.SubmitJob(ctxA, request)
		submitDone <- err
	}()
	select {
	case <-oldStarted:
	case <-time.After(5 * time.Second):
		t.Fatal("Attempt 1 did not reach the old worker")
	}
	running := JobRecord{
		JobID: jobID, State: JobRunning, AttemptID: 1, WorkerID: workerID,
		TaskType: request.TaskType, Payload: request.Payload, TimeoutMs: request.TimeoutMs,
	}
	assertSQLiteJobRecord(t, storeA, running)
	cancelA()
	select {
	case err := <-submitDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("old submission error=%v, want cancellation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("old submission did not stop")
	}
	select {
	case <-oldDispatchDone:
	case <-time.After(5 * time.Second):
		t.Fatal("old dispatch goroutine did not finish")
	}
	assertSQLiteJobRecord(t, storeA, running)
	if err := storeA.Close(); err != nil {
		t.Fatalf("close first SQLite store: %v", err)
	}
	// Coordinator A and its store are no longer used. Only the DB file is
	// carried into this fresh coordinator with empty in-memory state.
	storeB, err := NewSQLiteJobStore(dbPath)
	if err != nil {
		t.Fatalf("open replacement SQLite store: %v", err)
	}
	coordinatorB := &CoordinatorServer{
		workers: make(map[string]WorkerInfo), jobAttempts: make(map[int64]int64),
		leases: make(map[int64]JobLease), activeJobs: make(map[int64]struct{}),
		jobStore: storeB, recoveryReady: make(chan struct{}, 1),
	}
	if len(coordinatorB.workers) != 0 || len(coordinatorB.jobAttempts) != 0 ||
		len(coordinatorB.leases) != 0 || len(coordinatorB.activeJobs) != 0 {
		t.Fatal("Coordinator B did not start with empty in-memory state")
	}
	assertSQLiteJobRecord(t, storeB, running)
	if err := coordinatorB.recoverRunningJobs(); err != nil {
		t.Fatalf("mark persisted Running job as Recovering: %v", err)
	}
	recovering := running
	recovering.State = JobRecovering
	assertSQLiteJobRecord(t, storeB, recovering)
	if len(coordinatorB.jobAttempts) != 0 || len(coordinatorB.leases) != 0 {
		t.Fatal("startup marking allocated an attempt or lease")
	}

	terminalSaved := make(chan struct{}, 1)
	coordinatorB.jobStore = &recoveryPassStore{JobStore: storeB, save: func(record JobRecord) error {
		if err := storeB.Save(record); err != nil {
			return err
		}
		if record.JobID == jobID && record.State == JobSucceeded {
			terminalSaved <- struct{}{}
		}
		return nil
	}}
	loopCtx, cancelLoop := context.WithCancel(context.Background())
	loopDone := make(chan struct{})
	go func() { coordinatorB.runRecoveryLoop(loopCtx); close(loopDone) }()
	t.Cleanup(func() {
		cancelLoop()
		<-loopDone
		_ = storeB.Close()
	})
	client := &recordingCoordinatorClient{
		CoordinatorServiceClient: startTestCoordinator(t, coordinatorB),
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := workerheartbeat.Send(ctx, client, workerID, worker.Address); err != nil {
		t.Fatalf("worker heartbeat and re-registration: %v", err)
	}
	if len(client.heartbeatAccepted) != 1 || client.heartbeatAccepted[0] ||
		len(client.registrations) != 1 || client.registrations[0].WorkerId != workerID ||
		client.registrations[0].Address != worker.Address {
		t.Fatalf("unexpected worker re-registration: heartbeats=%v registrations=%v",
			client.heartbeatAccepted, client.registrations)
	}
	select {
	case <-terminalSaved:
	case <-ctx.Done():
		t.Fatal("registered worker did not finish recovered attempt")
	}
	want := running
	want.State, want.AttemptID = JobSucceeded, 2
	assertSQLiteJobRecord(t, storeB, want)
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || coordinatorB.jobAttempts[jobID] != 2 {
		t.Fatalf("wrong attempt counts: first=%d second=%d generation=%d",
			firstCalls.Load(), secondCalls.Load(), coordinatorB.jobAttempts[jobID])
	}
	lease, ok := coordinatorB.getLease(jobID)
	if !ok || lease.AttemptID != 2 || lease.WorkerID != workerID {
		t.Fatalf("recovered lease=%+v found=%v", lease, ok)
	}
	releaseOnce.Do(func() { close(releaseOld) })
	select {
	case <-oldReturned:
	case <-ctx.Done():
		t.Fatal("old worker did not finish late")
	}
	assertSQLiteJobRecord(t, storeB, want)
	cancelLoop()
	select {
	case <-loopDone:
	case <-ctx.Done():
		t.Fatal("recovery loop did not stop")
	}
	if firstCalls.Load() != 1 || secondCalls.Load() != 1 || coordinatorB.jobAttempts[jobID] != 2 {
		t.Fatal("late Attempt 1 caused an extra execution")
	}
	assertRecoveryOwnershipReleased(t, coordinatorB, jobID)
}
