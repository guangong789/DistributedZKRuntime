package main

import (
	"context"
	"fmt"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"time"
)

type WorkerStatus int

const (
	WorkerAlive WorkerStatus = iota
	WorkerDead
)

type WorkerInfo struct {
	ID       string
	Address  string
	LastSeen time.Time
	Status   WorkerStatus
}

func (s *CoordinatorServer) RegisterWorker(
	ctx context.Context,
	req *runtimepb.RegisterWorkerRequest,
) (*runtimepb.RegisterWorkerResponse, error) {

	if req.WorkerId == "" || req.Address == "" {
		return &runtimepb.RegisterWorkerResponse{
			Accepted: false,
			Error:    "worker_id and address are required",
		}, nil
	}

	s.mu.Lock()

	if s.workers == nil {
		s.workers = make(map[string]WorkerInfo)
	}

	previous, exists := s.workers[req.WorkerId]

	s.workers[req.WorkerId] = WorkerInfo{
		ID:       req.WorkerId,
		Address:  req.Address,
		LastSeen: time.Now(),
		Status:   WorkerAlive,
	}

	if !exists {
		s.workerOrder = append(s.workerOrder, req.WorkerId)
	}

	s.mu.Unlock()

	observer := metrics.Resolve(s.metrics)
	if !exists {
		observer.WorkerStateChanged(metrics.WorkerUnregistered, metrics.WorkerAlive, metrics.WorkerRegistration)
	} else if previous.Status == WorkerDead {
		observer.WorkerStateChanged(metrics.WorkerDead, metrics.WorkerAlive, metrics.WorkerRegistration)
	}

	// A nil or full channel disables this send; registration never waits for recovery.
	select {
	case s.recoveryReady <- struct{}{}:
	default:
	}

	fmt.Printf(
		"registered worker: id=%s address=%s\n",
		req.WorkerId,
		req.Address,
	)

	return &runtimepb.RegisterWorkerResponse{
		Accepted: true,
	}, nil
}

func (s *CoordinatorServer) getNextWorker() (WorkerInfo, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if len(s.workerOrder) == 0 {
		return WorkerInfo{}, false
	}

	for i := 0; i < len(s.workerOrder); i++ {
		workerID := s.workerOrder[s.nextWorker]

		s.nextWorker = (s.nextWorker + 1) % len(s.workerOrder)

		worker, ok := s.workers[workerID]
		if !ok {
			continue
		}

		if worker.Status != WorkerAlive {
			continue
		}

		return worker, true
	}

	return WorkerInfo{}, false
}

func (s *CoordinatorServer) Heartbeat(
	ctx context.Context,
	req *runtimepb.HeartbeatRequest,
) (*runtimepb.HeartbeatResponse, error) {
	s.mu.Lock()

	worker, ok := s.workers[req.WorkerId]
	if !ok {
		s.mu.Unlock()
		return &runtimepb.HeartbeatResponse{
			Accepted: false,
		}, nil
	}

	previousState := worker.Status
	worker.LastSeen = time.Now()
	worker.Status = WorkerAlive
	s.workers[req.WorkerId] = worker
	s.mu.Unlock()
	if previousState == WorkerDead {
		metrics.Resolve(s.metrics).WorkerStateChanged(metrics.WorkerDead, metrics.WorkerAlive, metrics.WorkerHeartbeat)
	}

	fmt.Printf(
		"heartbeat: worker=%s last_seen=%v\n",
		worker.ID,
		worker.LastSeen,
	)

	return &runtimepb.HeartbeatResponse{
		Accepted: true,
	}, nil
}

func (s *CoordinatorServer) checkWorkerLiveness(
	timeout time.Duration,
) {
	now := time.Now()

	s.mu.Lock()
	transitions := 0

	for id, worker := range s.workers {
		if now.Sub(worker.LastSeen) > timeout {
			if worker.Status != WorkerDead {
				transitions++
				fmt.Printf(
					"worker timed out: id=%s last_seen=%v\n",
					worker.ID,
					worker.LastSeen,
				)
			}

			worker.Status = WorkerDead
			s.workers[id] = worker
		}
	}
	s.mu.Unlock()
	observer := metrics.Resolve(s.metrics)
	for i := 0; i < transitions; i++ {
		observer.WorkerStateChanged(metrics.WorkerAlive, metrics.WorkerDead, metrics.WorkerTimeout)
	}
}

func (s *CoordinatorServer) runLivenessMonitor(
	checkInterval time.Duration,
	workerTimeout time.Duration,
) {
	ticker := time.NewTicker(checkInterval)
	defer ticker.Stop()

	for range ticker.C {
		s.checkWorkerLiveness(workerTimeout)
	}
}
