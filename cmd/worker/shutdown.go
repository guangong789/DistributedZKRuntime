package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	runtimepb "github.com/guangong789/DistributedZKRuntime/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const defaultWorkerShutdownTimeout = 30 * time.Second

type admittedWorkerRPC struct{ cancel context.CancelFunc }

// The gate owns request cancellation and completion, independently of gRPC.
// done closes only after admission is sealed and the last handler has returned.
type workerLifecycle struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	draining bool
	active   map[*admittedWorkerRPC]struct{}
	done     chan struct{}
}

func newWorkerLifecycle() *workerLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &workerLifecycle{ctx: ctx, cancel: cancel, active: make(map[*admittedWorkerRPC]struct{}), done: make(chan struct{})}
}

func (l *workerLifecycle) tryEnter(ctx context.Context) (context.Context, func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining {
		return nil, nil, false
	}
	requestCtx, cancel := context.WithCancel(ctx)
	entry := &admittedWorkerRPC{cancel: cancel}
	l.active[entry] = struct{}{}
	var once sync.Once
	leave := func() {
		once.Do(func() {
			cancel()
			l.mu.Lock()
			defer l.mu.Unlock()
			delete(l.active, entry)
			if l.draining && len(l.active) == 0 {
				close(l.done)
			}
		})
	}
	return requestCtx, leave, true
}

func (l *workerLifecycle) beginShutdown() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining {
		return
	}
	l.draining = true
	if len(l.active) == 0 {
		close(l.done)
	}
}

func (l *workerLifecycle) cancelWork() {
	l.cancel()
	l.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(l.active))
	for entry := range l.active {
		cancels = append(cancels, entry.cancel)
	}
	l.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}

// The existing rpcauth interceptor authenticates/authorizes before this wrapper.
// A pre-admitted task may pass its last cancellation check and enter synchronous
// Prove while draining. Its token remains owned until the entire handler exits.
type admittedWorkerService struct {
	runtimepb.WorkerServiceServer
	lifecycle *workerLifecycle
}

func (s *admittedWorkerService) ExecuteJob(ctx context.Context, req *runtimepb.ExecuteJobRequest) (*runtimepb.ExecuteJobResponse, error) {
	requestCtx, leave, ok := s.lifecycle.tryEnter(ctx)
	if !ok {
		return nil, status.Error(codes.Unavailable, "worker is shutting down")
	}
	defer leave()
	if err := requestCtx.Err(); err != nil {
		return nil, err
	}
	return s.WorkerServiceServer.ExecuteJob(requestCtx, req)
}

var errWorkerShutdownIncomplete = errors.New("worker shutdown incomplete; forced process exit required")

// resourcesReady publishes startup's resource fields before Stop/cleanup read
// them. backgroundDone owns initialization, registration and then heartbeat.
// No producer can appear after backgroundDone closes; no WaitGroup Add/Wait race.
type workerProcess struct {
	lifecycle       *workerLifecycle
	server          *grpc.Server
	listener        net.Listener
	resourcesReady  <-chan struct{}
	backgroundDone  <-chan struct{}
	serveResult     chan error
	serveDone       chan struct{}
	shutdownMetrics func(context.Context) error
	timeout         time.Duration
	shutdownOnce    sync.Once
	shutdownDone    chan struct{}
	cleanupDone     chan struct{}
	shutdownErr     error
}

func newWorkerProcess(timeout time.Duration) *workerProcess {
	return &workerProcess{lifecycle: newWorkerLifecycle(), timeout: timeout,
		serveResult: make(chan error, 1), shutdownDone: make(chan struct{}), cleanupDone: make(chan struct{})}
}

func (p *workerProcess) start(server *grpc.Server, listener net.Listener) {
	p.server, p.listener = server, listener
	p.serveDone = make(chan struct{})
	go func() {
		defer close(p.serveDone)
		p.serveResult <- server.Serve(listener)
	}()
}

func (p *workerProcess) shutdown() error {
	p.shutdownOnce.Do(func() {
		// Independent of task/RPC deadlines. Blocking cleanup is supervised too.
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		p.lifecycle.beginShutdown()
		p.lifecycle.cancelWork()
		go func() {
			defer close(p.shutdownDone)
			defer cancel()
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				if p.resourcesReady != nil {
					<-p.resourcesReady
				}
				if p.server != nil {
					p.server.Stop()
				} else if p.listener != nil {
					_ = p.listener.Close()
				}
			}()
			complete := make(chan error, 1)
			go func() {
				defer close(p.cleanupDone)
				if p.resourcesReady != nil {
					<-p.resourcesReady
				}
				if p.backgroundDone != nil {
					<-p.backgroundDone
				}
				<-p.lifecycle.done
				<-stopped
				if p.serveDone != nil {
					<-p.serveDone
				}
				if err := ctx.Err(); err != nil {
					complete <- err
					return
				}
				if p.shutdownMetrics != nil {
					if err := p.shutdownMetrics(ctx); err != nil {
						complete <- fmt.Errorf("metrics shutdown: %w", err)
						return
					}
				}
				complete <- ctx.Err()
			}()
			select {
			case err := <-complete:
				p.shutdownErr = errors.Join(err, ctx.Err())
				select {
				case <-p.cleanupDone:
				case <-ctx.Done():
					p.shutdownErr = errors.Join(p.shutdownErr, ctx.Err())
				}
			case <-ctx.Done():
				p.shutdownErr = ctx.Err()
			}
			if p.shutdownErr != nil {
				p.shutdownErr = errors.Join(errWorkerShutdownIncomplete, p.shutdownErr)
			}
		}()
	})
	<-p.shutdownDone
	return p.shutdownErr
}
