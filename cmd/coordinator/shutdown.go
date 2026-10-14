package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Admission has its own lock; it must never wait while holding Coordinator.mu.
// done closes only after admission is sealed and the last handler has left.
type admittedCoordinatorRPC struct{ cancel context.CancelFunc }

type coordinatorLifecycle struct {
	ctx      context.Context
	cancel   context.CancelFunc
	mu       sync.Mutex
	draining bool
	active   map[*admittedCoordinatorRPC]struct{}
	done     chan struct{}
}

func newCoordinatorLifecycle() *coordinatorLifecycle {
	ctx, cancel := context.WithCancel(context.Background())
	return &coordinatorLifecycle{ctx: ctx, cancel: cancel,
		active: make(map[*admittedCoordinatorRPC]struct{}), done: make(chan struct{})}
}

func (l *coordinatorLifecycle) tryEnter(ctx context.Context) (context.Context, func(), bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.draining {
		return nil, nil, false
	}
	// Derive from the authenticated RPC context, retaining its values/deadline.
	requestCtx, cancel := context.WithCancel(ctx)
	entry := &admittedCoordinatorRPC{cancel}
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

func (l *coordinatorLifecycle) beginShutdown() {
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

func (l *coordinatorLifecycle) cancelWork() {
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

func (l *coordinatorLifecycle) unaryInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		requestCtx, leave, ok := l.tryEnter(ctx)
		if !ok {
			return nil, status.Error(codes.Unavailable, "coordinator is shutting down")
		}
		defer leave()
		if err := requestCtx.Err(); err != nil {
			return nil, err
		}
		return handler(requestCtx, req)
	}
}

var errCoordinatorShutdownIncomplete = errors.New("coordinator shutdown incomplete; forced process exit required")

// startupDone publishes resources/producers before shutdown reads or joins them.
// Standalone users configure them before shutdown. Only this owner closes the
// store; startup failures use the same protocol with partial resources.
type coordinatorProcess struct {
	service         *CoordinatorServer
	server          *grpc.Server
	listener        net.Listener
	background      sync.WaitGroup
	serveResult     chan error
	serveDone       chan struct{}
	startupDone     <-chan struct{}
	shutdownMetrics func(context.Context) error
	closeStore      func() error
	timeout         time.Duration
	shutdownOnce    sync.Once
	shutdownDone    chan struct{}
	cleanupDone     chan struct{}
	shutdownErr     error
}

func newCoordinatorProcess(service *CoordinatorServer, timeout time.Duration) *coordinatorProcess {
	if service.lifecycle == nil {
		service.lifecycle = newCoordinatorLifecycle()
	}
	return &coordinatorProcess{service: service, timeout: timeout, shutdownDone: make(chan struct{}), cleanupDone: make(chan struct{})}
}

// start registers runtime producers before startupDone closes. Without a startup
// producer, the caller must finish start before invoking shutdown.
func (p *coordinatorProcess) start(server *grpc.Server, listener net.Listener) {
	p.server, p.listener = server, listener
	p.serveResult, p.serveDone = make(chan error, 1), make(chan struct{})
	p.startBackground()
	go func() {
		defer close(p.serveDone)
		p.serveResult <- server.Serve(listener)
	}()
}

func (p *coordinatorProcess) startBackground() {
	p.background.Add(2)
	go func() {
		defer p.background.Done()
		p.service.runLivenessMonitor(p.service.lifecycle.ctx, time.Second, 3*time.Second)
	}()
	go func() {
		defer p.background.Done()
		p.service.runRecoveryLoop(p.service.lifecycle.ctx)
	}()
}

func (p *coordinatorProcess) shutdown() error {
	p.shutdownOnce.Do(func() {
		// One deadline includes transport, application joins and resource cleanup.
		ctx, cancel := context.WithTimeout(context.Background(), p.timeout)
		p.service.lifecycle.beginShutdown()
		p.service.lifecycle.cancelWork()
		go func() {
			defer close(p.shutdownDone)
			defer cancel()
			stopped := make(chan struct{})
			go func() {
				defer close(stopped)
				if p.startupDone != nil {
					<-p.startupDone
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
				if p.startupDone != nil {
					<-p.startupDone
				}
				// Both dispatch producers must finish before dispatch Wait: no Add
				// can race Wait once handlers and the recovery loop have exited.
				<-p.service.lifecycle.done
				p.background.Wait()
				p.service.dispatchWG.Wait()
				<-stopped
				if p.serveDone != nil {
					<-p.serveDone
				}
				if err := ctx.Err(); err != nil {
					complete <- err
					return // Never resume ordinary cleanup after a timeout decision.
				}
				if p.shutdownMetrics != nil {
					if err := p.shutdownMetrics(ctx); err != nil {
						complete <- fmt.Errorf("metrics shutdown: %w", err)
						return
					}
				}
				if err := ctx.Err(); err != nil {
					complete <- err
					return
				}
				if p.closeStore != nil {
					if err := p.closeStore(); err != nil {
						complete <- fmt.Errorf("close job store: %w", err)
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
				p.shutdownErr = errors.Join(errCoordinatorShutdownIncomplete, p.shutdownErr)
			}
		}()
	})
	<-p.shutdownDone
	return p.shutdownErr
}
