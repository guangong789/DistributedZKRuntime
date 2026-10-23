package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"
)

// The demo targets Linux/WSL, like the GNU-based development PKI script.
// Each command has its own process group, including bootstrap/build descendants.
type child struct {
	cmd    *exec.Cmd
	done   chan struct{}
	err    error // Published by closing done.
	log    *os.File
	output *readiness
}

type readiness struct {
	mu                     sync.Mutex
	prefix, pending, value string
	ready                  chan struct{}
}

func (r *readiness) Write(p []byte) (int, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.value != "" || r.prefix == "" {
		return len(p), nil
	}
	r.pending += string(p)
	for {
		line, rest, ok := strings.Cut(r.pending, "\n")
		if !ok {
			break
		}
		r.pending = rest
		if strings.HasPrefix(line, r.prefix) {
			r.value = line
			close(r.ready)
			return len(p), nil
		}
	}
	if len(r.pending) > 64<<10 {
		return 0, errors.New("child log line exceeds readiness bound")
	}
	return len(p), nil
}

func startChild(repo, logPath, prefix, program string, args ...string) (*child, error) {
	log, err := os.OpenFile(logPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0600)
	if err != nil {
		return nil, err
	}
	r := &readiness{prefix: prefix, ready: make(chan struct{})}
	cmd := exec.Command(program, args...)
	cmd.Dir = repo
	cmd.Env = offlineEnv()
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
	cmd.Stdout = io.MultiWriter(log, r)
	cmd.Stderr = cmd.Stdout
	cmd.WaitDelay = time.Second
	if err := cmd.Start(); err != nil {
		log.Close()
		return nil, err
	}
	p := &child{cmd: cmd, done: make(chan struct{}), log: log, output: r}
	go func() { p.err = cmd.Wait(); close(p.done) }()
	return p, nil
}

func (p *child) waitReady(ctx context.Context) (string, error) {
	select {
	case <-p.done:
		return "", fmt.Errorf("child exited before readiness: %v", p.err)
	case <-ctx.Done():
		return "", ctx.Err()
	case <-p.output.ready:
		if err := p.alive(); err != nil {
			return "", err
		}
		p.output.mu.Lock()
		defer p.output.mu.Unlock()
		return p.output.value, nil
	}
}

func (p *child) alive() error {
	select {
	case <-p.done:
		return fmt.Errorf("child exited unexpectedly: %v", p.err)
	default:
		return nil
	}
}

func (p *child) groupGone() bool {
	return errors.Is(syscall.Kill(-p.cmd.Process.Pid, 0), syscall.ESRCH)
}

// stop always reaps the direct child. Escalation is bounded to its owned group.
// A forced kill or a child that exited before shutdown makes the demo fail.
func (p *child) stop() error {
	err := p.alive()
	if !p.groupGone() {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGINT)
	}
	timer := time.NewTimer(7 * time.Second)
	defer timer.Stop()
	select {
	case <-p.done:
	case <-timer.C:
		err = errors.Join(err, errors.New("child exceeded graceful shutdown budget"))
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		select {
		case <-p.done:
		case <-time.After(3 * time.Second):
			return errors.Join(err, errors.New("child could not be reaped"))
		}
	}
	if !p.groupGone() {
		_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
		err = errors.Join(err, errors.New("owned process group still present after child exit"))
	}
	return errors.Join(err, p.err, p.log.Close())
}

func runTool(ctx context.Context, repo, logPath, program string, args ...string) error {
	p, err := startChild(repo, logPath, "", program, args...)
	if err != nil {
		return err
	}
	select {
	case <-ctx.Done():
		return errors.Join(ctx.Err(), p.stop())
	case <-p.done:
		err = p.err
		if !p.groupGone() {
			_ = syscall.Kill(-p.cmd.Process.Pid, syscall.SIGKILL)
			err = errors.Join(err, errors.New("tool left an owned process group behind"))
		}
		return errors.Join(err, p.log.Close())
	}
}
