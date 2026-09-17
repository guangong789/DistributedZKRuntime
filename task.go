package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

type Task interface {
	Execute(ctx context.Context) (string, error)
}

type SleepTask struct {
	Duration time.Duration
}

func (t SleepTask) Execute(ctx context.Context) (string, error) {
	select {
	case <-time.After(t.Duration):
		return fmt.Sprintf("slept for %v", t.Duration), nil

	case <-ctx.Done():
		return "", ctx.Err()
	}
}

type HashTask struct {
	Input string
}

func (t HashTask) Execute(ctx context.Context) (string, error) {
	if t.Input == "" {
		return "", errors.New("hash input cannot be empty")
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()

	default:
	}

	sum := sha256.Sum256([]byte(t.Input))

	return hex.EncodeToString(sum[:]), nil
}
