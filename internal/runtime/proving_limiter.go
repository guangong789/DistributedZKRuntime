package runtime

import (
	"errors"
	"fmt"
)

var ErrProvingBusy = errors.New("proving capacity is full")

// ProvingLimiter bounds actual synchronous proving calls, with no waiting queue.
type ProvingLimiter struct {
	slots chan struct{}
}

func NewProvingLimiter(limit int) (*ProvingLimiter, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("maximum concurrent proofs must be positive: %d", limit)
	}
	return &ProvingLimiter{slots: make(chan struct{}, limit)}, nil
}

func (l *ProvingLimiter) TryAcquire() bool {
	select {
	case l.slots <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release must be called exactly once for each successful TryAcquire.
func (l *ProvingLimiter) Release() { <-l.slots }
