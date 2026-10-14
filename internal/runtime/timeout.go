package runtime

import (
	"context"
	"fmt"
	"math"
	"time"
)

const maxTaskTimeoutMs = math.MaxInt64 / int64(time.Millisecond)

// TaskTimeoutFromMilliseconds validates before multiplication. Zero is not a
// default: RPCs and persisted specs must carry an explicit positive task budget.
func TaskTimeoutFromMilliseconds(milliseconds int64) (time.Duration, error) {
	if milliseconds < 1 || milliseconds > maxTaskTimeoutMs {
		return 0, fmt.Errorf("timeout_ms must be between 1 and %d", maxTaskTimeoutMs)
	}
	return time.Duration(milliseconds) * time.Millisecond, nil
}

// resultContextError is the local success-finalization checkpoint. The explicit
// time sample also catches expiration before the context timer callback runs.
// Cancellation after this checkpoint does not retroactively revise the result;
// response delivery and Coordinator terminal persistence are separate decisions.
func resultContextError(ctx context.Context, now time.Time) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if deadline, ok := ctx.Deadline(); ok && !now.Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
