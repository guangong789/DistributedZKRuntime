package runtime

import (
	"context"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
)

func TestTaskTimeoutFromMilliseconds(t *testing.T) {
	for _, milliseconds := range []int64{math.MinInt64, -1, 0, 1, 5000, 9223372036854, 9223372036855, 1 << 58, 1<<58 + 1, math.MaxInt64} {
		t.Run(fmt.Sprint(milliseconds), func(t *testing.T) {
			got, err := TaskTimeoutFromMilliseconds(milliseconds)
			valid := milliseconds >= 1 && milliseconds <= 9223372036854
			if !valid {
				if err == nil || got != 0 {
					t.Fatalf("unsafe conversion: duration=%v error=%v", got, err)
				}
			} else if err != nil || int64(got)/int64(time.Millisecond) != milliseconds || got%time.Millisecond != 0 {
				t.Fatalf("inexact conversion: duration=%v error=%v", got, err)
			}
		})
	}
}

// Simulate a delayed context timer callback: Deadline is fixed and Err stays nil.
type pendingDeadlineContext struct {
	context.Context
	deadline time.Time
}

func (c pendingDeadlineContext) Deadline() (time.Time, bool) { return c.deadline, true }

func TestResultContextErrorDeadlineBoundary(t *testing.T) {
	deadline := time.Now()
	ctx := pendingDeadlineContext{context.Background(), deadline}
	for _, offset := range []time.Duration{-time.Nanosecond, 0, time.Nanosecond} {
		err := resultContextError(ctx, deadline.Add(offset))
		if offset < 0 && err != nil || offset >= 0 && !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("offset=%v error=%v (Err=%v)", offset, err, ctx.Err())
		}
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := resultContextError(cancelled, deadline); !errors.Is(err, context.Canceled) {
		t.Fatalf("observed cancellation must retain precedence: %v", err)
	}
}

type finalizationTask func(context.Context) (string, error)

func (f finalizationTask) Execute(ctx context.Context) (string, error) { return f(ctx) }

func TestExecuteJobStrictResultDeadline(t *testing.T) {
	failure := errors.New("task failed")
	for _, name := range []string{"live", "expired with pending timer", "cancel during task", "non-context error after expiry", "wrapped context error"} {
		t.Run(name, func(t *testing.T) {
			var ctx context.Context = context.Background()
			want, wantErr, output := JobSucceeded, error(nil), "output"
			var executionErr error
			cancel := func() {}
			switch name {
			case "expired with pending timer", "non-context error after expiry":
				ctx = pendingDeadlineContext{ctx, time.Now().Add(-time.Second)}
				want, wantErr, output = JobCancelled, context.DeadlineExceeded, ""
				if name == "non-context error after expiry" {
					want, executionErr, wantErr = JobFailed, failure, failure
				}
			case "cancel during task":
				ctx, cancel = context.WithCancel(ctx)
				defer cancel()
				want, wantErr, output = JobCancelled, context.Canceled, ""
			case "wrapped context error":
				executionErr = fmt.Errorf("wrapped: %w", context.Canceled)
				want, wantErr, output = JobCancelled, context.Canceled, ""
			}
			task := finalizationTask(func(context.Context) (string, error) { cancel(); return "output", executionErr })
			result := ExecuteJob(ctx, Job{ID: 10, Task: task})
			if result.JobID != 10 || result.Status != want || result.Output != output || !errors.Is(result.Err, wantErr) {
				t.Fatalf("result=%+v want status=%v error=%v", result, want, wantErr)
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	result := ExecuteJob(ctx, Job{Task: finalizationTask(func(context.Context) (string, error) { return "committed", nil })})
	cancel()
	if result.Status != JobSucceeded || result.Output != "committed" || result.Err != nil {
		t.Fatal("post-finalization cancellation revised a completed local result")
	}
}

type finalizationMetrics struct {
	metrics.NoopMetrics
	observe func()
}

func (m finalizationMetrics) ObserveZKProving(time.Duration) { m.observe() }

func TestZKPreimageTaskDeadlineBetweenProveAndOutputFinalization(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	store := NewMemoryWitnessStore()
	store.Put("ref", 42)
	limiter, err := NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	calls, samples := 0, 0
	task := ZKPreimageTask{
		Payload: preimageTaskPayload(t, "ref", "123"), WitnessStore: store, Limiter: limiter,
		Prover: metricTestProver{prove: func(int64, fr.Element) ([]byte, error) {
			calls++
			if ctx.Err() != nil {
				t.Fatal("Prove must complete before expiry")
			}
			return []byte("proof"), nil
		}},
		Metrics: finalizationMetrics{observe: func() {
			samples++
			// The production observation is after Prove and before encoding.
			// Hold this checkpoint until the actual deadline, without sleeping.
			<-ctx.Done()
			if limiter.TryAcquire() {
				limiter.Release()
				t.Fatal("premature permit release")
			}
		}},
	}
	output, err := task.Execute(ctx)
	if output != "" || !errors.Is(err, context.DeadlineExceeded) || calls != 1 || samples != 1 {
		t.Fatalf("output=%q error=%v calls=%d samples=%d", output, err, calls, samples)
	}
	if !limiter.TryAcquire() {
		t.Fatal("finalization leaked permit")
	}
	limiter.Release()
}
