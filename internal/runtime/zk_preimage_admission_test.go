package runtime

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
)

func TestPreimageProvingCapacitySurvivesCancellation(t *testing.T) {
	for _, capacity := range []int{1, 2} {
		t.Run(fmt.Sprintf("capacity %d", capacity), func(t *testing.T) {
			limiter, err := NewProvingLimiter(capacity)
			if err != nil {
				t.Fatal(err)
			}
			store := NewMemoryWitnessStore()
			store.Put("secret-001", 42)
			entered := make(chan struct{}, capacity)
			release := make(chan struct{})
			var releaseOnce sync.Once
			var calls, active, maximum atomic.Int32
			observer := &provingMetrics{durations: make(chan time.Duration, capacity+1)}
			task := ZKPreimageTask{
				Payload: preimageTaskPayload(t, "secret-001", "123"), WitnessStore: store, Limiter: limiter, Metrics: observer,
				Prover: metricTestProver{prove: func(int64, fr.Element) ([]byte, error) {
					calls.Add(1)
					n := active.Add(1)
					defer active.Add(-1)
					for old := maximum.Load(); n > old; old = maximum.Load() {
						if maximum.CompareAndSwap(old, n) {
							break
						}
					}
					entered <- struct{}{}
					<-release
					return []byte("controlled proof"), nil
				}},
			}
			parent, cancel := context.WithCancel(context.Background())
			var wg sync.WaitGroup
			t.Cleanup(func() {
				cancel()
				releaseOnce.Do(func() { close(release) })
				joined := make(chan struct{})
				go func() { wg.Wait(); close(joined) }()
				select {
				case <-joined:
				case <-time.After(5 * time.Second):
					t.Error("proving calls did not finish after release")
				}
			})
			results := make(chan error, capacity)
			for i := 0; i < capacity; i++ {
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := task.Execute(parent)
					results <- err
				}()
			}
			waitCtx, stopWaiting := context.WithTimeout(context.Background(), 5*time.Second)
			defer stopWaiting()
			for i := 0; i < capacity; i++ {
				select {
				case <-entered:
				case <-waitCtx.Done():
					t.Fatal("admitted prover did not start")
				}
			}
			assertBusy := func() {
				t.Helper()
				busy := make(chan error, 1)
				wg.Add(1)
				go func() {
					defer wg.Done()
					_, err := task.Execute(context.Background())
					busy <- err
				}()
				select {
				case err := <-busy:
					if !errors.Is(err, ErrProvingBusy) {
						t.Fatalf("full capacity returned %v, want busy", err)
					}
				case <-waitCtx.Done():
					t.Fatal("busy request queued instead of rejecting immediately")
				}
				if calls.Load() != int32(capacity) || len(observer.durations) != 0 {
					t.Fatal("busy admission entered Prove or emitted proving duration")
				}
			}
			assertBusy()
			cancel()
			assertBusy()
			select {
			case <-results:
				t.Fatal("cancellation detached a still-running synchronous prover")
			default:
			}
			releaseOnce.Do(func() { close(release) })
			for i := 0; i < capacity; i++ {
				select {
				case err := <-results:
					// Completion releases capacity, but cancellation discards the proof.
					if !errors.Is(err, context.Canceled) {
						t.Fatalf("released synchronous prover returned %v, want cancellation", err)
					}
				case <-waitCtx.Done():
					t.Fatal("prover did not return after release")
				}
			}
			if _, err := task.Execute(context.Background()); err != nil {
				t.Fatalf("slot not reusable after Prove returned: %v", err)
			}
			if maximum.Load() != int32(capacity) || active.Load() != 0 || calls.Load() != int32(capacity+1) ||
				len(observer.durations) != capacity+1 {
				t.Fatalf("maximum=%d active=%d calls=%d samples=%d", maximum.Load(), active.Load(), calls.Load(), len(observer.durations))
			}
			t.Logf("maximum simultaneous Prove calls=%d; busy requests emitted no proving samples", maximum.Load())
		})
	}
}

func TestPreimageProvingSlotReleasedOnError(t *testing.T) {
	limiter, err := NewProvingLimiter(1)
	if err != nil {
		t.Fatal(err)
	}
	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	want := errors.New("prover failed")
	task := ZKPreimageTask{
		Payload: preimageTaskPayload(t, "secret-001", "123"), WitnessStore: store, Limiter: limiter,
		Prover: &countingPreimageProver{err: want},
	}
	if _, err := task.Execute(context.Background()); !errors.Is(err, want) {
		t.Fatalf("prover error not propagated: %v", err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("prover error leaked slot")
	}
	limiter.Release()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := task.Execute(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation before Prove not preserved: %v", err)
	}
	if !limiter.TryAcquire() {
		t.Fatal("pre-Prove cancellation leaked slot")
	}
	limiter.Release()
}
