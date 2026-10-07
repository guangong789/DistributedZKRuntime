package runtime

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
)

type provingMetrics struct {
	metrics.NoopMetrics
	durations chan time.Duration
}

func (m *provingMetrics) ObserveZKProving(d time.Duration) { m.durations <- d }

type metricTestProver struct {
	prove func(int64, fr.Element) ([]byte, error)
}

func (p metricTestProver) Prove(secret int64, digest fr.Element) ([]byte, error) {
	return p.prove(secret, digest)
}

func TestPreimageProvingMetricsOnlyAroundActualProve(t *testing.T) {
	for _, scenario := range []string{"success", "prover error", "cancelled", "missing witness", "invalid digest", "malformed JSON"} {
		t.Run(scenario, func(t *testing.T) {
			observer := &provingMetrics{durations: make(chan time.Duration, 1)}
			store := NewMemoryWitnessStore()
			store.Put("secret-001", 42)
			calls := 0
			proverErr := errors.New("injected prove failure")
			task := ZKPreimageTask{
				Payload: preimageTaskPayload(t, "secret-001", "123"), WitnessStore: store, Metrics: observer,
				Prover: metricTestProver{prove: func(int64, fr.Element) ([]byte, error) {
					calls++
					if len(observer.durations) != 0 {
						t.Error("proving duration emitted before Prove")
					}
					if scenario == "prover error" {
						return nil, proverErr
					}
					return []byte("proof"), nil
				}},
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch scenario {
			case "cancelled":
				cancel()
			case "missing witness":
				task.Payload = preimageTaskPayload(t, "missing", "123")
			case "invalid digest":
				task.Payload = preimageTaskPayload(t, "secret-001", "invalid")
			case "malformed JSON":
				task.Payload = "{"
			}
			_, err := task.Execute(ctx)
			if scenario == "success" && err != nil {
				t.Fatal(err)
			}
			if scenario == "prover error" && !errors.Is(err, proverErr) {
				t.Fatalf("prover error=%v", err)
			}
			if scenario != "success" && err == nil {
				t.Fatal("expected task error")
			}
			want := 0
			if scenario == "success" || scenario == "prover error" {
				want = 1
			}
			if calls != want || len(observer.durations) != want {
				t.Fatalf("Prove calls=%d duration samples=%d, want %d", calls, len(observer.durations), want)
			}
			if want != 0 && <-observer.durations < 0 {
				t.Fatal("negative proving duration")
			}
		})
	}
}
