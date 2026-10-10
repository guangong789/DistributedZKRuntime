package runtime

import (
	"context"
	"encoding/base64"
	"fmt"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

type PreimageProver interface {
	Prove(secret int64, digest fr.Element) ([]byte, error)
}

type ZKPreimagePayload = zk.PreimagePublicPayload

type ZKPreimageTask struct {
	Payload      string
	Prover       PreimageProver
	WitnessStore WitnessStore
	// Optional for standalone tasks; Worker injects its shared process limiter.
	Limiter *ProvingLimiter
	// Optional observer; configure before executing the task.
	Metrics metrics.Metrics
}

func (t ZKPreimageTask) Execute(ctx context.Context) (string, error) {
	payload, err := zk.ParsePreimagePublicPayload(t.Payload)
	if err != nil {
		return "", err
	}
	secret, ok := t.WitnessStore.Get(payload.WitnessRef)
	if !ok {
		return "", fmt.Errorf("witness not found")
	}

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}
	if t.Limiter != nil {
		if !t.Limiter.TryAcquire() {
			return "", ErrProvingBusy
		}
		// Cancellation cannot release capacity while synchronous Prove runs.
		defer t.Limiter.Release()
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}

	start := time.Now()
	proofBytes, err := t.Prover.Prove(secret, payload.Digest)
	metrics.Resolve(t.Metrics).ObserveZKProving(time.Since(start))
	if err != nil {
		return "", fmt.Errorf("prove preimage: %w", err)
	}

	return base64.StdEncoding.EncodeToString(proofBytes), nil
}
