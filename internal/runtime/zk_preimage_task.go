package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
)

type PreimageProver interface {
	Prove(secret int64, digest fr.Element) ([]byte, error)
}

type ZKPreimagePayload struct {
	WitnessRef string `json:"witness_ref"`
	Digest     string `json:"digest"`
}

type ZKPreimageTask struct {
	Payload      string
	Prover       PreimageProver
	WitnessStore WitnessStore
	// Optional observer; configure before executing the task.
	Metrics metrics.Metrics
}

func (t ZKPreimageTask) Execute(ctx context.Context) (string, error) {
	var payload ZKPreimagePayload

	if err := json.Unmarshal([]byte(t.Payload), &payload); err != nil {
		return "", fmt.Errorf("decode preimage payload: %w", err)
	}

	secret, ok := t.WitnessStore.Get(payload.WitnessRef)
	if !ok {
		return "", fmt.Errorf("witness not found: %s", payload.WitnessRef)
	}

	var digestInt big.Int

	if _, ok := digestInt.SetString(payload.Digest, 10); !ok {
		return "", fmt.Errorf("invalid digest: %q", payload.Digest)
	}

	var digest fr.Element
	digest.SetBigInt(&digestInt)

	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	start := time.Now()
	proofBytes, err := t.Prover.Prove(secret, digest)
	metrics.Resolve(t.Metrics).ObserveZKProving(time.Since(start))
	if err != nil {
		return "", fmt.Errorf("prove preimage: %w", err)
	}

	return base64.StdEncoding.EncodeToString(proofBytes), nil
}
