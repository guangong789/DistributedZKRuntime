package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
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

	proofBytes, err := t.Prover.Prove(secret, digest)
	if err != nil {
		return "", fmt.Errorf("prove preimage: %w", err)
	}

	return base64.StdEncoding.EncodeToString(proofBytes), nil
}
