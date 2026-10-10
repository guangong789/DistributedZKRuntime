package main

import (
	"encoding/base64"
	"fmt"
	"time"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/metrics"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

type PreimageProofVerifier interface {
	Verify(proofBytes []byte, digest fr.Element) error
}

func verifyZKPreimageResult(
	verifier PreimageProofVerifier,
	payload string,
	output string,
	observer metrics.Metrics,
) error {
	if verifier == nil {
		return fmt.Errorf("preimage verifier is not configured")
	}

	publicInput, err := zk.ParsePreimagePublicPayload(payload)
	if err != nil {
		return err
	}

	proofBytes, err := base64.StdEncoding.DecodeString(output)
	if err != nil {
		return fmt.Errorf("decode preimage proof: %w", err)
	}

	start := time.Now()
	err = verifier.Verify(proofBytes, publicInput.Digest)
	metrics.Resolve(observer).ObserveZKVerification(time.Since(start))
	if err != nil {
		return fmt.Errorf("verify preimage proof: %w", err)
	}

	return nil
}
