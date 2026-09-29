package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
)

type PreimageProofVerifier interface {
	Verify(proofBytes []byte, digest fr.Element) error
}

type zkPreimagePublicInput struct {
	Digest string `json:"digest"`
}

func verifyZKPreimageResult(
	verifier PreimageProofVerifier,
	payload string,
	output string,
) error {
	if verifier == nil {
		return fmt.Errorf("preimage verifier is not configured")
	}

	var publicInput zkPreimagePublicInput

	if err := json.Unmarshal([]byte(payload), &publicInput); err != nil {
		return fmt.Errorf("decode preimage public input: %w", err)
	}

	var digestInt big.Int

	if _, ok := digestInt.SetString(publicInput.Digest, 10); !ok {
		return fmt.Errorf("invalid preimage digest: %q", publicInput.Digest)
	}

	var digest fr.Element
	digest.SetBigInt(&digestInt)

	proofBytes, err := base64.StdEncoding.DecodeString(output)
	if err != nil {
		return fmt.Errorf("decode preimage proof: %w", err)
	}

	if err := verifier.Verify(proofBytes, digest); err != nil {
		return fmt.Errorf("verify preimage proof: %w", err)
	}

	return nil
}
