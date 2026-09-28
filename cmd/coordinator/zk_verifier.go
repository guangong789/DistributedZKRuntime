package main

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type SquareProofVerifier interface {
	Verify(proofBytes []byte, y int64) error
}

type zkSquarePublicInput struct {
	Y int64 `json:"y"`
}

func verifyZKSquareResult(
	verifier SquareProofVerifier,
	payload string,
	output string,
) error {
	if verifier == nil {
		return fmt.Errorf("square verifier is not configured")
	}

	var publicInput zkSquarePublicInput

	if err := json.Unmarshal(
		[]byte(payload),
		&publicInput,
	); err != nil {
		return fmt.Errorf(
			"decode zk square public input: %w",
			err,
		)
	}

	proofBytes, err := base64.StdEncoding.DecodeString(output)
	if err != nil {
		return fmt.Errorf(
			"decode zk square proof: %w",
			err,
		)
	}

	if err := verifier.Verify(
		proofBytes,
		publicInput.Y,
	); err != nil {
		return fmt.Errorf(
			"verify zk square proof: %w",
			err,
		)
	}

	return nil
}
