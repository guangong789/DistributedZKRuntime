package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type ZKSquarePayload struct {
	X int64 `json:"x"`
	Y int64 `json:"y"`
}

type SquareProver interface {
	Prove(x, y int64) ([]byte, error)
}

type ZKSquareTask struct {
	Payload string
	Prover  SquareProver
}

func (t ZKSquareTask) Execute(ctx context.Context) (string, error) {
	if err := ctx.Err(); err != nil {
		return "", err
	}

	var input ZKSquarePayload

	if err := json.Unmarshal([]byte(t.Payload), &input); err != nil {
		return "", fmt.Errorf("decode zk square payload: %w", err)
	}

	if err := ctx.Err(); err != nil {
		return "", err
	}

	if t.Prover == nil {
		return "", fmt.Errorf("square prover is not configured")
	}

	proof, err := t.Prover.Prove(input.X, input.Y)
	if err != nil {
		return "", fmt.Errorf("prove square: %w", err)
	}

	return base64.StdEncoding.EncodeToString(proof), nil
}
