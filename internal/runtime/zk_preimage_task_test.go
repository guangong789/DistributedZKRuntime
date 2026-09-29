package runtime

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"math/big"
	"path/filepath"
	"strings"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

type countingPreimageProver struct {
	calls int
	err   error
}

func (p *countingPreimageProver) Prove(int64, fr.Element) ([]byte, error) {
	p.calls++
	if p.err != nil {
		return nil, p.err
	}
	return []byte("proof"), nil
}

func preimageTaskPayload(t *testing.T, ref, digest string) string {
	t.Helper()
	data, err := json.Marshal(ZKPreimagePayload{
		WitnessRef: ref,
		Digest:     digest,
	})
	if err != nil {
		t.Fatalf("marshal preimage payload: %v", err)
	}
	return string(data)
}

func decimalPreimageDigest(digest fr.Element) string {
	var value big.Int
	digest.BigInt(&value)
	return value.String()
}

func TestZKPreimageTaskRealProof(t *testing.T) {
	dir := t.TempDir()
	provingKeyPath := filepath.Join(dir, "proving.key")
	verifyingKeyPath := filepath.Join(dir, "verifying.key")
	if err := zk.GeneratePreimageSetup(provingKeyPath, verifyingKeyPath); err != nil {
		t.Fatalf("generate preimage setup: %v", err)
	}
	prover, err := zk.LoadPreimageProver(provingKeyPath)
	if err != nil {
		t.Fatalf("load preimage prover: %v", err)
	}
	verifier, err := zk.LoadPreimageVerifier(verifyingKeyPath)
	if err != nil {
		t.Fatalf("load preimage verifier: %v", err)
	}

	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	digest := zk.ComputePreimageDigest(42)
	task := ZKPreimageTask{
		Payload:      preimageTaskPayload(t, "secret-001", decimalPreimageDigest(digest)),
		Prover:       prover,
		WitnessStore: store,
	}

	t.Run("valid proof", func(t *testing.T) {
		output, err := task.Execute(context.Background())
		if err != nil {
			t.Fatalf("execute preimage task: %v", err)
		}
		proof, err := base64.StdEncoding.DecodeString(output)
		if err != nil {
			t.Fatalf("decode Base64 proof: %v", err)
		}
		if len(proof) == 0 {
			t.Fatal("proof is empty")
		}
		if err := verifier.Verify(proof, digest); err != nil {
			t.Fatalf("verify preimage proof: %v", err)
		}
	})

	t.Run("mismatched digest", func(t *testing.T) {
		task.Payload = preimageTaskPayload(t, "secret-001", decimalPreimageDigest(zk.ComputePreimageDigest(43)))
		output, err := task.Execute(context.Background())
		if err == nil || output != "" {
			t.Fatalf("expected proving failure for wrong public digest, got output=%q error=%v", output, err)
		}
	})
}

func TestZKPreimageTaskMissingWitness(t *testing.T) {
	prover := &countingPreimageProver{}
	task := ZKPreimageTask{
		Payload:      preimageTaskPayload(t, "missing", "123"),
		Prover:       prover,
		WitnessStore: NewMemoryWitnessStore(),
	}

	output, err := task.Execute(context.Background())
	if err == nil || output != "" || !strings.Contains(err.Error(), "witness not found") {
		t.Fatalf("expected missing-witness error, got output=%q error=%v", output, err)
	}
	if prover.calls != 0 {
		t.Fatalf("prover called %d times for missing witness", prover.calls)
	}
}

func TestZKPreimageTaskInvalidDigest(t *testing.T) {
	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	prover := &countingPreimageProver{}
	task := ZKPreimageTask{
		Payload:      preimageTaskPayload(t, "secret-001", "not-a-number"),
		Prover:       prover,
		WitnessStore: store,
	}

	output, err := task.Execute(context.Background())
	if err == nil || output != "" || !strings.Contains(err.Error(), "invalid digest") {
		t.Fatalf("expected invalid-digest error, got output=%q error=%v", output, err)
	}
	if prover.calls != 0 {
		t.Fatalf("prover called %d times for invalid digest", prover.calls)
	}
}

func TestZKPreimageTaskCanceledBeforeProving(t *testing.T) {
	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	prover := &countingPreimageProver{}
	task := ZKPreimageTask{
		Payload:      preimageTaskPayload(t, "secret-001", decimalPreimageDigest(zk.ComputePreimageDigest(42))),
		Prover:       prover,
		WitnessStore: store,
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	output, err := task.Execute(ctx)
	if !errors.Is(err, context.Canceled) || output != "" {
		t.Fatalf("expected context cancellation, got output=%q error=%v", output, err)
	}
	if prover.calls != 0 {
		t.Fatalf("prover called %d times after cancellation", prover.calls)
	}
}

func TestZKPreimageTaskPropagatesProverError(t *testing.T) {
	store := NewMemoryWitnessStore()
	store.Put("secret-001", 42)
	wantErr := errors.New("prover failed")
	prover := &countingPreimageProver{err: wantErr}
	task := ZKPreimageTask{
		Payload:      preimageTaskPayload(t, "secret-001", decimalPreimageDigest(zk.ComputePreimageDigest(42))),
		Prover:       prover,
		WitnessStore: store,
	}

	output, err := task.Execute(context.Background())
	if !errors.Is(err, wantErr) || output != "" {
		t.Fatalf("expected prover error, got output=%q error=%v", output, err)
	}
	if prover.calls != 1 {
		t.Fatalf("prover called %d times, want 1", prover.calls)
	}
}
