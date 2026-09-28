package zk

import (
	"path/filepath"
	"testing"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark/backend/groth16"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
)

func TestSquareProof(t *testing.T) {
	var circuit SquareCircuit

	ccs, err := frontend.Compile(
		ecc.BN254.ScalarField(),
		r1cs.NewBuilder,
		&circuit,
	)
	if err != nil {
		t.Fatalf("compile circuit: %v", err)
	}

	pk, vk, err := groth16.Setup(ccs)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	assignment := SquareCircuit{
		X: 5,
		Y: 25,
	}

	witness, err := frontend.NewWitness(
		&assignment,
		ecc.BN254.ScalarField(),
	)
	if err != nil {
		t.Fatalf("create witness: %v", err)
	}

	publicWitness, err := witness.Public()
	if err != nil {
		t.Fatalf("create public witness: %v", err)
	}

	proof, err := groth16.Prove(
		ccs,
		pk,
		witness,
	)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	if err := groth16.Verify(
		proof,
		vk,
		publicWitness,
	); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestSquareProofRejectsInvalidWitness(t *testing.T) {
	var circuit SquareCircuit

	ccs, err := frontend.Compile(
		ecc.BN254.ScalarField(),
		r1cs.NewBuilder,
		&circuit,
	)
	if err != nil {
		t.Fatalf("compile circuit: %v", err)
	}

	pk, _, err := groth16.Setup(ccs)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	assignment := SquareCircuit{
		X: 5,
		Y: 24,
	}

	witness, err := frontend.NewWitness(
		&assignment,
		ecc.BN254.ScalarField(),
	)
	if err != nil {
		t.Fatalf("create witness: %v", err)
	}

	_, err = groth16.Prove(
		ccs,
		pk,
		witness,
	)

	if err == nil {
		t.Fatal("expected proving to fail for invalid witness")
	}
}

func TestProveAndVerifySquare(t *testing.T) {
	proof, err := ProveSquare(5, 25)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	if err := VerifySquare(proof, 25); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifySquareRejectsWrongPublicInput(t *testing.T) {
	proof, err := ProveSquare(5, 25)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	if err := VerifySquare(proof, 24); err == nil {
		t.Fatal("expected verification to fail")
	}
}

func TestSquareSetupArtifactsRoundTrip(t *testing.T) {
	dir := t.TempDir()

	pkPath := filepath.Join(dir, "proving.key")
	vkPath := filepath.Join(dir, "verifying.key")

	if err := GenerateSquareSetup(pkPath, vkPath); err != nil {
		t.Fatalf("generate setup: %v", err)
	}

	prover, err := LoadSquareProver(pkPath)
	if err != nil {
		t.Fatalf("load prover: %v", err)
	}

	verifier, err := LoadSquareVerifier(vkPath)
	if err != nil {
		t.Fatalf("load verifier: %v", err)
	}

	proof, err := prover.Prove(5, 25)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	if err := verifier.Verify(proof, 25); err != nil {
		t.Fatalf("verify: %v", err)
	}
}
