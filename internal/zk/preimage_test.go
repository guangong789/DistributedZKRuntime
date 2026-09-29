package zk

import (
	"path/filepath"
	"testing"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	nativeMiMC "github.com/consensys/gnark-crypto/ecc/bn254/fr/mimc"
	"github.com/consensys/gnark/backend/groth16"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
)

func TestPreimageProof(t *testing.T) {
	var circuit PreimageCircuit

	ccs, err := frontend.Compile(
		ecc.BN254.ScalarField(),
		r1cs.NewBuilder,
		&circuit,
	)
	if err != nil {
		t.Fatalf("compile preimage circuit: %v", err)
	}

	pk, vk, err := groth16.Setup(ccs)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	var secret fr.Element
	secret.SetInt64(42)

	hasher := nativeMiMC.NewMiMC()

	secretBytes := secret.Bytes()

	if _, err := hasher.Write(secretBytes[:]); err != nil {
		t.Fatalf("hash secret: %v", err)
	}

	digestBytes := hasher.Sum(nil)

	var digest fr.Element
	digest.SetBytes(digestBytes)

	assignment := PreimageCircuit{
		Preimage: 42,
		Hash:     digest,
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

func TestPreimageProofRejectsWrongSecret(t *testing.T) {
	var circuit PreimageCircuit

	ccs, err := frontend.Compile(
		ecc.BN254.ScalarField(),
		r1cs.NewBuilder,
		&circuit,
	)
	if err != nil {
		t.Fatalf("compile preimage circuit: %v", err)
	}

	pk, _, err := groth16.Setup(ccs)
	if err != nil {
		t.Fatalf("setup: %v", err)
	}

	var committedSecret fr.Element
	committedSecret.SetInt64(42)

	hasher := nativeMiMC.NewMiMC()

	secretBytes := committedSecret.Bytes()

	if _, err := hasher.Write(secretBytes[:]); err != nil {
		t.Fatalf("hash secret: %v", err)
	}

	digestBytes := hasher.Sum(nil)

	var digest fr.Element
	digest.SetBytes(digestBytes)

	assignment := PreimageCircuit{
		Preimage: 43,
		Hash:     digest,
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
		t.Fatal("expected proving to fail for wrong preimage")
	}
}

func TestPreimageSetupLoadProveVerify(t *testing.T) {
	dir := t.TempDir()

	pkPath := filepath.Join(dir, "proving.key")
	vkPath := filepath.Join(dir, "verifying.key")

	if err := GeneratePreimageSetup(pkPath, vkPath); err != nil {
		t.Fatalf("generate setup: %v", err)
	}

	prover, err := LoadPreimageProver(pkPath)
	if err != nil {
		t.Fatalf("load prover: %v", err)
	}

	verifier, err := LoadPreimageVerifier(vkPath)
	if err != nil {
		t.Fatalf("load verifier: %v", err)
	}

	digest := ComputePreimageDigest(42)

	proof, err := prover.Prove(42, digest)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	if err := verifier.Verify(proof, digest); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestPreimageVerifyRejectsWrongDigest(t *testing.T) {
	dir := t.TempDir()

	pkPath := filepath.Join(dir, "proving.key")
	vkPath := filepath.Join(dir, "verifying.key")

	if err := GeneratePreimageSetup(pkPath, vkPath); err != nil {
		t.Fatalf("generate setup: %v", err)
	}

	prover, err := LoadPreimageProver(pkPath)
	if err != nil {
		t.Fatalf("load prover: %v", err)
	}

	verifier, err := LoadPreimageVerifier(vkPath)
	if err != nil {
		t.Fatalf("load verifier: %v", err)
	}

	digest42 := ComputePreimageDigest(42)

	digest43 := ComputePreimageDigest(43)

	proof, err := prover.Prove(42, digest42)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	if err := verifier.Verify(proof, digest43); err == nil {
		t.Fatal("expected verification to fail with wrong public digest")
	}
}
