package zk

import (
	"path/filepath"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	nativeMiMC "github.com/consensys/gnark-crypto/ecc/bn254/fr/mimc"
)

func newPreimageTestKeys(t testing.TB) (string, string) {
	t.Helper()
	dir := t.TempDir()

	pkPath := filepath.Join(dir, "proving.key")
	vkPath := filepath.Join(dir, "verifying.key")

	if err := GeneratePreimageSetup(pkPath, vkPath); err != nil {
		t.Fatalf("generate setup: %v", err)
	}
	return pkPath, vkPath
}

func TestPreimageSetupLoadProveVerify(t *testing.T) {
	pkPath, vkPath := newPreimageTestKeys(t)

	prover, err := LoadPreimageProver(pkPath)
	if err != nil {
		t.Fatalf("load prover: %v", err)
	}

	verifier, err := LoadPreimageVerifier(vkPath)
	if err != nil {
		t.Fatalf("load verifier: %v", err)
	}

	// Keep an independent byte-oriented digest calculation to check the public
	// statement, rather than relying only on the production digest helper.
	var secret fr.Element
	secret.SetInt64(42)
	secretBytes := secret.Bytes()
	hasher := nativeMiMC.NewMiMC()
	if _, err := hasher.Write(secretBytes[:]); err != nil {
		t.Fatalf("hash secret: %v", err)
	}
	var digest fr.Element
	digest.SetBytes(hasher.Sum(nil))
	if got := ComputePreimageDigest(42); got != digest {
		t.Fatal("public digest differs from byte-oriented MiMC hash")
	}

	proof, err := prover.Prove(42, digest)
	if err != nil {
		t.Fatalf("prove: %v", err)
	}

	for _, tc := range []struct {
		name    string
		digest  fr.Element
		wantErr bool
	}{
		{"matching public digest", digest, false},
		{"wrong public digest", ComputePreimageDigest(43), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := verifier.Verify(proof, tc.digest); (err != nil) != tc.wantErr {
				t.Fatalf("verify error = %v, want error = %t", err, tc.wantErr)
			}
		})
	}

	t.Run("wrong secret", func(t *testing.T) {
		if _, err := prover.Prove(43, digest); err == nil {
			t.Fatal("expected proving to fail for wrong preimage")
		}
	})
}
