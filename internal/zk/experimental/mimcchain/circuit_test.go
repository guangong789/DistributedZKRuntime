package mimcchain

import (
	"fmt"
	"io"
	"math"
	"math/big"
	"os"
	"path/filepath"
	"testing"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	nativeMiMC "github.com/consensys/gnark-crypto/ecc/bn254/fr/mimc"
	"github.com/consensys/gnark/backend/groth16"
	"github.com/consensys/gnark/backend/witness"
	"github.com/consensys/gnark/constraint"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

func compileChain(t *testing.T, n int) constraint.ConstraintSystem {
	t.Helper()
	circuit, err := NewCircuit(n)
	if err != nil {
		t.Fatal(err)
	}
	ccs, err := frontend.Compile(ecc.BN254.ScalarField(), r1cs.NewBuilder, circuit)
	if err != nil {
		t.Fatal("compile MiMC chain:", err)
	}
	return ccs
}

func chainWitness(t *testing.T, secret int64, digest fr.Element, n int, publicOnly bool) witness.Witness {
	t.Helper()
	assignment := &Circuit{Secret: big.NewInt(secret), Digest: digest, N: n}
	var options []frontend.WitnessOption
	if publicOnly {
		options = append(options, frontend.PublicOnly())
	}
	w, err := frontend.NewWitness(assignment, ecc.BN254.ScalarField(), options...)
	if err != nil {
		t.Fatal("construct chain witness")
	}
	return w
}

// This path is independent of ComputeDigest's field-element absorption. Each
// Write receives exactly one canonical 32-byte big-endian field element.
func byteDigest(t *testing.T, secret int64, n int) fr.Element {
	t.Helper()
	var value fr.Element
	value.SetInt64(secret)
	h := nativeMiMC.NewMiMC()
	for i := 0; i < n; i++ {
		h.Reset()
		encoded := value.Bytes()
		if written, err := h.Write(encoded[:]); err != nil || written != fr.Bytes {
			t.Fatal("native MiMC field encoding was not accepted")
		}
		value.SetBytes(h.Sum(nil))
	}
	return value
}

func TestChainLengthValidation(t *testing.T) {
	for _, n := range []int{1, 2, 8, 32} {
		circuit, err := NewCircuit(n)
		if err != nil || circuit == nil || circuit.N != n {
			t.Fatalf("supported construction length %d rejected", n)
		}
	}
	for _, n := range []int{-1, 0, 3, 7, 9, 31, 33, math.MaxInt} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			if circuit, err := NewCircuit(n); err == nil || circuit != nil {
				t.Fatal("invalid construction length accepted")
			}
			if _, err := ComputeDigest(42, n); err == nil {
				t.Fatal("invalid reference length accepted")
			}
			// Direct struct construction must not bypass the constructor's guard.
			if _, err := frontend.Compile(ecc.BN254.ScalarField(), r1cs.NewBuilder, &Circuit{N: n}); err == nil {
				t.Fatal("invalid length compiled")
			}
		})
	}
}

func TestCircuitRelations(t *testing.T) {
	for _, n := range []int{1, 2, 8} {
		t.Run(fmt.Sprintf("N%d", n), func(t *testing.T) {
			ccs := compileChain(t, n) // Once per size; shared by positive/negative assignments.
			if ccs.GetNbSecretVariables() != 1 || ccs.GetNbPublicVariables() != 2 {
				t.Fatal("expected one secret and one public input plus the ONE wire")
			}
			if n == 1 {
				production, err := frontend.Compile(ecc.BN254.ScalarField(), r1cs.NewBuilder, &zk.PreimageCircuit{})
				if err != nil || production.GetNbConstraints() != 331 || ccs.GetNbConstraints() != production.GetNbConstraints() {
					t.Fatal("N=1 differs from the production 331-constraint circuit")
				}
				t.Log("N=1 and the unchanged production circuit both have 331 constraints")
			}
			for _, tc := range []struct {
				name   string
				secret int64
			}{{"negative", -42}, {"minus_one", -1}, {"zero", 0}, {"one", 1}, {"positive", 42}, {"min_int64", math.MinInt64}, {"max_int64", math.MaxInt64}} {
				t.Run(tc.name, func(t *testing.T) {
					digest, err := ComputeDigest(tc.secret, n)
					if err != nil || digest != byteDigest(t, tc.secret, n) {
						t.Fatal("field and byte-oriented chain references disagree")
					}
					if n == 1 && digest != zk.ComputePreimageDigest(tc.secret) {
						t.Fatal("N=1 differs from the production digest reference")
					}
					if _, err := ccs.Solve(chainWitness(t, tc.secret, digest, n, false)); err != nil {
						t.Fatal("valid chain assignment rejected")
					}
					public := chainWitness(t, tc.secret, digest, n, true).Vector().(fr.Vector)
					if len(public) != 1 || public[0] != digest {
						t.Fatal("public witness must contain exactly the final digest")
					}
				})
			}
			digest, err := ComputeDigest(42, n)
			if err != nil {
				t.Fatal(err)
			}
			wrongSecretDigest, err := ComputeDigest(43, n)
			if err != nil || wrongSecretDigest == digest {
				t.Fatal("negative secret vector unexpectedly has the same digest")
			}
			var one, wrongDigest fr.Element
			one.SetOne()
			wrongDigest.Add(&digest, &one)
			otherLength := 1
			if n == 1 {
				otherLength = 2
			}
			wrongLengthDigest := byteDigest(t, 42, otherLength)
			if wrongLengthDigest == digest {
				t.Fatal("negative length vector unexpectedly has the same digest")
			}
			for _, tc := range []struct {
				name   string
				secret int64
				digest fr.Element
			}{{"wrong_digest", 42, wrongDigest}, {"wrong_secret", 43, digest}, {"wrong_length_digest", 42, wrongLengthDigest}} {
				t.Run(tc.name, func(t *testing.T) {
					if _, err := ccs.Solve(chainWitness(t, tc.secret, tc.digest, n, false)); err == nil {
						t.Fatal("incorrect chain relation satisfied the constraints")
					}
				})
			}
		})
	}
}

func TestReferenceConcurrent(t *testing.T) {
	const callers = 4
	type result struct {
		index  int
		digest fr.Element
		err    error
	}
	expected := make([]fr.Element, callers)
	for i := range expected {
		expected[i] = byteDigest(t, int64(i)-2, 8)
	}
	results := make(chan result, callers)
	for i := 0; i < callers; i++ {
		go func() {
			digest, err := ComputeDigest(int64(i)-2, 8)
			results <- result{i, digest, err}
		}()
	}
	for range callers {
		got := <-results
		if got.err != nil || got.digest != expected[got.index] {
			t.Fatal("concurrent reference computation disagrees with independent encoding")
		}
	}
}

func writeKey(t *testing.T, path string, key io.WriterTo) {
	t.Helper()
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal("create temporary key file")
	}
	_, writeErr := key.WriteTo(file)
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatal("serialize temporary key")
	}
}

func readKey(t *testing.T, path string, key io.ReaderFrom) {
	t.Helper()
	file, err := os.Open(path)
	if err != nil {
		t.Fatal("open temporary key")
	}
	_, readErr := key.ReadFrom(file)
	closeErr := file.Close()
	if readErr != nil || closeErr != nil {
		t.Fatal("reload temporary key")
	}
}

// Run only after correctness and calibration gates:
// DZKR_MIMCCHAIN_GROTH16=1 go test -mod=readonly -count=1 -v -run '^TestGroth16N2$' .
// Exactly two N=2 setups (the second for wrong-VK rejection), one real proof,
// and no N=8/N=32 setup. Caller must supply a process watchdog; these APIs do
// not accept a cancelable context. Temporary keys are removed by t.TempDir.
func TestGroth16N2(t *testing.T) {
	if os.Getenv("DZKR_MIMCCHAIN_GROTH16") != "1" {
		t.Skip("opt-in N=2 Groth16 gate; set DZKR_MIMCCHAIN_GROTH16=1")
	}
	finish := startStage(t, "compile", 2)
	ccs := compileChain(t, 2)
	finish()
	finish = startStage(t, "setup", 2)
	pk, vk, err := groth16.Setup(ccs)
	if err != nil {
		t.Fatal("N=2 setup failed")
	}
	finish()
	dir := t.TempDir()
	pkPath, vkPath := filepath.Join(dir, "proving.key"), filepath.Join(dir, "verifying.key")
	writeKey(t, pkPath, pk)
	writeKey(t, vkPath, vk)
	reloadedPK, reloadedVK := groth16.NewProvingKey(ecc.BN254), groth16.NewVerifyingKey(ecc.BN254)
	readKey(t, pkPath, reloadedPK)
	readKey(t, vkPath, reloadedVK)
	digest, err := ComputeDigest(42, 2)
	if err != nil {
		t.Fatal(err)
	}
	full, public := chainWitness(t, 42, digest, 2, false), chainWitness(t, 42, digest, 2, true)
	if vk.NbPublicWitness() != 1 || reloadedVK.NbPublicWitness() != 1 {
		t.Fatal("unexpected verifying-key public input count")
	}
	finish = startStage(t, "prove", 2)
	proof, err := groth16.Prove(ccs, reloadedPK, full)
	if err != nil {
		t.Fatal("N=2 proving failed")
	}
	finish()
	if err := groth16.Verify(proof, vk, public); err != nil {
		t.Fatal("reloaded proving key does not match original verifying key")
	}
	if err := groth16.Verify(proof, reloadedVK, public); err != nil {
		t.Fatal("reloaded verifying key rejected the proof")
	}
	var wrong, one fr.Element
	one.SetOne()
	wrong.Add(&digest, &one)
	if err := groth16.Verify(proof, reloadedVK, chainWitness(t, 42, wrong, 2, true)); err == nil {
		t.Fatal("wrong public digest verified")
	}
	finish = startStage(t, "setup_wrong_vk", 2)
	_, wrongVK, err := groth16.Setup(ccs)
	if err != nil {
		t.Fatal("wrong-key setup failed")
	}
	finish()
	if err := groth16.Verify(proof, wrongVK, public); err == nil {
		t.Fatal("proof verified against an unrelated setup")
	}
	emitMetadata(t, "MIMCCHAIN_GROTH16", map[string]any{
		"length": 2, "setup_pairs": 2, "proofs": 1, "matching_vk_verified": true,
		"reloaded_pk_used": true, "reloaded_vk_verified": true,
		"wrong_digest_rejected": true, "wrong_vk_rejected": true,
		"key_files_retained": false,
	})
}
