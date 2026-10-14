package zk

import "testing"

// Setup and key generation are excluded from every benchmark. The fixed MiMC
// statement uses the public demo secret; Groth16 retains its normal randomness.
func newPreimageBenchmarkFixture(b *testing.B) (*PreimageProver, *PreimageVerifier, []byte) {
	b.Helper()
	pkPath, vkPath := newPreimageTestKeys(b)
	prover, err := LoadPreimageProver(pkPath)
	if err != nil {
		b.Fatal(err)
	}
	verifier, err := LoadPreimageVerifier(vkPath)
	if err != nil {
		b.Fatal(err)
	}
	var proof []byte
	for i := 0; i < 3; i++ {
		proof, err = prover.Prove(42, ComputePreimageDigest(42))
		if err != nil || len(proof) == 0 {
			b.Fatalf("warmup proof: error=%v bytes=%d", err, len(proof))
		}
		if err := verifier.Verify(proof, ComputePreimageDigest(42)); err != nil {
			b.Fatalf("verify warmup proof: %v", err)
		}
	}
	return prover, verifier, proof
}

// BenchmarkPreimageProve includes witness construction, real Groth16 proving,
// and binary proof serialization through the production wrapper. Only the last
// proof is retained and independently verified after stopping the timer.
func BenchmarkPreimageProve(b *testing.B) {
	prover, verifier, _ := newPreimageBenchmarkFixture(b)
	digest := ComputePreimageDigest(42)
	var proof []byte
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		var err error
		proof, err = prover.Prove(42, digest)
		if err != nil || len(proof) == 0 {
			b.Fatalf("prove: error=%v bytes=%d", err, len(proof))
		}
	}
	b.StopTimer()
	if err := verifier.Verify(proof, digest); err != nil {
		b.Fatalf("verify measured proof: %v", err)
	}
	b.ReportMetric(float64(prover.ccs.GetNbConstraints()), "constraints")
}

// BenchmarkPreimageVerify includes binary proof decoding, public witness
// construction, and real Groth16 verification. It excludes Base64 decoding.
func BenchmarkPreimageVerify(b *testing.B) {
	prover, verifier, proof := newPreimageBenchmarkFixture(b)
	digest := ComputePreimageDigest(42)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := verifier.Verify(proof, digest); err != nil {
			b.Fatalf("verify: %v", err)
		}
	}
	b.StopTimer()
	b.ReportMetric(float64(prover.ccs.GetNbConstraints()), "constraints")
}

// Load benchmarks measure repeated initialization with a potentially warm OS
// page cache, not cold-disk startup. Prover loading also recompiles the circuit.
func BenchmarkLoadPreimageProver(b *testing.B) {
	pkPath, vkPath := newPreimageTestKeys(b)
	verifier, err := LoadPreimageVerifier(vkPath)
	if err != nil {
		b.Fatal(err)
	}
	var prover *PreimageProver
	// Preflight the load path before measuring repeated initialization.
	if _, err := LoadPreimageProver(pkPath); err != nil {
		b.Fatal(err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		prover, err = LoadPreimageProver(pkPath)
		if err != nil || prover == nil {
			b.Fatalf("load prover: %v", err)
		}
	}
	b.StopTimer()
	digest := ComputePreimageDigest(42)
	proof, err := prover.Prove(42, digest)
	if err != nil || len(proof) == 0 {
		b.Fatalf("loaded prover: error=%v bytes=%d", err, len(proof))
	}
	if err := verifier.Verify(proof, digest); err != nil {
		b.Fatalf("verify loaded prover output: %v", err)
	}
	b.ReportMetric(float64(prover.ccs.GetNbConstraints()), "constraints")
}

func BenchmarkLoadPreimageVerifier(b *testing.B) {
	pkPath, vkPath := newPreimageTestKeys(b)
	prover, err := LoadPreimageProver(pkPath)
	if err != nil {
		b.Fatal(err)
	}
	digest := ComputePreimageDigest(42)
	proof, err := prover.Prove(42, digest)
	if err != nil || len(proof) == 0 {
		b.Fatalf("fixture proof: error=%v bytes=%d", err, len(proof))
	}
	verifier, err := LoadPreimageVerifier(vkPath)
	if err != nil {
		b.Fatal(err)
	}
	if err := verifier.Verify(proof, digest); err != nil {
		b.Fatalf("preflight verification: %v", err)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		verifier, err = LoadPreimageVerifier(vkPath)
		if err != nil || verifier == nil {
			b.Fatalf("load verifier: %v", err)
		}
	}
	b.StopTimer()
	if err := verifier.Verify(proof, digest); err != nil {
		b.Fatalf("verify with measured verifier: %v", err)
	}
}
