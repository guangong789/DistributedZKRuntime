package mimcchain

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/consensys/gnark/backend/groth16"
	"github.com/consensys/gnark/backend/witness"
	"github.com/consensys/gnark/constraint"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
)

func pilotN2Plan() []pilotSpec {
	return []pilotSpec{
		{"compile", "compile", true, false},
		{"digest", "digest", false, false},
		{"witness", "witness", true, false},
		{"public_witness", "public_witness", false, false},
		{"setup", "setup", true, false},
		{"preflight_prove", "preflight_prove", false, false},
		{"preflight_verify", "preflight_verify", false, false},
		{"ccs_serialize", "ccs_serialize", false, false},
		{"pk_serialize", "pk_serialize", true, false},
		{"vk_serialize", "vk_serialize", true, false},
		{"pk_decode", "pk_decode", true, false},
		{"vk_decode", "vk_decode", true, false},
		{"prove", "prove", true, false},
		{"verify", "verify", true, false},
		{"wrong_digest_verify", "wrong_digest_verify", false, true},
		{"proof_serialize", "proof_serialize", true, false},
		{"artifact_io", "artifact_io", false, false},
	}
}

func pilotWriteFile(path string, data []byte) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return err
	}
	n, writeErr := f.Write(data)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil || n != len(data) {
		return errors.New("artifact write/close incomplete")
	}
	return nil
}

// One opt-in archival run, explicitly selecting N=2 or N=8. No fixture is created before all guards.
// One Setup, one preflight + one timed Prove, two in-process Verify calls,
// and one expected-rejection Verify. The separate artifact audit remains N=2-only.
// Ordinary/race suites skip this test.
func TestMiMCChainCPUPilot(t *testing.T) {
	if os.Getenv("DZKR_MIMCCHAIN_CPU_PILOT") != "1" {
		t.Skip("opt-in N=2/N=8 CPU pilot")
	}
	n, constraints := 0, 0
	switch os.Getenv("DZKR_MIMCCHAIN_CPU_N") {
	case "2":
		n, constraints = 2, 661
	case "8":
		n, constraints = 8, 2641
	default:
		t.Fatal("pilot requires explicit N=2 or N=8")
	}
	if flag.Lookup("test.benchtime").Value.String() != "1x" || runtime.GOMAXPROCS(0) != 4 {
		t.Fatal("pilot requires benchtime=1x and GOMAXPROCS=4")
	}
	if err := pilotStartBarrier("pilot"); err != nil {
		t.Fatal(err)
	}
	t.Run(fmt.Sprintf("N%d", n), func(t *testing.T) {
		out, runID, sourceID := os.Getenv("DZKR_MIMCCHAIN_EVIDENCE_DIR"), os.Getenv("DZKR_MIMCCHAIN_RUN_ID"), os.Getenv("DZKR_MIMCCHAIN_SOURCE_ID")
		if !filepath.IsAbs(out) || runID == "" || strings.ContainsAny(runID, "/\\") || len(sourceID) != 64 {
			t.Fatal("missing explicit external evidence/source identity")
		}
		if _, err := hex.DecodeString(sourceID); err != nil {
			t.Fatal("invalid source identity")
		}
		root, err := os.Getwd()
		if err != nil {
			t.Fatal("cannot identify repository")
		}
		for {
			if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
				break
			}
			parent := filepath.Dir(root)
			if parent == root {
				t.Fatal("cannot identify repository")
			}
			root = parent
		}
		out = filepath.Clean(out)
		if out == root || strings.HasPrefix(out, root+string(os.PathSeparator)) {
			t.Fatal("pilot artifacts must be outside repository")
		}
		if err := os.Mkdir(out, 0o700); err != nil {
			t.Fatal("evidence leaf must be new")
		}
		ledgerFile, err := os.OpenFile(filepath.Join(out, "ledger.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			t.Fatal("create ledger")
		}
		sampleFile, err := os.OpenFile(filepath.Join(out, "samples.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			_ = ledgerFile.Close()
			t.Fatal("create samples")
		}
		d := pilotDriver{RunID: runID, FixtureID: fmt.Sprintf("%s/N%d", runID, n), Length: n, Origin: time.Now(), Plan: pilotN2Plan(), Ledger: ledgerFile, Samples: sampleFile}
		defer func() {
			ledgerClose, sampleClose := ledgerFile.Close(), sampleFile.Close()
			if ledgerClose != nil || sampleClose != nil {
				d.Invalid = true
				t.Error("evidence close failure")
			}
			footer := map[string]any{"run_id": runID, "source_id": sourceID, "fixture_id": d.FixtureID, "length": n, "complete": d.Complete(), "operations": len(d.Operations), "planned_operations": len(d.Plan)}
			if n == 8 {
				footer["profile"] = "mimcchain-n8-groth16-v1"
			}
			encoded, err := json.Marshal(footer)
			if err != nil || pilotWriteFile(filepath.Join(out, "footer.json"), encoded) != nil {
				t.Error("footer evidence failure")
			}
			fmt.Printf("MIMCCHAIN_CPU_FOOTER %s\n", encoded)
		}()
		do := func(id string, op func() error) {
			t.Helper()
			if err := d.Do(id, op); err != nil {
				t.Fatal(err)
			}
		}
		var ccs constraint.ConstraintSystem
		var digest fr.Element
		var full, public witness.Witness
		var pk, reloadPK groth16.ProvingKey
		var vk, reloadVK groth16.VerifyingKey
		var preflight, proof groth16.Proof
		var ccsBytes, pkBytes, vkBytes, proofBytes bytes.Buffer
		do("compile", func() error {
			c, err := NewCircuit(n)
			if err != nil {
				return err
			}
			ccs, err = frontend.Compile(ecc.BN254.ScalarField(), r1cs.NewBuilder, c)
			return err
		})
		if ccs.GetNbConstraints() != constraints || ccs.GetNbSecretVariables() != 1 || ccs.GetNbPublicVariables() != 2 {
			d.Invalid = true
			t.Fatal("unexpected compiled pilot metadata")
		}
		do("digest", func() error { digest, err = ComputeDigest(42, n); return err })
		do("witness", func() error {
			full, err = frontend.NewWitness(&Circuit{N: n, Secret: big.NewInt(42), Digest: digest}, ecc.BN254.ScalarField())
			return err
		})
		do("public_witness", func() error {
			public, err = frontend.NewWitness(&Circuit{N: n, Digest: digest}, ecc.BN254.ScalarField(), frontend.PublicOnly())
			if err == nil {
				v := public.Vector().(fr.Vector)
				if len(v) != 1 || v[0] != digest {
					return errors.New("unexpected public witness")
				}
			}
			return err
		})
		do("setup", func() error { pk, vk, err = groth16.Setup(ccs); return err })
		do("preflight_prove", func() error { preflight, err = groth16.Prove(ccs, pk, full); return err })
		do("preflight_verify", func() error { return groth16.Verify(preflight, vk, public) })
		do("ccs_serialize", func() error { _, err := ccs.WriteTo(&ccsBytes); return err })
		do("pk_serialize", func() error { _, err := pk.WriteTo(&pkBytes); return err })
		do("vk_serialize", func() error { _, err := vk.WriteTo(&vkBytes); return err })
		do("pk_decode", func() error {
			reloadPK = groth16.NewProvingKey(ecc.BN254)
			_, err := reloadPK.ReadFrom(bytes.NewReader(pkBytes.Bytes()))
			return err
		})
		do("vk_decode", func() error {
			reloadVK = groth16.NewVerifyingKey(ecc.BN254)
			_, err := reloadVK.ReadFrom(bytes.NewReader(vkBytes.Bytes()))
			return err
		})
		do("prove", func() error { proof, err = groth16.Prove(ccs, reloadPK, full); return err })
		do("verify", func() error { return groth16.Verify(proof, reloadVK, public) })
		do("wrong_digest_verify", func() error {
			var wrong, one fr.Element
			one.SetOne()
			wrong.Add(&digest, &one)
			w, err := frontend.NewWitness(&Circuit{N: n, Digest: wrong}, ecc.BN254.ScalarField(), frontend.PublicOnly())
			if err != nil {
				panic("negative witness construction failed")
			}
			return groth16.Verify(proof, reloadVK, w)
		})
		do("proof_serialize", func() error { _, err := proof.WriteTo(&proofBytes); return err })
		do("artifact_io", func() error {
			statement, err := json.Marshal(map[string]any{"length": n, "digest": digest.String()})
			if err != nil {
				return err
			}
			artifacts := map[string][]byte{"circuit.ccs": ccsBytes.Bytes(), "proving.key": pkBytes.Bytes(), "verifying.key": vkBytes.Bytes(), "proof.bin": proofBytes.Bytes(), "public.json": statement}
			identities := make(map[string]any)
			for name, data := range artifacts {
				if len(data) == 0 {
					return errors.New("empty artifact")
				}
				if err := pilotWriteFile(filepath.Join(out, name), data); err != nil {
					return err
				}
				hash := sha256.Sum256(data)
				identities[name] = map[string]any{"sha256": hex.EncodeToString(hash[:]), "bytes": len(data)}
			}
			metadata := map[string]any{"run_id": runID, "source_id": sourceID, "fixture_id": d.FixtureID, "length": n,
				"constraints": ccs.GetNbConstraints(), "internal_variables": ccs.GetNbInternalVariables(), "instructions": ccs.GetNbInstructions(),
				"public_variables_including_one": ccs.GetNbPublicVariables(), "secret_variables": ccs.GetNbSecretVariables(),
				"go_version": runtime.Version(), "num_cpu": runtime.NumCPU(), "gomaxprocs": runtime.GOMAXPROCS(0), "module_versions": moduleVersions(t),
				"artifacts": identities, "allocation_scope": "whole-process Go benchmark timed-window deltas", "gc_policy": "testing.Benchmark runs GC before each sample"}
			if n == 8 {
				metadata["profile"] = "mimcchain-n8-groth16-v1"
				metadata["curve"], metadata["backend"], metadata["constraint_system"] = "BN254", "groth16", "R1CS"
			}
			encoded, err := json.Marshal(metadata)
			if err != nil {
				return err
			}
			return pilotWriteFile(filepath.Join(out, "fixture.json"), encoded)
		})
	})
}

// This separate process reserves the third successful Verify in the run budget.
// It reloads persisted artifacts and computes the expected statement independently
// of the proving fixture; it never performs Setup or Prove.
func TestMiMCChainCPUArtifactAudit(t *testing.T) {
	if os.Getenv("DZKR_MIMCCHAIN_CPU_AUDIT") != "1" {
		t.Skip("opt-in artifact audit")
	}
	if err := pilotStartBarrier("audit"); err != nil {
		t.Fatal(err)
	}
	out := os.Getenv("DZKR_MIMCCHAIN_EVIDENCE_DIR")
	if !filepath.IsAbs(out) {
		t.Fatal("explicit external evidence required")
	}
	root, err := os.Getwd()
	if err != nil {
		t.Fatal("cwd")
	}
	for {
		if _, err := os.Stat(filepath.Join(root, "go.mod")); err == nil {
			break
		}
		parent := filepath.Dir(root)
		if parent == root {
			t.Fatal("repository root")
		}
		root = parent
	}
	out = filepath.Clean(out)
	if out == root || strings.HasPrefix(out, root+string(os.PathSeparator)) {
		t.Fatal("audit must be external")
	}
	data, err := os.ReadFile(filepath.Join(out, "fixture.json"))
	if err != nil {
		t.Fatal("fixture missing")
	}
	var fixture struct {
		RunID     string `json:"run_id"`
		SourceID  string `json:"source_id"`
		FixtureID string `json:"fixture_id"`
		Length    int    `json:"length"`
		Artifacts map[string]struct {
			SHA   string `json:"sha256"`
			Bytes int    `json:"bytes"`
		} `json:"artifacts"`
	}
	if json.Unmarshal(data, &fixture) != nil || fixture.Length != 2 || fixture.SourceID != os.Getenv("DZKR_MIMCCHAIN_SOURCE_ID") || fixture.RunID != os.Getenv("DZKR_MIMCCHAIN_RUN_ID") {
		t.Fatal("fixture identity mismatch")
	}
	ledger, err := os.OpenFile(filepath.Join(out, "audit-ledger.jsonl"), os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal("audit ledger")
	}
	defer func() {
		if ledger.Close() != nil {
			t.Error("audit close")
		}
	}()
	d := pilotDriver{RunID: fixture.RunID, FixtureID: fixture.FixtureID, Origin: time.Now(), Plan: []pilotSpec{{ID: "audit_decode", Kind: "audit_decode"}, {ID: "audit_verify", Kind: "audit_verify"}}, Ledger: ledger}
	var proof groth16.Proof
	var vk groth16.VerifyingKey
	var public witness.Witness
	err = d.Do("audit_decode", func() error {
		artifacts := make(map[string][]byte)
		for name, expected := range fixture.Artifacts {
			if filepath.Base(name) != name {
				return errors.New("artifact path")
			}
			b, e := os.ReadFile(filepath.Join(out, name))
			if e != nil {
				return e
			}
			h := sha256.Sum256(b)
			if len(b) != expected.Bytes || hex.EncodeToString(h[:]) != expected.SHA {
				return errors.New("artifact hash")
			}
			artifacts[name] = b
		}
		ccs := groth16.NewCS(ecc.BN254)
		if _, e := ccs.ReadFrom(bytes.NewReader(artifacts["circuit.ccs"])); e != nil {
			return e
		}
		if ccs.GetNbConstraints() != 661 || ccs.GetNbPublicVariables() != 2 || ccs.GetNbSecretVariables() != 1 {
			return errors.New("CCS identity")
		}
		vk = groth16.NewVerifyingKey(ecc.BN254)
		if _, e := vk.ReadFrom(bytes.NewReader(artifacts["verifying.key"])); e != nil {
			return e
		}
		proof = groth16.NewProof(ecc.BN254)
		if _, e := proof.ReadFrom(bytes.NewReader(artifacts["proof.bin"])); e != nil {
			return e
		}
		digest, e := ComputeDigest(42, 2)
		if e != nil {
			return e
		}
		var statement struct {
			Length int    `json:"length"`
			Digest string `json:"digest"`
		}
		if json.Unmarshal(artifacts["public.json"], &statement) != nil || statement.Length != 2 || statement.Digest != digest.String() {
			return errors.New("public statement")
		}
		public, e = frontend.NewWitness(&Circuit{N: 2, Digest: digest}, ecc.BN254.ScalarField(), frontend.PublicOnly())
		return e
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = d.Do("audit_verify", func() error { return groth16.Verify(proof, vk, public) }); err != nil {
		t.Fatal(err)
	}
	result, _ := json.Marshal(map[string]any{"complete": d.Complete(), "run_id": fixture.RunID, "fixture_id": fixture.FixtureID, "successful_verify_calls": 1, "setup_calls": 0, "prove_calls": 0})
	if pilotWriteFile(filepath.Join(out, "audit-result.json"), result) != nil {
		t.Fatal("audit evidence")
	}
	fmt.Printf("MIMCCHAIN_CPU_AUDIT %s\n", result)
}
