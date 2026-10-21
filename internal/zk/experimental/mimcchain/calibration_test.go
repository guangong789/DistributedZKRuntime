package mimcchain

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
)

func emitMetadata(t *testing.T, prefix string, record any) {
	t.Helper()
	encoded, err := json.Marshal(record)
	if err != nil {
		t.Fatal("encode experiment metadata")
	}
	fmt.Printf("%s %s\n", prefix, encoded)
}

// VmHWM is the process's cumulative high-water RSS, not an isolated per-stage
// allocation or per-length peak. Other platforms explicitly report unavailable.
func processPeakRSSKiB() *uint64 {
	data, err := os.ReadFile("/proc/self/status")
	if err != nil {
		return nil
	}
	for _, line := range strings.Split(string(data), "\n") {
		fields := strings.Fields(line)
		if len(fields) == 3 && fields[0] == "VmHWM:" && fields[2] == "kB" {
			value, err := strconv.ParseUint(fields[1], 10, 64)
			if err == nil {
				return &value
			}
		}
	}
	return nil
}

// Stage markers let an external parent bound synchronous Compile/Setup/Prove.
// They contain no witness, key, digest, or proof contents.
func startStage(t *testing.T, stage string, n int) func() {
	t.Helper()
	emitMetadata(t, "MIMCCHAIN_STAGE", map[string]any{"event": "start", "stage": stage, "length": n})
	start := time.Now()
	return func() {
		emitMetadata(t, "MIMCCHAIN_STAGE", map[string]any{
			"event": "end", "stage": stage, "length": n,
			"elapsed_ns": time.Since(start).Nanoseconds(), "process_peak_rss_kib": processPeakRSSKiB(),
		})
	}
}

func moduleVersions(t *testing.T) map[string]string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if data, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil {
			versions := make(map[string]string)
			for _, line := range strings.Split(string(data), "\n") {
				fields := strings.Fields(line)
				if len(fields) >= 2 && (fields[0] == "github.com/consensys/gnark" || fields[0] == "github.com/consensys/gnark-crypto") {
					versions[fields[0]] = fields[1]
				}
			}
			if len(versions) != 2 {
				t.Fatal("gnark dependency versions unavailable in go.mod")
			}
			return versions
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatal("cannot locate experiment go.mod")
		}
		dir = parent
	}
}

// Explicit compile-only gate; ordinary and race suites skip N=32 calibration.
// DZKR_MIMCCHAIN_CALIBRATE=1 go test -mod=readonly -count=1 -v -run '^TestCompileCalibration$' .
// Each size is compiled exactly once here; no Setup, Solve, or Prove is called.
func TestCompileCalibration(t *testing.T) {
	if os.Getenv("DZKR_MIMCCHAIN_CALIBRATE") != "1" {
		t.Skip("opt-in compilation calibration; set DZKR_MIMCCHAIN_CALIBRATE=1")
	}
	versions := moduleVersions(t)
	for _, n := range []int{1, 2, 8, 32} {
		t.Run(fmt.Sprintf("N%d", n), func(t *testing.T) {
			circuit, err := NewCircuit(n)
			if err != nil {
				t.Fatal(err)
			}
			finish := startStage(t, "compile", n)
			var before, after runtime.MemStats
			runtime.ReadMemStats(&before)
			start := time.Now()
			ccs, err := frontend.Compile(ecc.BN254.ScalarField(), r1cs.NewBuilder, circuit)
			elapsed := time.Since(start)
			runtime.ReadMemStats(&after)
			finish()
			if err != nil {
				t.Fatal("calibration compilation failed")
			}
			if n == 1 && ccs.GetNbConstraints() != 331 {
				t.Fatal("N=1 calibration differs from production baseline")
			}
			emitMetadata(t, "MIMCCHAIN_COMPILE", map[string]any{
				"length": n, "constraints": ccs.GetNbConstraints(),
				"internal_variables": ccs.GetNbInternalVariables(), "instructions": ccs.GetNbInstructions(),
				"public_variables_including_one": ccs.GetNbPublicVariables(), "secret_variables": ccs.GetNbSecretVariables(),
				"compile_elapsed_ns": elapsed.Nanoseconds(), "compile_window_alloc_bytes": after.TotalAlloc - before.TotalAlloc,
				"compile_window_mallocs": after.Mallocs - before.Mallocs, "process_peak_rss_kib": processPeakRSSKiB(),
				"rss_scope":        "cumulative process high-water mark, not an isolated per-length peak",
				"allocation_scope": "process allocation increments around Compile, not a benchmark",
				"go_version":       runtime.Version(), "module_versions": versions, "version_source": "go.mod",
				"gomaxprocs": runtime.GOMAXPROCS(0),
			})
		})
	}
}
