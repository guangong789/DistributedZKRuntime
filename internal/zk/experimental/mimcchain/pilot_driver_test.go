package mimcchain

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"testing"
	"time"
)

// Test-only, sequential driver. The callback stops the Go timer around ledger
// counters; only the operation closure and minimal call/timer overhead are timed.
// Benchmark performs a GC before its callback. No per-operation logging or
// independent verification occurs in the timed region.
type pilotSample struct {
	N           int    `json:"benchmark_n"`
	Callbacks   int    `json:"callbacks"`
	Started     int    `json:"calls_started"`
	Completed   int    `json:"calls_completed"`
	DurationNS  int64  `json:"duration_ns"`
	Allocated   uint64 `json:"allocated_bytes"`
	Allocations uint64 `json:"allocations"`
	NSPerOp     int64  `json:"ns_per_op"`
	BytesPerOp  int64  `json:"bytes_per_op"`
	AllocsPerOp int64  `json:"allocs_per_op"`
	Valid       bool   `json:"valid"`
}

type pilotOperation struct {
	ID            string       `json:"operation_id"`
	RunID         string       `json:"run_id"`
	Length        int          `json:"length"`
	Kind          string       `json:"kind"`
	Timed         bool         `json:"timed"`
	Attempt       int          `json:"attempt"`
	FixtureID     string       `json:"fixture_id"`
	StartNS       int64        `json:"start_ns"`
	EndNS         int64        `json:"end_ns"`
	Status        string       `json:"status"`
	ErrorCategory string       `json:"error_category,omitempty"`
	Started       int          `json:"calls_started"`
	Completed     int          `json:"calls_completed"`
	Sample        *pilotSample `json:"sample,omitempty"`
}

type pilotSpec struct {
	ID, Kind string
	Timed    bool
	Reject   bool
}

type pilotDriver struct {
	RunID, FixtureID string
	Origin           time.Time
	Length           int // Zero retains the original N=2 driver contract.
	Plan             []pilotSpec
	Operations       []pilotOperation
	Invalid          bool
	Ledger, Samples  io.Writer
	Benchmark        func(func(*testing.B)) testing.BenchmarkResult
}

func pilotMeasure(operation func() error, benchmark func(func(*testing.B)) testing.BenchmarkResult) (sample pilotSample, category string) {
	if flag.Lookup("test.benchtime").Value.String() != "1x" {
		return sample, "benchtime_not_1x"
	}
	var operationError error
	panicked, extra := false, false
	result := benchmark(func(b *testing.B) {
		sample.Callbacks++
		if sample.Callbacks != 1 {
			extra = true
			b.Fail() // Never repeat an expensive operation, even with a bad backend.
			return
		}
		b.ReportAllocs()
		defer func() {
			if recover() != nil {
				b.StopTimer()
				panicked = true // Do not serialize panic values or witness data.
				b.Fail()
			}
		}()
		for b.Loop() {
			b.StopTimer()
			if sample.Started != 0 {
				extra = true
				b.Fail()
				b.StartTimer()
				continue
			}
			sample.Started++
			b.StartTimer()
			operationError = operation()
			b.StopTimer()
			sample.Completed++
			b.StartTimer()
		}
	})
	sample.N, sample.DurationNS = result.N, result.T.Nanoseconds()
	sample.Allocated, sample.Allocations = result.MemBytes, result.MemAllocs
	sample.NSPerOp, sample.BytesPerOp, sample.AllocsPerOp = result.NsPerOp(), result.AllocedBytesPerOp(), result.AllocsPerOp()
	switch {
	case panicked:
		category = "operation_panic"
	case extra || sample.Callbacks != 1 || sample.Started != 1 || sample.Completed != 1:
		category = "operation_count"
	case operationError != nil:
		category = "operation_error"
	case result.N != 1 || result.T <= 0:
		category = "incomplete_result"
	case sample.NSPerOp != sample.DurationNS || sample.BytesPerOp != int64(sample.Allocated) || sample.AllocsPerOp != int64(sample.Allocations):
		category = "inconsistent_result"
	default:
		sample.Valid = true
	}
	return sample, category
}

func (d *pilotDriver) emit(writer io.Writer, value any) error {
	if writer == nil {
		return errors.New("missing evidence writer")
	}
	return json.NewEncoder(writer).Encode(value)
}

func (d *pilotDriver) Do(id string, operation func() error) error {
	if d.Invalid || len(d.Operations) >= len(d.Plan) || d.Plan[len(d.Operations)].ID != id {
		d.Invalid = true
		return errors.New("pilot plan/order violation before operation")
	}
	n := d.Length
	if n == 0 {
		n = 2
	}
	spec := d.Plan[len(d.Operations)]
	op := pilotOperation{ID: id, RunID: d.RunID, Length: n, Kind: spec.Kind, Timed: spec.Timed,
		Attempt: len(d.Operations) + 1, FixtureID: d.FixtureID, StartNS: time.Since(d.Origin).Nanoseconds(), Status: "running"}
	d.Operations = append(d.Operations, op)
	index := len(d.Operations) - 1
	if err := d.emit(d.Ledger, map[string]any{"event": "start", "operation": op}); err != nil {
		d.Invalid = true
		return err
	}
	stage := spec.Kind
	if stage == "preflight_prove" || stage == "prove" {
		stage = "prove"
	}
	fmt.Printf("MIMCCHAIN_STAGE {\"event\":\"start\",\"stage\":%q,\"length\":%d}\n", stage, n)
	if spec.Timed {
		backend := d.Benchmark
		if backend == nil {
			backend = testing.Benchmark
		}
		sample, category := pilotMeasure(operation, backend)
		op.Sample, op.ErrorCategory = &sample, category
		op.Started, op.Completed = sample.Started, sample.Completed
	} else {
		func() {
			defer func() {
				if recover() != nil {
					op.ErrorCategory = "operation_panic"
				}
			}()
			op.Started++
			err := operation()
			op.Completed++
			if spec.Reject {
				if err == nil {
					op.ErrorCategory = "expected_rejection_missing"
				} else {
					op.Status = "rejected"
				}
			} else if err != nil {
				op.ErrorCategory = "operation_error"
			}
		}()
	}
	op.EndNS = time.Since(d.Origin).Nanoseconds()
	if op.ErrorCategory != "" {
		op.Status = "failed"
		d.Invalid = true
	} else if op.Status != "rejected" {
		op.Status = "succeeded"
	}
	d.Operations[index] = op
	fmt.Printf("MIMCCHAIN_STAGE {\"event\":\"end\",\"stage\":%q,\"length\":%d,\"elapsed_ns\":%d}\n", stage, n, op.EndNS-op.StartNS)
	if err := d.emit(d.Ledger, map[string]any{"event": "end", "operation": op}); err != nil {
		d.Invalid = true
		return err
	}
	if op.Sample != nil {
		if err := d.emit(d.Samples, map[string]any{"operation_id": op.ID, "sample": op.Sample}); err != nil {
			d.Invalid = true
			return err
		}
	}
	if d.Invalid {
		return fmt.Errorf("pilot operation %s failed: %s", id, op.ErrorCategory)
	}
	return nil
}

func (d *pilotDriver) Complete() bool {
	return !d.Invalid && len(d.Operations) == len(d.Plan)
}
