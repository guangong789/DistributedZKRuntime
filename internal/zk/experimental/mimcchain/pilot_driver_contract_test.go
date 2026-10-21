package mimcchain

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"testing"
	"time"
)

func pilotOneIteration(t *testing.T) {
	t.Helper()
	old := flag.Lookup("test.benchtime").Value.String()
	if err := flag.Set("test.benchtime", "1x"); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set("test.benchtime", old) })
}

func TestPilotMeasureContract(t *testing.T) {
	pilotOneIteration(t)
	fixtureCalls := 0
	fixture := func() []byte { fixtureCalls++; return make([]byte, 1<<20) }()
	actualCalls := 0
	for i := 0; i < 2; i++ {
		s, category := pilotMeasure(func() error { actualCalls++; fixture[0]++; return nil }, testing.Benchmark)
		t.Logf("fake sample %d: %+v", i+1, s)
		if category != "" || !s.Valid || s.N != 1 || s.Callbacks != 1 || s.Started != 1 || s.Completed != 1 || s.Allocated >= uint64(len(fixture)) {
			t.Fatalf("invalid single-operation sample: %+v category=%s", s, category)
		}
	}
	if fixtureCalls != 1 || actualCalls != 2 {
		t.Fatalf("fixture=%d operation=%d", fixtureCalls, actualCalls)
	}
}

func TestPilotLedgerFailureEvidence(t *testing.T) {
	for _, tc := range []struct {
		name, category string
		op             func() error
	}{
		{"error", "operation_error", func() error { return errors.New("controlled") }},
		{"panic", "operation_panic", func() error { panic("not logged") }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var out bytes.Buffer
			d := pilotDriver{Origin: time.Now(), Plan: []pilotSpec{{ID: "fake"}}, Ledger: &out}
			if d.Do("fake", tc.op) == nil || d.Complete() {
				t.Fatal("failure accepted")
			}
			decoder := json.NewDecoder(&out)
			var start, end struct {
				Event string         `json:"event"`
				Op    pilotOperation `json:"operation"`
			}
			if decoder.Decode(&start) != nil || decoder.Decode(&end) != nil || start.Event != "start" || end.Event != "end" || end.Op.Status != "failed" || end.Op.ErrorCategory != tc.category || end.Op.Started != 1 {
				t.Fatal("failure missing from ledger")
			}
		})
	}
	// A missing evidence writer must stop execution before any actual operation.
	d := pilotDriver{Origin: time.Now(), Plan: []pilotSpec{{ID: "fake"}}}
	if d.Do("fake", func() error { t.Fatal("must not execute"); return nil }) == nil || !d.Invalid {
		t.Fatal("missing evidence accepted")
	}
}

func TestPilotMeasureFailures(t *testing.T) {
	pilotOneIteration(t)
	for _, tc := range []struct {
		name, category string
		op             func() error
		backend        func(func(*testing.B)) testing.BenchmarkResult
	}{
		{"error", "operation_error", func() error { return errors.New("controlled") }, testing.Benchmark},
		{"panic", "operation_panic", func() error { panic("not logged") }, testing.Benchmark},
		{"missing", "operation_count", func() error { t.Fatal("must not execute"); return nil }, func(func(*testing.B)) testing.BenchmarkResult { return testing.BenchmarkResult{} }},
		{"wrong_n", "incomplete_result", func() error { return nil }, func(f func(*testing.B)) testing.BenchmarkResult { r := testing.Benchmark(f); r.N = 2; return r }},
		{"duplicate_callback", "operation_count", func() error { return nil }, func(f func(*testing.B)) testing.BenchmarkResult { testing.Benchmark(f); return testing.Benchmark(f) }},
		{"fatal", "operation_count", func() error { return nil }, func(func(*testing.B)) testing.BenchmarkResult {
			return testing.Benchmark(func(b *testing.B) { b.Fatal("controlled benchmark failure") })
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			s, category := pilotMeasure(tc.op, tc.backend)
			if category != tc.category || s.Valid || s.Started > 1 {
				t.Fatalf("failure lost: %+v category=%s", s, category)
			}
		})
	}
}

func TestPilotRejectsUnboundedMeasurement(t *testing.T) {
	pilotOneIteration(t)
	_ = flag.Set("test.benchtime", "2x")
	s, category := pilotMeasure(func() error { t.Fatal("must not execute"); return nil }, testing.Benchmark)
	if category != "benchtime_not_1x" || s.Started != 0 {
		t.Fatal("unbounded measurement accepted")
	}
}

func TestPilotLedgerContract(t *testing.T) {
	pilotOneIteration(t)
	for _, failing := range []bool{false, true} {
		var ledger, samples bytes.Buffer
		d := pilotDriver{RunID: "contract", FixtureID: "contract/N2", Origin: time.Now(),
			Plan: []pilotSpec{{ID: "one", Kind: "fake", Timed: true}}, Ledger: &ledger, Samples: &samples}
		calls := 0
		err := d.Do("one", func() error {
			calls++
			if failing {
				return errors.New("controlled")
			}
			return nil
		})
		if (err != nil) != failing || calls != 1 || len(d.Operations) != 1 || d.Complete() == failing || ledger.Len() == 0 || samples.Len() == 0 {
			t.Fatal("ledger lost success/failure or evidence")
		}
		if err := d.Do("one", func() error { calls++; return nil }); err == nil || calls != 1 {
			t.Fatal("duplicate operation executed")
		}
	}
	var out bytes.Buffer
	d := pilotDriver{RunID: "contract", Origin: time.Now(), Plan: []pilotSpec{{ID: "one"}}, Ledger: &out}
	if d.Complete() || d.Do("unexpected", func() error { t.Fatal("must not execute"); return nil }) == nil {
		t.Fatal("incomplete/order violation accepted")
	}
}
