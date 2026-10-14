package runtime

import (
	"context"
	"encoding/base64"
	"testing"
)

// BenchmarkRuntimeExecuteJob includes payload parsing, witness lookup, capacity
// admission, real proving, Base64 encoding, deadline gates, and Result creation.
// Setup and independent proof verification are excluded. Metrics use the normal
// nil-observer/Noop path; no exporter is running. Calls are sequential.
func BenchmarkRuntimeExecuteJob(b *testing.B) {
	task, verifier, digest := newPreimageTaskFixture(b)
	limiter, err := NewProvingLimiter(1)
	if err != nil {
		b.Fatal(err)
	}
	task.Limiter = limiter
	job := Job{ID: 601, Task: task, Status: JobRunning}
	// ExecuteJob accepts a live context without a deadline. Background cannot
	// expire during benchmark calibration; no task deadline is bypassed or reset.
	ctx := context.Background()
	check := func(result Result) {
		if result.JobID != job.ID || result.Status != JobSucceeded || result.Err != nil || result.Output == "" {
			b.Fatalf("unexpected runtime result: id=%d status=%v error=%v output_bytes=%d", result.JobID, result.Status, result.Err, len(result.Output))
		}
	}
	verify := func(result Result) {
		proof, err := base64.StdEncoding.DecodeString(result.Output)
		if err != nil || len(proof) == 0 {
			b.Fatalf("decode proof: error=%v bytes=%d", err, len(proof))
		}
		if err := verifier.Verify(proof, digest); err != nil {
			b.Fatalf("verify runtime output: %v", err)
		}
	}
	for i := 0; i < 3; i++ {
		result := ExecuteJob(ctx, job)
		check(result)
		verify(result)
	}
	var result Result
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		result = ExecuteJob(ctx, job)
		check(result)
	}
	b.StopTimer()
	verify(result)
}
