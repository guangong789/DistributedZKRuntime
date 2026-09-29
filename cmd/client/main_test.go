package main

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"testing"

	"github.com/guangong789/DistributedZKRuntime/internal/zk"
)

func TestBuildPreimagePayloadContainsOnlyPublicFields(t *testing.T) {
	digest := zk.ComputePreimageDigest(42)
	var digestInt big.Int
	digest.BigInt(&digestInt)
	if demoPreimageDigest != digestInt.String() {
		t.Fatalf("demo digest=%s, want digest for the demo witness", demoPreimageDigest)
	}

	payload, err := buildPreimagePayload("secret-001", demoPreimageDigest)
	if err != nil {
		t.Fatalf("build preimage payload: %v", err)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal([]byte(payload), &fields); err != nil {
		t.Fatalf("decode payload: %v", err)
	}
	if len(fields) != 2 {
		t.Fatalf("payload fields=%v, want only witness_ref and digest", fields)
	}
	for key, want := range map[string]string{
		"witness_ref": "secret-001",
		"digest":      digestInt.String(),
	} {
		raw, ok := fields[key]
		if !ok {
			t.Fatalf("missing payload field %q: %v", key, fields)
		}
		var got string
		if err := json.Unmarshal(raw, &got); err != nil || got != want {
			t.Fatalf("payload field %q=%q error=%v, want %q", key, got, err, want)
		}
	}
	for _, privateKey := range []string{"secret", "preimage", "private_witness"} {
		if _, present := fields[privateKey]; present {
			t.Fatalf("private field %q appeared in payload", privateKey)
		}
	}
}

func TestProofByteLength(t *testing.T) {
	for _, tc := range []struct {
		name   string
		output string
		want   int
	}{
		{name: "proof", output: base64.StdEncoding.EncodeToString([]byte{1, 2, 3}), want: 3},
		{name: "empty", output: "", want: 0},
		{name: "non-proof", output: "not base64!", want: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := proofByteLength(tc.output); got != tc.want {
				t.Fatalf("proofByteLength(%q)=%d, want %d", tc.output, got, tc.want)
			}
		})
	}
}
