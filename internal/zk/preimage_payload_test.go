package zk

import (
	"encoding/json"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
)

func TestPreimagePublicPayloadRejectsAmbiguity(t *testing.T) {
	const marker = "PRIVATE_VALUE_MARKER"
	for _, tc := range []struct{ name, raw string }{
		{"secret", `{"witness_ref":"ref","digest":"1","secret":"` + marker + `"}`},
		{"witness", `{"witness_ref":"ref","digest":"1","witness":"` + marker + `"}`},
		{"private key", `{"witness_ref":"ref","digest":"1","private_key":"` + marker + `"}`},
		{"private input", `{"witness_ref":"ref","digest":"1","private_input":42}`},
		{"metadata", `{"witness_ref":"ref","digest":"1","metadata":{}}`},
		{"private field name", `{"witness_ref":"ref","digest":"1","` + marker + `":"value"}`},
		{"duplicate ref", `{"witness_ref":"ref","witness_ref":"other","digest":"1"}`},
		{"duplicate digest", `{"witness_ref":"ref","digest":"1","digest":"2"}`},
		{"escaped duplicate", `{"witness_ref":"ref","digest":"1","di\u0067est":"2"}`},
		{"case ref", `{"Witness_Ref":"ref","digest":"1"}`},
		{"case digest", `{"witness_ref":"ref","DIGEST":"1"}`},
		{"missing ref", `{"digest":"1"}`}, {"missing digest", `{"witness_ref":"ref"}`},
		{"null ref", `{"witness_ref":null,"digest":"1"}`}, {"null digest", `{"witness_ref":"ref","digest":null}`},
		{"number ref", `{"witness_ref":42,"digest":"1"}`}, {"number digest", `{"witness_ref":"ref","digest":42}`},
		{"boolean", `{"witness_ref":true,"digest":"1"}`}, {"array field", `{"witness_ref":[],"digest":"1"}`},
		{"object field", `{"witness_ref":"ref","digest":{}}`},
		{"empty ref", `{"witness_ref":"","digest":"1"}`}, {"space ref", `{"witness_ref":" \t\n\u2003","digest":"1"}`},
		{"empty digest", `{"witness_ref":"ref","digest":""}`},
		{"broken", `{"witness_ref":"ref","digest":"1"`}, {"trailing comma", `{"witness_ref":"ref","digest":"1",}`},
		{"trailing object", `{"witness_ref":"ref","digest":"1"}{}`},
		{"trailing null", `{"witness_ref":"ref","digest":"1"}null`},
		{"trailing garbage", `{"witness_ref":"ref","digest":"1"}PRIVATE_VALUE_MARKER`},
		{"root array", `[]`}, {"root number", `42`}, {"root string", `"` + marker + `"`}, {"root null", `null`}, {"empty", ``},
		{"digest letters", `{"witness_ref":"ref","digest":"` + marker + `"}`},
		{"digest spaces", `{"witness_ref":"ref","digest":" 1 "}`},
		{"digest hex", `{"witness_ref":"ref","digest":"0x10"}`},
		{"digest exponent", `{"witness_ref":"ref","digest":"1e2"}`},
		{"digest separators", `{"witness_ref":"ref","digest":"1_000"}`},
		{"digest decimal point", `{"witness_ref":"ref","digest":"1.0"}`},
		{"digest sign only", `{"witness_ref":"ref","digest":"+"}`},
		{"invalid UTF8", string([]byte{'{', '"', 0xff, '"', ':', '1', '}'})},
		{"lone high surrogate", `{"witness_ref":"\ud800","digest":"1"}`},
		{"lone low surrogate", `{"witness_ref":"\udc00","digest":"1"}`},
		{"wrong surrogate pair", `{"witness_ref":"\ud800\u0041","digest":"1"}`},
		{"deep unknown value", `{"witness_ref":"ref","digest":"1","secret":` + strings.Repeat("[", 500) + `"` + marker + `"` + strings.Repeat("]", 500) + `}`},
		{"large", strings.Repeat(" ", MaxPreimagePayloadBytes+1)},
		{"canonical escape expansion", `{"witness_ref":"` + strings.Repeat("<", 700) + `","digest":"1"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			_, err := ParsePreimagePublicPayload(tc.raw)
			if !errors.Is(err, ErrInvalidPreimagePayload) {
				t.Fatal("invalid input not rejected with payload error")
			}
			if strings.Contains(err.Error(), marker) {
				t.Fatal("error disclosed untrusted data")
			}
		})
	}
}

func TestPreimagePublicPayloadCanonicalAndLegacyDigestSemantics(t *testing.T) {
	modulus := fr.Modulus()
	minusOne := new(big.Int).Sub(modulus, big.NewInt(1)).String()
	above := new(big.Int).Add(modulus, big.NewInt(42)).String()
	for _, tc := range []struct{ raw, want string }{
		{"0", "0"}, {"00042", "42"}, {"+00042", "42"}, {"-0", "0"}, {"-1", minusOne},
		{modulus.String(), "0"}, {above, "42"}, {"-" + above, new(big.Int).Sub(modulus, big.NewInt(42)).String()},
	} {
		input := ` { "digest" : "` + tc.raw + `", "witness_ref" : " /local:ref<&> \ud83d\ude00 " } `
		validated, err := ParsePreimagePublicPayload(input)
		if err != nil {
			t.Fatal(err)
		}
		var record PreimagePublicPayload
		if err := json.Unmarshal([]byte(validated.CanonicalJSON), &record); err != nil {
			t.Fatal(err)
		}
		if record.Digest != tc.want || record.WitnessRef != " /local:ref<&> 😀 " || validated.WitnessRef != record.WitnessRef {
			t.Fatal("digest or exact witness reference changed")
		}
		var integer big.Int
		integer.SetString(tc.raw, 10)
		var original fr.Element
		original.SetBigInt(&integer)
		if !validated.Digest.Equal(&original) {
			t.Fatal("canonicalization changed mathematical digest")
		}
		repeated, err := ParsePreimagePublicPayload(validated.CanonicalJSON)
		if err != nil || repeated.CanonicalJSON != validated.CanonicalJSON {
			t.Fatal("canonical representation is not idempotent")
		}
		fields := map[string]json.RawMessage{}
		if err := json.Unmarshal([]byte(validated.CanonicalJSON), &fields); err != nil || len(fields) != 2 {
			t.Fatal("canonical representation leaked extra fields")
		}
	}
	// Literal replacement characters and an escaped backslash are valid refs;
	// only lossy unpaired Unicode surrogates are rejected.
	for _, ref := range []string{"�", `\ud800`, "secret-001"} {
		data, _ := json.Marshal(PreimagePublicPayload{WitnessRef: ref, Digest: "123"})
		parsed, err := ParsePreimagePublicPayload(string(data))
		if err != nil || parsed.WitnessRef != ref {
			t.Fatal("valid reference encoding rejected")
		}
	}
}

func TestPreimagePublicPayloadSizeBoundary(t *testing.T) {
	base := `{"witness_ref":"","digest":"1"}`
	raw := `{"witness_ref":"` + strings.Repeat("a", MaxPreimagePayloadBytes-len(base)) + `","digest":"1"}`
	if len(raw) != MaxPreimagePayloadBytes {
		t.Fatal("incorrect test boundary")
	}
	if _, err := ParsePreimagePublicPayload(raw); err != nil {
		t.Fatal("exact size limit rejected")
	}
	if _, err := ParsePreimagePublicPayload(raw + " "); err == nil {
		t.Fatal("over-size payload accepted")
	}
}
