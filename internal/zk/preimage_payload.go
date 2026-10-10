package zk

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
)

// The public schema needs a field element and a local witness reference, not a
// witness. Bound both incoming and canonical JSON, including escape expansion.
const MaxPreimagePayloadBytes = 4 * 1024

var ErrInvalidPreimagePayload = errors.New("invalid ZK public payload")

type PreimagePublicPayload struct {
	WitnessRef string `json:"witness_ref"`
	Digest     string `json:"digest"`
}

type ValidatedPreimagePayload struct {
	WitnessRef    string
	Digest        fr.Element
	CanonicalJSON string
}

func invalidPreimagePayload(reason string) error {
	// Reasons are fixed diagnostics. Never include supplied keys, values or JSON
	// decoder errors: unknown field names and wrong-type values can be secrets.
	return fmt.Errorf("%w: %s", ErrInvalidPreimagePayload, reason)
}

func ParsePreimagePublicPayload(raw string) (ValidatedPreimagePayload, error) {
	var result ValidatedPreimagePayload
	if len(raw) > MaxPreimagePayloadBytes {
		return result, invalidPreimagePayload("payload too large")
	}
	if !utf8.ValidString(raw) {
		return result, invalidPreimagePayload("invalid JSON encoding")
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	root, err := decoder.Token()
	if err != nil || root != json.Delim('{') {
		return result, invalidPreimagePayload("expected one JSON object")
	}
	var public PreimagePublicPayload
	var seen uint8
	for decoder.More() {
		key, err := decoder.Token()
		if err != nil {
			return result, invalidPreimagePayload("malformed JSON")
		}
		var bit uint8
		var target *string
		switch key {
		case "witness_ref":
			bit, target = 1, &public.WitnessRef
		case "digest":
			bit, target = 2, &public.Digest
		default:
			return result, invalidPreimagePayload("unknown public payload field")
		}
		if seen&bit != 0 {
			return result, invalidPreimagePayload("duplicate public payload field")
		}
		seen |= bit
		var value json.RawMessage
		if err := decoder.Decode(&value); err != nil {
			return result, invalidPreimagePayload("malformed JSON")
		}
		if len(value) == 0 || value[0] != '"' {
			return result, invalidPreimagePayload("public payload fields must be strings")
		}
		if err := json.Unmarshal(value, target); err != nil || !validPublicStringEscapes(value) {
			return result, invalidPreimagePayload("invalid public payload string")
		}
	}
	if close, err := decoder.Token(); err != nil || close != json.Delim('}') {
		return result, invalidPreimagePayload("malformed JSON")
	}
	if _, err := decoder.Token(); err != io.EOF {
		return result, invalidPreimagePayload("trailing JSON data")
	}
	if seen&1 == 0 {
		return result, invalidPreimagePayload("missing witness_ref")
	}
	if seen&2 == 0 {
		return result, invalidPreimagePayload("missing digest")
	}
	if strings.TrimSpace(public.WitnessRef) == "" {
		return result, invalidPreimagePayload("empty witness_ref")
	}
	// Keep legacy signed/zero-padded decimal and modulo-field semantics. Spaces,
	// exponents, hex and separators are not decimal digest representations.
	digits := public.Digest
	if len(digits) > 0 && (digits[0] == '+' || digits[0] == '-') {
		digits = digits[1:]
	}
	if digits == "" {
		return result, invalidPreimagePayload("invalid digest format")
	}
	for _, c := range digits {
		if c < '0' || c > '9' {
			return result, invalidPreimagePayload("invalid digest format")
		}
	}
	var integer big.Int
	if _, ok := integer.SetString(public.Digest, 10); !ok {
		return result, invalidPreimagePayload("invalid digest format")
	}
	result.Digest.SetBigInt(&integer)
	result.Digest.BigInt(&integer)
	public.Digest = integer.String()
	encoded, err := json.Marshal(public)
	if err != nil {
		return result, invalidPreimagePayload("invalid public payload string")
	}
	if len(encoded) > MaxPreimagePayloadBytes {
		return result, invalidPreimagePayload("canonical payload too large")
	}
	result.WitnessRef, result.CanonicalJSON = public.WitnessRef, string(encoded)
	return result, nil
}

// encoding/json replaces lone UTF-16 surrogates with U+FFFD. Reject that lossy
// representation while retaining valid Unicode, including literal U+FFFD.
// json.Unmarshal has already checked JSON string syntax before this is called.
func validPublicStringEscapes(raw []byte) bool {
	for i := 1; i < len(raw)-1; i++ {
		if raw[i] != '\\' {
			continue
		}
		i++
		if raw[i] != 'u' {
			continue
		}
		code, _ := strconv.ParseUint(string(raw[i+1:i+5]), 16, 16)
		i += 4
		switch {
		case code >= 0xd800 && code <= 0xdbff:
			if i+7 >= len(raw) || raw[i+1] != '\\' || raw[i+2] != 'u' {
				return false
			}
			low, _ := strconv.ParseUint(string(raw[i+3:i+7]), 16, 16)
			if low < 0xdc00 || low > 0xdfff {
				return false
			}
			i += 6
		case code >= 0xdc00 && code <= 0xdfff:
			return false
		}
	}
	return true
}
