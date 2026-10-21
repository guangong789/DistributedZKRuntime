package mimcchain

import (
	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	"github.com/consensys/gnark-crypto/ecc/bn254/fr/mimc"
)

// ComputeDigest maps secret to the scalar field and applies n independently
// initialized, single-element MiMC hashes. The hasher belongs to this call;
// concurrent reference computations do not share mutable hash state.
func ComputeDigest(secret int64, n int) (fr.Element, error) {
	if err := validateLength(n); err != nil {
		return fr.Element{}, err
	}
	var value fr.Element
	value.SetInt64(secret)
	h := mimc.NewFieldHasher()
	for i := 0; i < n; i++ {
		h.Reset()
		value = h.SumElements([]fr.Element{value})
	}
	return value, nil
}
