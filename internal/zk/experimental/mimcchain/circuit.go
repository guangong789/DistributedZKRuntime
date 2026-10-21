// Package mimcchain contains an experimental BN254 MiMC hash chain. It is not
// connected to production task dispatch, payloads, or key loading.
package mimcchain

import (
	"fmt"

	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/std/hash/mimc"
)

// MaxLength bounds construction in this experiment. Only 1, 2, 8, and 32 are
// supported; 32 is reserved for compile-only calibration in this milestone.
const MaxLength = 32

// Circuit proves H applied N times to Secret equals Digest. Secret is a field
// element; it is not range-constrained to int64. N changes the compiled circuit
// and requires matching setup material. It is not part of either witness.
type Circuit struct {
	Secret frontend.Variable
	Digest frontend.Variable `gnark:",public"`
	N      int               `gnark:"-"`
}

func validateLength(n int) error {
	if n <= 0 || n > MaxLength {
		return fmt.Errorf("MiMC chain length must be positive and at most %d", MaxLength)
	}
	switch n {
	case 1, 2, 8, 32:
		return nil
	default:
		return fmt.Errorf("unsupported MiMC chain length: use 1, 2, 8, or 32")
	}
}

// NewCircuit validates the fixed construction parameter before compilation.
// Each compilation must receive a fresh circuit instance.
func NewCircuit(n int) (*Circuit, error) {
	if err := validateLength(n); err != nil {
		return nil, err
	}
	return &Circuit{N: n}, nil
}

func (c *Circuit) Define(api frontend.API) error {
	// Also reject direct struct construction and the zero-value configuration.
	if err := validateLength(c.N); err != nil {
		return err
	}
	h, err := mimc.NewMiMC(api)
	if err != nil {
		return err
	}
	value := c.Secret
	for i := 0; i < c.N; i++ {
		// Sum flushes inputs but retains state. Each link is a fresh full hash
		// of one field element, rather than continued multi-element absorption.
		h.Reset()
		h.Write(value)
		value = h.Sum()
	}
	api.AssertIsEqual(value, c.Digest)
	return nil
}
