package zk

import (
	"bytes"
	"fmt"
	"os"
	"sync"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark/backend/groth16"
	"github.com/consensys/gnark/constraint"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
)

type SquareCircuit struct {
	X frontend.Variable
	Y frontend.Variable `gnark:",public"`
}

type SquareProver struct {
	ccs constraint.ConstraintSystem
	pk  groth16.ProvingKey
}

type SquareVerifier struct {
	vk groth16.VerifyingKey
}

func (c *SquareCircuit) Define(api frontend.API) error {
	api.AssertIsEqual(
		api.Mul(c.X, c.X),
		c.Y,
	)

	return nil
}

var (
	squareOnce sync.Once
	squareCCS  constraint.ConstraintSystem
	squarePK   groth16.ProvingKey
	squareVK   groth16.VerifyingKey
	squareErr  error
)

func initSquare() error {
	squareOnce.Do(func() {
		var circuit SquareCircuit

		squareCCS, squareErr = frontend.Compile(
			ecc.BN254.ScalarField(),
			r1cs.NewBuilder,
			&circuit,
		)
		if squareErr != nil {
			return
		}

		squarePK, squareVK, squareErr = groth16.Setup(squareCCS)
	})

	return squareErr
}

func ProveSquare(x, y int64) ([]byte, error) {
	if err := initSquare(); err != nil {
		return nil, fmt.Errorf("initialize square circuit: %w", err)
	}

	assignment := SquareCircuit{
		X: x,
		Y: y,
	}

	witness, err := frontend.NewWitness(
		&assignment,
		ecc.BN254.ScalarField(),
	)
	if err != nil {
		return nil, fmt.Errorf("create witness: %w", err)
	}

	proof, err := groth16.Prove(
		squareCCS,
		squarePK,
		witness,
	)
	if err != nil {
		return nil, fmt.Errorf("prove square: %w", err)
	}

	var buf bytes.Buffer

	if _, err := proof.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("serialize proof: %w", err)
	}

	return buf.Bytes(), nil
}

func VerifySquare(
	proofBytes []byte,
	y int64,
) error {
	if err := initSquare(); err != nil {
		return fmt.Errorf("initialize square circuit: %w", err)
	}

	proof := groth16.NewProof(ecc.BN254)

	if _, err := proof.ReadFrom(
		bytes.NewReader(proofBytes),
	); err != nil {
		return fmt.Errorf("deserialize proof: %w", err)
	}

	publicAssignment := SquareCircuit{
		Y: y,
	}

	publicWitness, err := frontend.NewWitness(
		&publicAssignment,
		ecc.BN254.ScalarField(),
		frontend.PublicOnly(),
	)
	if err != nil {
		return fmt.Errorf("create public witness: %w", err)
	}

	if err := groth16.Verify(
		proof,
		squareVK,
		publicWitness,
	); err != nil {
		return fmt.Errorf("verify square proof: %w", err)
	}

	return nil
}

func compileSquareCircuit() (constraint.ConstraintSystem, error) {
	var circuit SquareCircuit

	ccs, err := frontend.Compile(
		ecc.BN254.ScalarField(),
		r1cs.NewBuilder,
		&circuit,
	)
	if err != nil {
		return nil, fmt.Errorf("compile square circuit: %w", err)
	}

	return ccs, nil
}

func GenerateSquareSetup(
	provingKeyPath string,
	verifyingKeyPath string,
) error {
	ccs, err := compileSquareCircuit()
	if err != nil {
		return err
	}

	pk, vk, err := groth16.Setup(ccs)
	if err != nil {
		return fmt.Errorf("groth16 setup: %w", err)
	}

	pkFile, err := os.Create(provingKeyPath)
	if err != nil {
		return fmt.Errorf("create proving key file: %w", err)
	}
	defer pkFile.Close()

	if _, err := pk.WriteTo(pkFile); err != nil {
		return fmt.Errorf("write proving key: %w", err)
	}

	vkFile, err := os.Create(verifyingKeyPath)
	if err != nil {
		return fmt.Errorf("create verifying key file: %w", err)
	}
	defer vkFile.Close()

	if _, err := vk.WriteTo(vkFile); err != nil {
		return fmt.Errorf("write verifying key: %w", err)
	}

	return nil
}

func LoadSquareProver(
	provingKeyPath string,
) (*SquareProver, error) {
	ccs, err := compileSquareCircuit()
	if err != nil {
		return nil, err
	}

	file, err := os.Open(provingKeyPath)
	if err != nil {
		return nil, fmt.Errorf("open proving key: %w", err)
	}
	defer file.Close()

	pk := groth16.NewProvingKey(ecc.BN254)

	if _, err := pk.ReadFrom(file); err != nil {
		return nil, fmt.Errorf("read proving key: %w", err)
	}

	return &SquareProver{
		ccs: ccs,
		pk:  pk,
	}, nil
}

func LoadSquareVerifier(
	verifyingKeyPath string,
) (*SquareVerifier, error) {
	file, err := os.Open(verifyingKeyPath)
	if err != nil {
		return nil, fmt.Errorf("open verifying key: %w", err)
	}
	defer file.Close()

	vk := groth16.NewVerifyingKey(ecc.BN254)

	if _, err := vk.ReadFrom(file); err != nil {
		return nil, fmt.Errorf("read verifying key: %w", err)
	}

	return &SquareVerifier{
		vk: vk,
	}, nil
}

func (p *SquareProver) Prove(
	x int64,
	y int64,
) ([]byte, error) {
	assignment := SquareCircuit{
		X: x,
		Y: y,
	}

	witness, err := frontend.NewWitness(
		&assignment,
		ecc.BN254.ScalarField(),
	)
	if err != nil {
		return nil, fmt.Errorf("create witness: %w", err)
	}

	proof, err := groth16.Prove(
		p.ccs,
		p.pk,
		witness,
	)
	if err != nil {
		return nil, fmt.Errorf("prove square: %w", err)
	}

	var buf bytes.Buffer

	if _, err := proof.WriteTo(&buf); err != nil {
		return nil, fmt.Errorf("serialize proof: %w", err)
	}

	return buf.Bytes(), nil
}

func (v *SquareVerifier) Verify(
	proofBytes []byte,
	y int64,
) error {
	proof := groth16.NewProof(ecc.BN254)

	if _, err := proof.ReadFrom(
		bytes.NewReader(proofBytes),
	); err != nil {
		return fmt.Errorf("deserialize proof: %w", err)
	}

	publicAssignment := SquareCircuit{
		Y: y,
	}

	publicWitness, err := frontend.NewWitness(
		&publicAssignment,
		ecc.BN254.ScalarField(),
		frontend.PublicOnly(),
	)
	if err != nil {
		return fmt.Errorf("create public witness: %w", err)
	}

	if err := groth16.Verify(
		proof,
		v.vk,
		publicWitness,
	); err != nil {
		return fmt.Errorf("verify square proof: %w", err)
	}

	return nil
}
