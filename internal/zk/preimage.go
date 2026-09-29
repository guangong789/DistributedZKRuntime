package zk

import (
	"bytes"
	"math/big"
	"os"

	"github.com/consensys/gnark-crypto/ecc"
	"github.com/consensys/gnark-crypto/ecc/bn254/fr"
	nativeMiMC "github.com/consensys/gnark-crypto/ecc/bn254/fr/mimc"

	"github.com/consensys/gnark/backend/groth16"
	"github.com/consensys/gnark/constraint"
	"github.com/consensys/gnark/frontend"
	"github.com/consensys/gnark/frontend/cs/r1cs"
	"github.com/consensys/gnark/std/hash/mimc"
)

type PreimageCircuit struct {
	Preimage frontend.Variable
	Hash     frontend.Variable `gnark:",public"`
}

type PreimageProver struct {
	ccs constraint.ConstraintSystem
	pk  groth16.ProvingKey
}

type PreimageVerifier struct {
	vk groth16.VerifyingKey
}

func (c *PreimageCircuit) Define(api frontend.API) error {
	hasher, err := mimc.NewMiMC(api)
	if err != nil {
		return err
	}

	hasher.Write(c.Preimage)

	digest := hasher.Sum()

	api.AssertIsEqual(
		digest,
		c.Hash,
	)

	return nil
}

func compilePreimageCircuit() (constraint.ConstraintSystem, error) {
	var circuit PreimageCircuit

	return frontend.Compile(
		ecc.BN254.ScalarField(),
		r1cs.NewBuilder,
		&circuit,
	)
}

func GeneratePreimageSetup(provingKeyPath, verifyingKeyPath string) error {
	ccs, err := compilePreimageCircuit()
	if err != nil {
		return err
	}

	pk, vk, err := groth16.Setup(ccs)
	if err != nil {
		return err
	}

	pkFile, err := os.Create(provingKeyPath)
	if err != nil {
		return err
	}
	defer pkFile.Close()

	if _, err := pk.WriteTo(pkFile); err != nil {
		return err
	}

	vkFile, err := os.Create(verifyingKeyPath)
	if err != nil {
		return err
	}
	defer vkFile.Close()

	if _, err := vk.WriteTo(vkFile); err != nil {
		return err
	}

	return nil
}

func LoadPreimageProver(provingKeyPath string) (*PreimageProver, error) {
	ccs, err := compilePreimageCircuit()
	if err != nil {
		return nil, err
	}

	file, err := os.Open(provingKeyPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	pk := groth16.NewProvingKey(ecc.BN254)

	if _, err := pk.ReadFrom(file); err != nil {
		return nil, err
	}

	return &PreimageProver{
		ccs: ccs,
		pk:  pk,
	}, nil
}

func LoadPreimageVerifier(verifyingKeyPath string) (*PreimageVerifier, error) {
	file, err := os.Open(verifyingKeyPath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	vk := groth16.NewVerifyingKey(ecc.BN254)

	if _, err := vk.ReadFrom(file); err != nil {
		return nil, err
	}

	return &PreimageVerifier{
		vk: vk,
	}, nil
}

func (p *PreimageProver) Prove(secret int64, digest fr.Element) ([]byte, error) {
	var digestBigInt big.Int
	digest.BigInt(&digestBigInt)

	assignment := PreimageCircuit{
		Preimage: big.NewInt(secret),
		Hash:     &digestBigInt,
	}

	witness, err := frontend.NewWitness(
		&assignment,
		ecc.BN254.ScalarField(),
	)
	if err != nil {
		return nil, err
	}

	proof, err := groth16.Prove(
		p.ccs,
		p.pk,
		witness,
	)
	if err != nil {
		return nil, err
	}

	var buf bytes.Buffer

	if _, err := proof.WriteTo(&buf); err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

func (v *PreimageVerifier) Verify(proofBytes []byte, digest fr.Element) error {
	proof := groth16.NewProof(ecc.BN254)

	if _, err := proof.ReadFrom(bytes.NewReader(proofBytes)); err != nil {
		return err
	}

	var digestBigInt big.Int
	digest.BigInt(&digestBigInt)

	assignment := PreimageCircuit{
		Hash: &digestBigInt,
	}

	witness, err := frontend.NewWitness(
		&assignment,
		ecc.BN254.ScalarField(),
		frontend.PublicOnly(),
	)
	if err != nil {
		return err
	}

	return groth16.Verify(
		proof,
		v.vk,
		witness,
	)
}

func ComputePreimageDigest(secret int64) fr.Element {
	var element fr.Element
	element.SetInt64(secret)

	hasher := nativeMiMC.NewFieldHasher()

	return hasher.SumElements([]fr.Element{element})
}
