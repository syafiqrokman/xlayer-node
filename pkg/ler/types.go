package ler

import (
	"math/big"

	poseidon "github.com/iden3/go-iden3-crypto/poseidon"
)

// ExitLeaf represents a cross-chain message leaf in LER.
type ExitLeaf struct {
	OriginNetwork      uint64
	DestinationNetwork uint64
	OriginAddress      [32]byte
	DestinationAddress [32]byte
	Amount             *big.Int
	Metadata           [32]byte
}

var FieldModulus, _ = new(big.Int).SetString("21888242871839275222246405745257275088548364400416034343698204186575808495617", 10)

// ToKey hashes the ExitLeaf to a Poseidon hash.
func (e ExitLeaf) ToKey() *big.Int {
	const maxLen = 31 // BN254 requires < 254 bits

	truncate := func(b [32]byte) *big.Int {
		return new(big.Int).SetBytes(b[:maxLen])
	}

	inputs := []*big.Int{
		new(big.Int).SetUint64(e.OriginNetwork),
		new(big.Int).SetUint64(e.DestinationNetwork),
		truncate(e.OriginAddress),
		truncate(e.DestinationAddress),
		new(big.Int).Mod(e.Amount, FieldModulus), // optional, to be extra safe
		truncate(e.Metadata),
	}

	hash, err := poseidon.Hash(inputs)
	if err != nil {
		panic("poseidon hash failed: " + err.Error())
	}
	return hash
}
