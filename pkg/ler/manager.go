package ler

import (
	"context"
	"github.com/iden3/go-merkletree-sql"
	"github.com/iden3/go-merkletree-sql/db/memory"
	"strings"
)

// LERManager wraps a Sparse Merkle Tree for ExitLeafs.
type LERManager struct {
	tree *merkletree.MerkleTree
}

func NewLERManager(ctx context.Context) (*LERManager, error) {
	db := memory.NewMemoryStorage()
	tree, err := merkletree.NewMerkleTree(ctx, db, 32) // height = 32
	if err != nil {
		return nil, err
	}
	return &LERManager{tree: tree}, nil
}

// AddExit inserts a Poseidon(leaf) => Poseidon(leaf) into the SMT.
func (m *LERManager) AddExit(ctx context.Context, leaf ExitLeaf) error {
	key := leaf.ToKey()
	val := key // LER uses key = value
	err := m.tree.Add(ctx, key, val)
	if err != nil && strings.Contains(err.Error(), "already exists") {
		return nil // silently ignore duplicate for now
	}
	return err
}

// Root returns the current SMT root as a [32]byte.
func (m *LERManager) Root() [32]byte {
	root := m.tree.Root()
	var r [32]byte
	copy(r[:], root.BigInt().Bytes())
	return r
}
