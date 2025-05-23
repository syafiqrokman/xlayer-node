package ler

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestLERManager_AddExitAndRoot(t *testing.T) {
	ctx := context.Background()

	// Create LERManager
	lerMgr, err := NewLERManager(ctx)
	require.NoError(t, err)

	// Sample leaf
	leaf := ExitLeaf{
		OriginNetwork:      1,
		DestinationNetwork: 137,
		OriginAddress:      [32]byte{0xaa},
		DestinationAddress: [32]byte{0xbb},
		Amount:             big.NewInt(1000),
		Metadata:           [32]byte{0xcc},
	}

	// Add to tree
	err = lerMgr.AddExit(ctx, leaf)
	require.NoError(t, err)

	// Fetch root
	root := lerMgr.Root()
	t.Logf("SMT Root: 0x%x", root)
	require.NotEqual(t, [32]byte{}, root)
}

func TestLERManager_MultipleExits(t *testing.T) {
	ctx := context.Background()
	lerMgr, err := NewLERManager(ctx)
	require.NoError(t, err)

	initialRoot := lerMgr.Root()

	// Create multiple test leaves
	leaves := []ExitLeaf{
		{
			OriginNetwork:      1,
			DestinationNetwork: 137,
			OriginAddress:      [32]byte{0x1},
			DestinationAddress: [32]byte{0x2},
			Amount:             big.NewInt(1000),
			Metadata:           [32]byte{0x3},
		},
		{
			OriginNetwork:      2,
			DestinationNetwork: 138,
			OriginAddress:      [32]byte{0x4},
			DestinationAddress: [32]byte{0x5},
			Amount:             big.NewInt(2000),
			Metadata:           [32]byte{0x6},
		},
	}

	// Add all leaves
	for _, leaf := range leaves {
		err := lerMgr.AddExit(ctx, leaf)
		require.NoError(t, err)
	}

	finalRoot := lerMgr.Root()
	require.NotEqual(t, initialRoot, finalRoot)
}

func TestLERManager_DuplicateExit(t *testing.T) {
	ctx := context.Background()
	lerMgr, err := NewLERManager(ctx)
	require.NoError(t, err)

	leaf := ExitLeaf{
		OriginNetwork:      1,
		DestinationNetwork: 137,
		OriginAddress:      [32]byte{0x1},
		DestinationAddress: [32]byte{0x2},
		Amount:             big.NewInt(1000),
		Metadata:           [32]byte{0x3},
	}

	// Add leaf first time
	err = lerMgr.AddExit(ctx, leaf)
	require.NoError(t, err)
	firstRoot := lerMgr.Root()

	// Add same leaf again
	err = lerMgr.AddExit(ctx, leaf)
	require.NoError(t, err)
	secondRoot := lerMgr.Root()

	// Root should be the same as we're adding identical key-value pair
	require.Equal(t, firstRoot, secondRoot)
}

func TestLERManager_LargeValues(t *testing.T) {
	ctx := context.Background()
	lerMgr, err := NewLERManager(ctx)
	require.NoError(t, err)

	// Create large values
	largeAmount := new(big.Int)
	largeAmount.SetString("115792089237316195423570985008687907853269984665640564039457584007913129639935", 10) // 2^256 - 1

	leaf := ExitLeaf{
		OriginNetwork:      1<<64 - 1,                        // max uint64
		DestinationNetwork: 1<<64 - 1,                        // max uint64
		OriginAddress:      [32]byte{0xff, 0xff, 0xff, 0xff}, // some large bytes
		DestinationAddress: [32]byte{0xff, 0xff, 0xff, 0xff}, // some large bytes
		Amount:             largeAmount,
		Metadata:           [32]byte{0xff, 0xff, 0xff, 0xff}, // some large bytes
	}

	err = lerMgr.AddExit(ctx, leaf)
	require.NoError(t, err)

	root := lerMgr.Root()
	require.NotEqual(t, [32]byte{}, root)
}

func TestLERManager_ZeroValues(t *testing.T) {
	ctx := context.Background()
	lerMgr, err := NewLERManager(ctx)
	require.NoError(t, err)

	// Test with zero values
	leaf := ExitLeaf{
		OriginNetwork:      0,
		DestinationNetwork: 0,
		OriginAddress:      [32]byte{},
		DestinationAddress: [32]byte{},
		Amount:             big.NewInt(0),
		Metadata:           [32]byte{},
	}

	err = lerMgr.AddExit(ctx, leaf)
	require.NoError(t, err)

	root := lerMgr.Root()
	require.NotEqual(t, [32]byte{}, root)
}

func TestLERManager_Consistency(t *testing.T) {
	ctx := context.Background()

	// Create two managers and add same data
	lerMgr1, err := NewLERManager(ctx)
	require.NoError(t, err)

	lerMgr2, err := NewLERManager(ctx)
	require.NoError(t, err)

	leaf := ExitLeaf{
		OriginNetwork:      1,
		DestinationNetwork: 137,
		OriginAddress:      [32]byte{0x1},
		DestinationAddress: [32]byte{0x2},
		Amount:             big.NewInt(1000),
		Metadata:           [32]byte{0x3},
	}

	// Add to both trees
	err = lerMgr1.AddExit(ctx, leaf)
	require.NoError(t, err)

	err = lerMgr2.AddExit(ctx, leaf)
	require.NoError(t, err)

	// Roots should be identical
	require.Equal(t, lerMgr1.Root(), lerMgr2.Root())
}
