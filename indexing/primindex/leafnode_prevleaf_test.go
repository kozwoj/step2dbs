package primindex

import (
	"os"
	"testing"
)

// TestPrevLeafPointers verifies that PrevLeaf pointers are correctly maintained
// during leaf node splits and that we can traverse the leaf chain backwards
func TestPrevLeafPointers(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	indexName := "testprevleaf"

	// Index configuration
	blockSize := uint16(512)
	blockCount := uint16(100)
	keyType := KeyTypeUint32
	valueSize := uint32(8)

	// Create index
	err := CreateIndexFile(tempDir, indexName, blockSize, blockCount, keyType, valueSize)
	if err != nil {
		t.Fatalf("CreateIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenIndex(tempDir, indexName)
	if err != nil {
		t.Fatalf("OpenIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert enough entries to cause multiple leaf splits
	// With blockSize=512, keySize=4, valueSize=8, headerSize=9:
	// MaxEntries = (512 - 9) / 12 = 41 entries per leaf
	numEntries := 100
	t.Logf("Inserting %d entries to trigger multiple splits", numEntries)

	for i := 1; i <= numEntries; i++ {
		key := uint32(i * 100)
		value := []byte{byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i), byte(i)}

		err = idx.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", key, err)
		}
	}

	t.Logf("Successfully inserted %d entries", numEntries)

	// Traverse forward through leaf chain and record block numbers
	var forwardChain []uint16
	currentBlock := idx.Header.FirstLeaf

	for currentBlock != NoNextLeaf {
		forwardChain = append(forwardChain, currentBlock)

		// Read the leaf block
		leafBlock, err := ReadIndexBlock(idx, int(currentBlock))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", currentBlock, err)
		}

		// Deserialize
		leafNode, err := DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf block %d: %v", currentBlock, err)
		}

		currentBlock = leafNode.NextLeaf
	}

	t.Logf("Forward chain has %d leaf nodes: %v", len(forwardChain), forwardChain)

	// Now traverse backwards using PrevLeaf pointers
	// Start from the last leaf in the forward chain
	lastLeafBlock := forwardChain[len(forwardChain)-1]
	var backwardChain []uint16
	currentBlock = lastLeafBlock

	for {
		backwardChain = append(backwardChain, currentBlock)

		// Read the leaf block
		leafBlock, err := ReadIndexBlock(idx, int(currentBlock))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", currentBlock, err)
		}

		// Deserialize
		leafNode, err := DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf block %d: %v", currentBlock, err)
		}

		// Check if we're at the first leaf
		if leafNode.PrevLeaf == NoPrevLeaf {
			break
		}

		currentBlock = leafNode.PrevLeaf
	}

	t.Logf("Backward chain has %d leaf nodes: %v", len(backwardChain), backwardChain)

	// Verify that forward and backward chains match (in reverse order)
	if len(forwardChain) != len(backwardChain) {
		t.Errorf("Chain length mismatch: forward=%d, backward=%d", len(forwardChain), len(backwardChain))
	}

	// Check that backward chain is the reverse of forward chain
	for i := 0; i < len(forwardChain); i++ {
		forwardIdx := i
		backwardIdx := len(backwardChain) - 1 - i

		if forwardChain[forwardIdx] != backwardChain[backwardIdx] {
			t.Errorf("Chain mismatch at position %d: forward=%d, backward=%d",
				i, forwardChain[forwardIdx], backwardChain[backwardIdx])
		}
	}

	// Verify that each leaf has correct PrevLeaf and NextLeaf pointers
	for i, blockNum := range forwardChain {
		leafBlock, err := ReadIndexBlock(idx, int(blockNum))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", blockNum, err)
		}

		leafNode, err := DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf block %d: %v", blockNum, err)
		}

		// Check PrevLeaf pointer
		if i == 0 {
			// First leaf should have NoPrevLeaf
			if leafNode.PrevLeaf != NoPrevLeaf {
				t.Errorf("First leaf (block %d) has PrevLeaf=%d, want NoPrevLeaf", blockNum, leafNode.PrevLeaf)
			}
		} else {
			// Should point to previous block in chain
			expectedPrev := forwardChain[i-1]
			if leafNode.PrevLeaf != expectedPrev {
				t.Errorf("Leaf block %d has PrevLeaf=%d, want %d", blockNum, leafNode.PrevLeaf, expectedPrev)
			}
		}

		// Check NextLeaf pointer
		if i == len(forwardChain)-1 {
			// Last leaf should have NoNextLeaf
			if leafNode.NextLeaf != NoNextLeaf {
				t.Errorf("Last leaf (block %d) has NextLeaf=%d, want NoNextLeaf", blockNum, leafNode.NextLeaf)
			}
		} else {
			// Should point to next block in chain
			expectedNext := forwardChain[i+1]
			if leafNode.NextLeaf != expectedNext {
				t.Errorf("Leaf block %d has NextLeaf=%d, want %d", blockNum, leafNode.NextLeaf, expectedNext)
			}
		}
	}

	t.Logf("All PrevLeaf and NextLeaf pointers verified successfully!")

	// Clean up
	os.RemoveAll(tempDir)
}

// TestPrevLeafAfterRootSplit verifies PrevLeaf pointers are correct even after root splits
func TestPrevLeafAfterRootSplit(t *testing.T) {
	tempDir := t.TempDir()
	indexName := "testrootsplit"

	blockSize := uint16(512)
	blockCount := uint16(200)
	keyType := KeyTypeUint32
	valueSize := uint32(8)

	err := CreateIndexFile(tempDir, indexName, blockSize, blockCount, keyType, valueSize)
	if err != nil {
		t.Fatalf("CreateIndexFile failed: %v", err)
	}

	idx, err := OpenIndex(tempDir, indexName)
	if err != nil {
		t.Fatalf("OpenIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert many entries to trigger root splits
	numEntries := 200
	t.Logf("Inserting %d entries to trigger root split", numEntries)

	for i := 1; i <= numEntries; i++ {
		key := uint32(i)
		value := []byte{byte(i), byte(i >> 8), 0, 0, 0, 0, 0, 0}

		err = idx.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", key, err)
		}
	}

	// Collect all leaf blocks in forward order
	var leafBlocks []uint16
	currentBlock := idx.Header.FirstLeaf

	for currentBlock != NoNextLeaf {
		leafBlocks = append(leafBlocks, currentBlock)

		leafBlock, err := ReadIndexBlock(idx, int(currentBlock))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", currentBlock, err)
		}

		leafNode, err := DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf block %d: %v", currentBlock, err)
		}

		currentBlock = leafNode.NextLeaf
	}

	t.Logf("Found %d leaf blocks after root split", len(leafBlocks))

	// Verify bidirectional consistency
	for i, blockNum := range leafBlocks {
		leafBlock, err := ReadIndexBlock(idx, int(blockNum))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", blockNum, err)
		}

		leafNode, err := DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf block %d: %v", blockNum, err)
		}

		// If there's a NextLeaf, verify it points back to us
		if leafNode.NextLeaf != NoNextLeaf {
			nextBlock, err := ReadIndexBlock(idx, int(leafNode.NextLeaf))
			if err != nil {
				t.Fatalf("Failed to read next leaf block %d: %v", leafNode.NextLeaf, err)
			}

			nextLeaf, err := DeserializeLeafNode(nextBlock, idx.Codec, int(idx.Header.ValueSize))
			if err != nil {
				t.Fatalf("Failed to deserialize next leaf block %d: %v", leafNode.NextLeaf, err)
			}

			if nextLeaf.PrevLeaf != blockNum {
				t.Errorf("Leaf block %d has NextLeaf=%d, but that leaf has PrevLeaf=%d (should be %d)",
					blockNum, leafNode.NextLeaf, nextLeaf.PrevLeaf, blockNum)
			}
		}

		// If there's a PrevLeaf, verify it points to us
		if leafNode.PrevLeaf != NoPrevLeaf {
			prevBlock, err := ReadIndexBlock(idx, int(leafNode.PrevLeaf))
			if err != nil {
				t.Fatalf("Failed to read prev leaf block %d: %v", leafNode.PrevLeaf, err)
			}

			prevLeaf, err := DeserializeLeafNode(prevBlock, idx.Codec, int(idx.Header.ValueSize))
			if err != nil {
				t.Fatalf("Failed to deserialize prev leaf block %d: %v", leafNode.PrevLeaf, err)
			}

			if prevLeaf.NextLeaf != blockNum {
				t.Errorf("Leaf block %d has PrevLeaf=%d, but that leaf has NextLeaf=%d (should be %d)",
					blockNum, leafNode.PrevLeaf, prevLeaf.NextLeaf, blockNum)
			}
		}

		// Also verify against our sequential list
		if i > 0 && leafNode.PrevLeaf != leafBlocks[i-1] {
			t.Errorf("Leaf %d (block %d) has PrevLeaf=%d, want %d",
				i, blockNum, leafNode.PrevLeaf, leafBlocks[i-1])
		}

		if i < len(leafBlocks)-1 && leafNode.NextLeaf != leafBlocks[i+1] {
			t.Errorf("Leaf %d (block %d) has NextLeaf=%d, want %d",
				i, blockNum, leafNode.NextLeaf, leafBlocks[i+1])
		}
	}

	t.Logf("Bidirectional pointer consistency verified for all %d leaves!", len(leafBlocks))

	os.RemoveAll(tempDir)
}
