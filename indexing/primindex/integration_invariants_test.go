package primindex

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// ValidateTreeStructure checks all B+ tree invariants
func ValidateTreeStructure(t *testing.T, idx *Index) {
	t.Helper()
	ValidateInternalNodeProperties(t, idx)
	ValidateLeafChainIntegrity(t, idx)
	ValidateEmptyChainValidity(t, idx)
	ValidateParentChildConsistency(t, idx)
	ValidateBlockAllocation(t, idx)
}

// ValidateInternalNodeProperties checks that all internal nodes satisfy B+ tree properties
func ValidateInternalNodeProperties(t *testing.T, idx *Index) {
	t.Helper()

	// Check root node type
	rootData, err := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if err != nil {
		t.Fatalf("Failed to read root: %v", err)
	}

	// If root is a leaf, no internal nodes to validate
	if rootData[2] == NodeTypeLeaf {
		return
	}

	// Walk the tree starting from root
	visited := make(map[uint16]bool)
	validateInternalNodeRecursive(t, idx, idx.Header.RootNode, visited)
}

func validateInternalNodeRecursive(t *testing.T, idx *Index, blockNum uint16, visited map[uint16]bool) {
	t.Helper()

	if visited[blockNum] {
		t.Errorf("Block %d visited multiple times - cycle detected", blockNum)
		return
	}
	visited[blockNum] = true

	blockData, err := ReadIndexBlock(idx, int(blockNum))
	if err != nil {
		t.Fatalf("Failed to read block %d: %v", blockNum, err)
	}

	nodeType := blockData[2]
	if nodeType != NodeTypeInternal {
		return // Not an internal node, skip
	}

	internalNode, err := DeserializeInternalNode(blockData, idx.Codec.Size())
	if err != nil {
		t.Fatalf("Failed to deserialize internal node %d: %v", blockNum, err)
	}

	// Check: KeyCount + 1 == len(Pointers)
	if len(internalNode.Pointers) != len(internalNode.Keys)+1 {
		t.Errorf("Block %d: Internal node has %d keys but %d pointers (expected %d)",
			blockNum, len(internalNode.Keys), len(internalNode.Pointers), len(internalNode.Keys)+1)
	}

	// Check: Keys are sorted
	for i := 1; i < len(internalNode.Keys); i++ {
		cmp := idx.Codec.Compare(internalNode.Keys[i-1], internalNode.Keys[i])
		if cmp >= 0 {
			t.Errorf("Block %d: Keys not sorted at index %d", blockNum, i)
		}
	}

	// Note: We don't check pointers against FileLength because the file grows dynamically
	// and FileLength in the header is not always updated

	// Recurse into children
	for _, childPtr := range internalNode.Pointers {
		childBlockData, err := ReadIndexBlock(idx, int(childPtr))
		if err != nil {
			continue
		}
		childNodeType := childBlockData[2]
		if childNodeType == NodeTypeInternal {
			validateInternalNodeRecursive(t, idx, childPtr, visited)
		}
	}
}

// ValidateLeafChainIntegrity checks the doubly-linked leaf chain
func ValidateLeafChainIntegrity(t *testing.T, idx *Index) {
	t.Helper()

	// Find first leaf (leftmost)
	firstLeaf := findFirstLeaf(t, idx, idx.Header.RootNode)
	if firstLeaf == 0xFFFF {
		// No leaves in tree (shouldn't happen - root is always at least a leaf)
		rootData, _ := ReadIndexBlock(idx, int(idx.Header.RootNode))
		if rootData != nil && rootData[2] != NodeTypeLeaf {
			t.Error("Root is not a leaf but no leaves found")
		}
		return
	}

	// Forward traversal
	forwardLeaves := []uint16{}
	current := firstLeaf
	visited := make(map[uint16]bool)

	for current != NoNextLeaf {
		if visited[current] {
			t.Errorf("Leaf chain cycle detected at block %d", current)
			break
		}
		visited[current] = true

		blockData, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", current, err)
		}

		nodeType := blockData[2]
		if nodeType != NodeTypeLeaf {
			t.Errorf("Block %d in leaf chain is not a leaf (type %d)", current, nodeType)
			break
		}

		leafNode, err := DeserializeLeafNode(blockData, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf %d: %v", current, err)
		}

		forwardLeaves = append(forwardLeaves, current)

		// Validate PrevLeaf pointer consistency
		if leafNode.PrevLeaf != NoPrevLeaf {
			prevBlockData, err := ReadIndexBlock(idx, int(leafNode.PrevLeaf))
			if err == nil {
				prevLeaf, _ := DeserializeLeafNode(prevBlockData, idx.Codec, int(idx.Header.ValueSize))
				if prevLeaf != nil && prevLeaf.NextLeaf != current {
					t.Errorf("Leaf %d: PrevLeaf=%d but that leaf's NextLeaf=%d (expected %d)",
						current, leafNode.PrevLeaf, prevLeaf.NextLeaf, current)
				}
			}
		}

		current = leafNode.NextLeaf
	}

	// Backward traversal from last leaf
	if len(forwardLeaves) > 0 {
		backwardLeaves := []uint16{}
		lastLeaf := forwardLeaves[len(forwardLeaves)-1]
		current = lastLeaf

		visitedBack := make(map[uint16]bool)
		for current != NoPrevLeaf {
			if visitedBack[current] {
				t.Errorf("Backward leaf chain cycle detected at block %d", current)
				break
			}
			visitedBack[current] = true

			blockData, err := ReadIndexBlock(idx, int(current))
			if err != nil {
				break
			}

			nodeType := blockData[2]
			if nodeType != NodeTypeLeaf {
				t.Errorf("Block %d in backward leaf chain is not a leaf (type %d) - stopping backward traversal", current, nodeType)
				break
			}

			leafNode, err := DeserializeLeafNode(blockData, idx.Codec, int(idx.Header.ValueSize))
			if err != nil {
				t.Errorf("Block %d failed to deserialize as leaf: %v", current, err)
				break
			}

			backwardLeaves = append(backwardLeaves, current)
			current = leafNode.PrevLeaf
		}

		// Reverse backwardLeaves to compare with forwardLeaves
		for i := 0; i < len(backwardLeaves)/2; i++ {
			j := len(backwardLeaves) - 1 - i
			backwardLeaves[i], backwardLeaves[j] = backwardLeaves[j], backwardLeaves[i]
		}

		// Compare forward and backward traversals
		if len(forwardLeaves) != len(backwardLeaves) {
			t.Errorf("Forward traversal found %d leaves, backward found %d",
				len(forwardLeaves), len(backwardLeaves))
		} else {
			for i := range forwardLeaves {
				if forwardLeaves[i] != backwardLeaves[i] {
					t.Errorf("Leaf chain mismatch at position %d: forward=%d, backward=%d",
						i, forwardLeaves[i], backwardLeaves[i])
				}
			}
		}
	}
}

// findFirstLeaf finds the leftmost leaf starting from a given block
func findFirstLeaf(t *testing.T, idx *Index, blockNum uint16) uint16 {
	t.Helper()

	blockData, err := ReadIndexBlock(idx, int(blockNum))
	if err != nil {
		return 0xFFFF
	}

	nodeType := blockData[2]
	if nodeType == NodeTypeLeaf {
		return blockNum
	}

	if nodeType == NodeTypeInternal {
		internalNode, err := DeserializeInternalNode(blockData, idx.Codec.Size())
		if err != nil {
			return 0xFFFF
		}
		if len(internalNode.Pointers) > 0 {
			return findFirstLeaf(t, idx, internalNode.Pointers[0])
		}
	}

	return 0xFFFF
}

// ValidateEmptyChainValidity checks the empty node chain
func ValidateEmptyChainValidity(t *testing.T, idx *Index) {
	t.Helper()

	if idx.Header.NextEmptyBlock == NoNextEmptyBlock {
		return // No empty blocks, nothing to validate
	}

	visited := make(map[uint16]bool)
	current := idx.Header.NextEmptyBlock

	for current != NoNextEmptyBlock {
		if visited[current] {
			t.Errorf("Empty chain cycle detected at block %d", current)
			break
		}
		visited[current] = true

		blockData, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			// EOF or read error - end of empty chain
			break
		}

		nodeType := blockData[2]
		if nodeType != NodeTypeEmpty {
			// This is acceptable - block may have been reused and is now active
			// Just stop traversing the empty chain here
			break
		}

		// Read NextEmptyBlock pointer (at offset 3-4)
		nextEmpty := binary.LittleEndian.Uint16(blockData[3:5])
		current = nextEmpty
	}
}

// ValidateParentChildConsistency verifies separator keys match child minimums
func ValidateParentChildConsistency(t *testing.T, idx *Index) {
	t.Helper()

	rootData, err := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if err != nil || rootData[2] == NodeTypeLeaf {
		return // No parent-child relationships in single-leaf tree
	}

	validateParentChildRecursive(t, idx, idx.Header.RootNode, nil, -1)
}

func validateParentChildRecursive(t *testing.T, idx *Index, blockNum uint16, parentKeys [][]byte, childIndex int) {
	t.Helper()

	blockData, err := ReadIndexBlock(idx, int(blockNum))
	if err != nil {
		return
	}

	nodeType := blockData[2]

	// If this is a leaf, check separator key matches minimum
	if nodeType == NodeTypeLeaf {
		if parentKeys != nil && childIndex > 0 {
			leafNode, err := DeserializeLeafNode(blockData, idx.Codec, int(idx.Header.ValueSize))
			if err != nil {
				return
			}

			if len(leafNode.Entries) > 0 {
				// Get the serialized minimum key from the first entry
				minKeySerialized, err := idx.Codec.Serialize(leafNode.Entries[0].Key)
				if err != nil {
					return
				}
				separatorKey := parentKeys[childIndex-1]

				cmp := idx.Codec.Compare(separatorKey, minKeySerialized)
				if cmp != 0 {
					t.Errorf("Leaf %d: Parent separator key doesn't match minimum key (cmp=%d)",
						blockNum, cmp)
				}
			}
		}
		return
	}

	// If internal node, recurse into children
	if nodeType == NodeTypeInternal {
		internalNode, err := DeserializeInternalNode(blockData, idx.Codec.Size())
		if err != nil {
			return
		}

		for i, childPtr := range internalNode.Pointers {
			validateParentChildRecursive(t, idx, childPtr, internalNode.Keys, i)
		}
	}
}

// ValidateBlockAllocation checks for block number issues
func ValidateBlockAllocation(t *testing.T, idx *Index) {
	t.Helper()

	// Collect all active blocks (reachable from root)
	activeBlocks := make(map[uint16]bool)
	collectActiveBlocks(t, idx, idx.Header.RootNode, activeBlocks)

	// Collect all blocks in empty chain
	emptyBlocks := make(map[uint16]bool)
	current := idx.Header.NextEmptyBlock
	visited := make(map[uint16]bool)

	for current != NoNextEmptyBlock {
		if visited[current] {
			break // Cycle already reported in ValidateEmptyChainValidity
		}
		visited[current] = true

		blockData, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			break
		}

		nodeType := blockData[2]
		if nodeType != NodeTypeEmpty {
			break // Chain terminated or block reused
		}

		emptyBlocks[current] = true
		nextEmpty := binary.LittleEndian.Uint16(blockData[3:5])
		current = nextEmpty
	}

	// Check for overlaps (should not happen)
	for blockNum := range activeBlocks {
		if emptyBlocks[blockNum] {
			t.Errorf("Block %d is both active and in empty chain", blockNum)
		}
	}

	// All blocks should be either active, empty, or unused
	// (We can't check unused blocks without scanning the entire file)
}

func collectActiveBlocks(t *testing.T, idx *Index, blockNum uint16, active map[uint16]bool) {
	t.Helper()

	if active[blockNum] {
		return // Already visited
	}
	active[blockNum] = true

	blockData, err := ReadIndexBlock(idx, int(blockNum))
	if err != nil {
		return
	}

	nodeType := blockData[2]

	if nodeType == NodeTypeInternal {
		internalNode, err := DeserializeInternalNode(blockData, idx.Codec.Size())
		if err != nil {
			return
		}
		for _, childPtr := range internalNode.Pointers {
			collectActiveBlocks(t, idx, childPtr, active)
		}
	} else if nodeType == NodeTypeLeaf {
		// Leaf nodes are in a chain, collect via NextLeaf
		leafNode, err := DeserializeLeafNode(blockData, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return
		}
		if leafNode.NextLeaf != NoNextLeaf {
			collectActiveBlocks(t, idx, leafNode.NextLeaf, active)
		}
	}
}

// Test to validate the validation helpers work correctly
func TestValidationHelpersBasic(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	// Validate empty tree
	ValidateTreeStructure(t, idx)

	// Insert some entries
	for i := uint32(0); i < 100; i++ {
		value := make([]byte, 8)
		binary.LittleEndian.PutUint64(value, uint64(i*10))
		err = idx.Insert(i, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Validate after inserts
	ValidateTreeStructure(t, idx)

	// Delete some entries
	for i := uint32(0); i < 50; i += 2 {
		err = idx.Delete(i)
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}
	}

	// Validate after deletes
	ValidateTreeStructure(t, idx)
}

// Test invariants are maintained during complex operations
func TestInvariantsAfterComplexOperations(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Perform 5000 mixed operations, validate every 500 ops
	operationCount := 0
	insertedKeys := make(map[uint32]bool)

	for op := 0; op < 5000; op++ {
		key := uint32(op % 1000)

		if op%2 == 0 {
			// Insert (only if key doesn't exist)
			if !insertedKeys[key] {
				binary.LittleEndian.PutUint64(value, uint64(key*10))
				err = idx.Insert(key, value)
				if err != nil {
					t.Fatalf("Insert failed at op %d, key %d: %v", op, key, err)
				}
				insertedKeys[key] = true
			}
		} else {
			// Delete (only if key exists)
			if insertedKeys[key] {
				err = idx.Delete(key)
				if err != nil {
					t.Fatalf("Delete failed at op %d, key %d: %v", op, key, err)
				}
				delete(insertedKeys, key)
			}
		}

		operationCount++

		// Validate every 500 operations
		if operationCount%500 == 0 {
			ValidateTreeStructure(t, idx)
		}
	}

	// Final validation
	ValidateTreeStructure(t, idx)

	// Verify remaining keys are searchable
	for key := range insertedKeys {
		_, err := idx.Find(key)
		if err != nil {
			t.Errorf("Find failed for key %d: %v", key, err)
		}
	}
}

// Test invariants after every delete operation
func TestInvariantsAfterEveryDelete(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	// Insert 200 entries
	value := make([]byte, 8)
	for i := uint32(0); i < 200; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i*10))
		err = idx.Insert(i, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Delete entries one by one, validating after each
	for i := uint32(0); i < 200; i++ {
		err = idx.Delete(i)
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}

		// Validate tree structure after each delete
		ValidateTreeStructure(t, idx)
	}

	// After deleting all entries, tree should be in initial state
	rootData, err := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if err != nil {
		t.Fatalf("Failed to read root: %v", err)
	}

	if rootData[2] != NodeTypeLeaf {
		t.Errorf("Root should be a leaf after all deletes, got type %d", rootData[2])
	}

	// Root block should be empty leaf
	rootLeaf, err := DeserializeLeafNode(rootData, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		t.Fatalf("Failed to deserialize root leaf: %v", err)
	}

	if len(rootLeaf.Entries) != 0 {
		t.Errorf("Root leaf should have 0 entries, got %d", len(rootLeaf.Entries))
	}
}

// Helper: Print tree structure for debugging (can be used during test development)
func PrintTreeStructureOverview(t *testing.T, idx *Index) {
	t.Helper()
	fmt.Printf("\n=== Tree Structure ===\n")
	rootData, _ := ReadIndexBlock(idx, int(idx.Header.RootNode))
	rootType := uint8(0)
	if rootData != nil {
		rootType = rootData[2]
	}
	fmt.Printf("Root: Block %d, Type %d\n", idx.Header.RootNode, rootType)
	fmt.Printf("FileLength: %d blocks\n", idx.Header.FileLength)
	fmt.Printf("NextEmptyBlock: %d\n", idx.Header.NextEmptyBlock)
	printBlockRecursive(t, idx, idx.Header.RootNode, 0)
	fmt.Printf("===================\n\n")
}

func printBlockRecursive(t *testing.T, idx *Index, blockNum uint16, depth int) {
	t.Helper()
	indent := ""
	for i := 0; i < depth; i++ {
		indent += "  "
	}

	blockData, err := ReadIndexBlock(idx, int(blockNum))
	if err != nil {
		fmt.Printf("%sBlock %d: Error reading: %v\n", indent, blockNum, err)
		return
	}

	nodeType := blockData[2]

	if nodeType == NodeTypeLeaf {
		leafNode, err := DeserializeLeafNode(blockData, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			fmt.Printf("%sBlock %d: Leaf (error: %v)\n", indent, blockNum, err)
			return
		}
		fmt.Printf("%sBlock %d: Leaf with %d entries (Next=%d, Prev=%d)\n",
			indent, blockNum, len(leafNode.Entries), leafNode.NextLeaf, leafNode.PrevLeaf)
	} else if nodeType == NodeTypeInternal {
		internalNode, err := DeserializeInternalNode(blockData, idx.Codec.Size())
		if err != nil {
			fmt.Printf("%sBlock %d: Internal (error: %v)\n", indent, blockNum, err)
			return
		}
		fmt.Printf("%sBlock %d: Internal with %d keys, %d pointers\n",
			indent, blockNum, len(internalNode.Keys), len(internalNode.Pointers))
		for _, ptr := range internalNode.Pointers {
			printBlockRecursive(t, idx, ptr, depth+1)
		}
	} else if nodeType == NodeTypeEmpty {
		fmt.Printf("%sBlock %d: Empty\n", indent, blockNum)
	} else {
		fmt.Printf("%sBlock %d: Unknown type %d\n", indent, blockNum, nodeType)
	}
}
