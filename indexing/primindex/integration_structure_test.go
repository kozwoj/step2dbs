package primindex

import (
	"encoding/binary"
	"testing"
)

// TestSingleLeafToMultiLevelAndBack tests tree structural transformations
func TestSingleLeafToMultiLevelAndBack(t *testing.T) {
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

	// Start: Single empty leaf (root)
	ValidateTreeStructure(t, idx)
	rootData, _ := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if rootData[2] != NodeTypeLeaf {
		t.Error("Initial root should be a leaf")
	}

	// Calculate entries needed for split
	// Leaf header: 9 bytes, entry: 4 (key) + 8 (value) = 12 bytes
	// Available: 512 - 9 = 503 bytes
	// Max entries: 503 / 12 = 41 entries
	maxEntriesPerLeaf := (512 - 9) / (4 + 8)
	t.Logf("Max entries per leaf: %d", maxEntriesPerLeaf)

	// Insert until split → 2 leaves + internal root
	splitCount := maxEntriesPerLeaf + 1
	for i := 0; i < splitCount; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Verify 2-level tree (internal root with leaf children)
	ValidateTreeStructure(t, idx)
	rootData, _ = ReadIndexBlock(idx, int(idx.Header.RootNode))
	if rootData[2] != NodeTypeInternal {
		t.Error("Root should be internal after split")
	}

	rootInternal, _ := DeserializeInternalNode(rootData, idx.Codec.Size())
	if len(rootInternal.Pointers) != 2 {
		t.Errorf("Root should have 2 children, got %d", len(rootInternal.Pointers))
	}

	// Continue inserting until parent split → 3-level tree
	// Need enough leaves to trigger internal node split
	// Internal node: 5 byte header, each key 4 bytes, each pointer 2 bytes
	// Available: 512 - 5 = 507 bytes
	// Each entry: 4 (key) + 2 (pointer) = 6 bytes, plus one extra pointer
	// Max entries: (507 - 2) / 6 = 84 key/pointer pairs + 1 pointer
	maxInternalKeys := (512 - 5 - 2) / (4 + 2)

	// Each leaf holds maxEntriesPerLeaf entries, need enough to split internal
	entriesToInsert := splitCount + (maxInternalKeys+1)*maxEntriesPerLeaf

	for i := splitCount; i < entriesToInsert; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Verify 3-level tree
	ValidateTreeStructure(t, idx)
	rootData, _ = ReadIndexBlock(idx, int(idx.Header.RootNode))
	if rootData[2] != NodeTypeInternal {
		t.Error("Root should still be internal")
	}

	// Check root has multiple children
	rootInternal, _ = DeserializeInternalNode(rootData, idx.Codec.Size())
	if len(rootInternal.Pointers) < 2 {
		t.Errorf("Root should have multiple children, got %d", len(rootInternal.Pointers))
	}

	// Now delete back down to 2-level tree
	deleteTarget := splitCount + maxEntriesPerLeaf
	for i := entriesToInsert - 1; i >= deleteTarget; i-- {
		err = idx.Delete(uint32(i))
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}
	}

	ValidateTreeStructure(t, idx)

	// Delete until single leaf
	for i := deleteTarget - 1; i >= 0; i-- {
		err = idx.Delete(uint32(i))
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}
	}

	// Verify back to single empty leaf
	ValidateTreeStructure(t, idx)
	rootData, _ = ReadIndexBlock(idx, int(idx.Header.RootNode))
	if rootData[2] != NodeTypeLeaf {
		t.Error("Final root should be a leaf")
	}

	rootLeaf, _ := DeserializeLeafNode(rootData, idx.Codec, int(idx.Header.ValueSize))
	if rootLeaf.EntryCount != 0 {
		t.Errorf("Root leaf should be empty, has %d entries", rootLeaf.EntryCount)
	}
}

// TestCascadingDeletions tests recursive parent removal
func TestCascadingDeletions(t *testing.T) {
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

	// Build a larger tree (aim for 3-4 levels)
	maxEntriesPerLeaf := (512 - 9) / (4 + 8)
	insertCount := maxEntriesPerLeaf * 100 // Enough for multi-level tree

	t.Logf("Inserting %d entries to build deep tree", insertCount)
	for i := 0; i < insertCount; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	ValidateTreeStructure(t, idx)

	// Get initial tree depth
	initialDepth := getTreeDepth(t, idx, idx.Header.RootNode)
	t.Logf("Initial tree depth: %d levels", initialDepth)

	// Delete entries to trigger cascading parent removal
	t.Logf("Deleting all entries...")
	for i := insertCount - 1; i >= 0; i-- {
		err = idx.Delete(uint32(i))
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}

		// Validate periodically
		if i%500 == 0 {
			ValidateTreeStructure(t, idx)
			depth := getTreeDepth(t, idx, idx.Header.RootNode)
			t.Logf("After deleting to key %d: depth = %d", i, depth)
		}
	}

	// Final validation - should be back to single empty leaf
	ValidateTreeStructure(t, idx)
	finalDepth := getTreeDepth(t, idx, idx.Header.RootNode)
	if finalDepth != 1 {
		t.Errorf("Final tree depth should be 1, got %d", finalDepth)
	}

	rootData, _ := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if rootData[2] != NodeTypeLeaf {
		t.Error("Final root should be a leaf")
	}
}

// TestEmptyNodeReuse tests empty block recycling mechanism
func TestEmptyNodeReuse(t *testing.T) {
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

	// Insert 500 entries to create multiple leaves
	t.Log("Phase 1: Insert 500 entries")
	for i := 0; i < 500; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	initialNextEmpty := idx.Header.NextEmptyBlock
	t.Logf("After initial inserts: NextEmptyBlock = %d", initialNextEmpty)

	// Delete all to create empty nodes
	t.Log("Phase 2: Delete all entries to create empty nodes")
	for i := 0; i < 500; i++ {
		err = idx.Delete(uint32(i))
		if err != nil {
			t.Fatalf("Delete failed: %v", err)
		}
	}

	afterDeleteNextEmpty := idx.Header.NextEmptyBlock
	t.Logf("After deletions: NextEmptyBlock = %d", afterDeleteNextEmpty)

	// Count empty blocks in chain
	emptyCount := countEmptyBlocks(t, idx)
	t.Logf("Empty blocks in chain: %d", emptyCount)

	if emptyCount == 0 {
		t.Error("Expected empty blocks in chain after deletions")
	}

	// Insert 200 new entries
	t.Log("Phase 3: Insert 200 new entries (should reuse empty blocks)")
	for i := 1000; i < 1200; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Reinsert failed: %v", err)
		}
	}

	afterReinsertNextEmpty := idx.Header.NextEmptyBlock
	t.Logf("After reinserts: NextEmptyBlock = %d", afterReinsertNextEmpty)

	// Count empty blocks again (should be fewer)
	emptyCountAfter := countEmptyBlocks(t, idx)
	t.Logf("Empty blocks after reinsert: %d", emptyCountAfter)

	if emptyCountAfter >= emptyCount {
		t.Errorf("Expected empty block reuse, but count didn't decrease: %d -> %d",
			emptyCount, emptyCountAfter)
	}

	// Verify all new entries findable
	for i := 1000; i < 1200; i++ {
		_, err := idx.Find(uint32(i))
		if err != nil {
			t.Errorf("Find failed for reinserted key %d: %v", i, err)
		}
	}

	ValidateTreeStructure(t, idx)
}

// TestLeafChainMaintenance tests leaf chain integrity after deletions
func TestLeafChainMaintenance(t *testing.T) {
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

	// Build tree with many leaves
	maxEntriesPerLeaf := (512 - 9) / (4 + 8)
	targetLeaves := 20
	insertCount := maxEntriesPerLeaf * targetLeaves

	t.Logf("Inserting %d entries to create ~%d leaves", insertCount, targetLeaves)
	for i := 0; i < insertCount; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Count initial leaves
	initialLeafCount := countLeaves(t, idx)
	t.Logf("Initial leaf count: %d", initialLeafCount)

	// Delete entries to empty approximately half the leaves
	// Delete entries in groups to empty specific leaves
	for leaf := 0; leaf < targetLeaves/2; leaf++ {
		startKey := leaf * maxEntriesPerLeaf
		endKey := startKey + maxEntriesPerLeaf
		for i := startKey; i < endKey && i < insertCount; i++ {
			err = idx.Delete(uint32(i))
			if err != nil {
				t.Fatalf("Delete failed for key %d: %v", i, err)
			}
		}
	}

	// Count leaves after deletion
	afterDeleteLeafCount := countLeaves(t, idx)
	t.Logf("Leaf count after deletions: %d", afterDeleteLeafCount)

	// Verify no EmptyNodes in leaf chain
	current := idx.Header.FirstLeaf
	for current != NoNextLeaf {
		block, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", current, err)
		}

		if block[2] != NodeTypeLeaf {
			t.Errorf("Block %d in leaf chain has type %d (expected NodeTypeLeaf=%d)",
				current, block[2], NodeTypeLeaf)
		}

		leaf, _ := DeserializeLeafNode(block, idx.Codec, int(idx.Header.ValueSize))
		current = leaf.NextLeaf
	}

	ValidateTreeStructure(t, idx)
}

// Helper function to get tree depth
func getTreeDepth(t *testing.T, idx *Index, blockNum uint16) int {
	t.Helper()

	block, err := ReadIndexBlock(idx, int(blockNum))
	if err != nil {
		return 0
	}

	nodeType := block[2]
	if nodeType == NodeTypeLeaf {
		return 1
	}

	if nodeType == NodeTypeInternal {
		internal, err := DeserializeInternalNode(block, idx.Codec.Size())
		if err != nil {
			return 0
		}

		if len(internal.Pointers) == 0 {
			return 1
		}

		// Get depth of first child and add 1
		childDepth := getTreeDepth(t, idx, internal.Pointers[0])
		return childDepth + 1
	}

	return 0
}

// Helper function to count empty blocks in chain
func countEmptyBlocks(t *testing.T, idx *Index) int {
	t.Helper()

	count := 0
	current := idx.Header.NextEmptyBlock
	visited := make(map[uint16]bool)

	for current != NoNextEmptyBlock && count < 1000 {
		if visited[current] {
			t.Errorf("Cycle detected in empty chain at block %d", current)
			break
		}
		visited[current] = true

		block, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			break
		}

		if block[2] != NodeTypeEmpty {
			break
		}

		emptyNode, _ := DeserializeEmptyNode(block)
		current = emptyNode.NextEmptyBlock
		count++
	}

	return count
}

// Helper function to count leaves in chain
func countLeaves(t *testing.T, idx *Index) int {
	t.Helper()

	count := 0
	current := idx.Header.FirstLeaf
	visited := make(map[uint16]bool)

	for current != NoNextLeaf && count < 1000 {
		if visited[current] {
			t.Errorf("Cycle detected in leaf chain at block %d", current)
			break
		}
		visited[current] = true

		block, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			break
		}

		if block[2] != NodeTypeLeaf {
			t.Errorf("Block %d in leaf chain is not a leaf (type %d)", current, block[2])
			break
		}

		leaf, _ := DeserializeLeafNode(block, idx.Codec, int(idx.Header.ValueSize))
		current = leaf.NextLeaf
		count++
	}

	return count
}
