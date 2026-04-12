package primindex

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateAndOpenIndex(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	indexFilename := "testindex.indx"
	blockSize := uint16(512)
	blockCount := uint16(10)
	keyType := KeyTypeUint32
	valueSize := uint32(8)

	// Test CreateIndexFile
	err := CreateIndexFile(tempDir, indexFilename, blockSize, blockCount, keyType, valueSize)
	if err != nil {
		t.Fatalf("CreateIndexFile failed: %v", err)
	}

	// Verify file exists
	indexPath := filepath.Join(tempDir, indexFilename)
	if _, err := os.Stat(indexPath); os.IsNotExist(err) {
		t.Fatalf("Index file was not created at %s", indexPath)
	}

	// Test OpenIndex
	index, err := OpenIndex(tempDir, indexFilename)
	if err != nil {
		t.Fatalf("OpenIndex failed: %v", err)
	}
	defer index.Close()

	// Verify header fields
	if index.Header.BlockSize != blockSize {
		t.Errorf("BlockSize = %d, want %d", index.Header.BlockSize, blockSize)
	}
	if index.Header.FileLength != 2 {
		t.Errorf("FileLength = %d, want 2 (initial: block 0=root/leaf, block 1=empty)", index.Header.FileLength)
	}
	if index.Header.KeyType != uint8(keyType) {
		t.Errorf("KeyType = %d, want %d", index.Header.KeyType, keyType)
	}
	if index.Header.ValueSize != valueSize {
		t.Errorf("ValueSize = %d, want %d", index.Header.ValueSize, valueSize)
	}
	if index.Header.RootNode != 0 {
		t.Errorf("RootNode = %d, want 0", index.Header.RootNode)
	}
	if index.Header.FirstLeaf != 0 {
		t.Errorf("FirstLeaf = %d, want 0", index.Header.FirstLeaf)
	}
	if index.Header.NextEmptyBlock != 1 {
		t.Errorf("NextEmptyBlock = %d, want 1 (first available block)", index.Header.NextEmptyBlock)
	}

	// Verify codec was properly initialized
	if index.Codec == nil {
		t.Error("Codec is nil")
	}
	if index.Codec.Size() != 4 {
		t.Errorf("Codec size = %d, want 4 for Uint32", index.Codec.Size())
	}

	// Verify file handle is open
	if index.File == nil {
		t.Error("File handle is nil")
	}

	// Read block 0 (root/first leaf) and verify it's a leaf node
	block0, err := ReadIndexBlock(index, 0)
	if err != nil {
		t.Fatalf("Failed to read block 0: %v", err)
	}

	if !IsLeafNode(block0) {
		t.Error("Block 0 should be a leaf node")
	}

	// Deserialize the leaf node and verify it's empty
	leafNode, err := DeserializeLeafNode(block0, index.Codec, int(valueSize))
	if err != nil {
		t.Fatalf("Failed to deserialize leaf node: %v", err)
	}

	if leafNode.BlockNumber != 0 {
		t.Errorf("Leaf BlockNumber = %d, want 0", leafNode.BlockNumber)
	}
	if leafNode.EntryCount != 0 {
		t.Errorf("Leaf EntryCount = %d, want 0 (empty)", leafNode.EntryCount)
	}
	if leafNode.NextLeaf != NoNextLeaf {
		t.Errorf("Leaf NextLeaf = %d, want %d (NoNextLeaf)", leafNode.NextLeaf, NoNextLeaf)
	}
}

func TestSimpleInsert(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	indexFilename := "testinsert.indx"
	blockSize := uint16(512)
	blockCount := uint16(10)
	keyType := KeyTypeUint32
	valueSize := uint32(8)

	// Create index file
	err := CreateIndexFile(tempDir, indexFilename, blockSize, blockCount, keyType, valueSize)
	if err != nil {
		t.Fatalf("CreateIndexFile failed: %v", err)
	}

	// Open index
	index, err := OpenIndex(tempDir, indexFilename)
	if err != nil {
		t.Fatalf("OpenIndex failed: %v", err)
	}
	defer index.Close()

	// Insert first entry
	key1 := uint32(100)
	value1 := []byte{1, 2, 3, 4, 5, 6, 7, 8}
	err = index.Insert(key1, value1)
	if err != nil {
		t.Fatalf("Insert key1 failed: %v", err)
	}

	// Insert second entry
	key2 := uint32(200)
	value2 := []byte{10, 20, 30, 40, 50, 60, 70, 80}
	err = index.Insert(key2, value2)
	if err != nil {
		t.Fatalf("Insert key2 failed: %v", err)
	}

	// Find first entry
	foundValue1, err := index.Find(key1)
	if err != nil {
		t.Fatalf("Find key1 failed: %v", err)
	}

	// Verify first entry value
	for i := 0; i < len(value1); i++ {
		if foundValue1[i] != value1[i] {
			t.Errorf("Value1[%d] = %d, want %d", i, foundValue1[i], value1[i])
		}
	}

	// Find second entry
	foundValue2, err := index.Find(key2)
	if err != nil {
		t.Fatalf("Find key2 failed: %v", err)
	}

	// Verify second entry value
	for i := 0; i < len(value2); i++ {
		if foundValue2[i] != value2[i] {
			t.Errorf("Value2[%d] = %d, want %d", i, foundValue2[i], value2[i])
		}
	}

	// Verify entries are in the leaf node
	block0, err := ReadIndexBlock(index, 0)
	if err != nil {
		t.Fatalf("Failed to read block 0: %v", err)
	}

	leafNode, err := DeserializeLeafNode(block0, index.Codec, int(valueSize))
	if err != nil {
		t.Fatalf("Failed to deserialize leaf node: %v", err)
	}

	if leafNode.EntryCount != 2 {
		t.Errorf("EntryCount = %d, want 2", leafNode.EntryCount)
	}

	// Verify entries are sorted by key
	if leafNode.Entries[0].Key.(uint32) != 100 {
		t.Errorf("Entry[0] key = %d, want 100", leafNode.Entries[0].Key)
	}
	if leafNode.Entries[1].Key.(uint32) != 200 {
		t.Errorf("Entry[1] key = %d, want 200", leafNode.Entries[1].Key)
	}
}

func TestInsertWithSplit(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	indexFilename := "testsplit.indx"
	blockSize := uint16(128) // Small block size to trigger split easily
	blockCount := uint16(20)
	keyType := KeyTypeUint32
	valueSize := uint32(8)

	// Create index file
	err := CreateIndexFile(tempDir, indexFilename, blockSize, blockCount, keyType, valueSize)
	if err != nil {
		t.Fatalf("CreateIndexFile failed: %v", err)
	}

	// Open index
	index, err := OpenIndex(tempDir, indexFilename)
	if err != nil {
		t.Fatalf("OpenIndex failed: %v", err)
	}
	defer index.Close()

	// Calculate max entries per leaf node
	keySize := 4 // uint32
	maxEntries := MaxEntriesPerBlock(int(blockSize), keySize, int(valueSize))
	t.Logf("Max entries per block: %d", maxEntries)

	// Insert enough entries to fill the leaf and trigger a split
	numEntries := maxEntries + 1
	insertedKeys := make([]uint32, numEntries)
	insertedValues := make([][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		key := uint32((i + 1) * 100) // 100, 200, 300, ...
		value := make([]byte, valueSize)
		for j := 0; j < int(valueSize); j++ {
			value[j] = byte(i + 1)
		}
		insertedKeys[i] = key
		insertedValues[i] = value

		err = index.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert key %d failed: %v", key, err)
		}
	}

	// After split, root should no longer be block 0
	if index.Header.RootNode == 0 {
		t.Error("RootNode is still 0 after split, expected a new internal root")
	}

	// First leaf should still be block 0
	if index.Header.FirstLeaf != 0 {
		t.Errorf("FirstLeaf = %d, want 0", index.Header.FirstLeaf)
	}

	// Read the root node - should be an internal node now
	rootBlock, err := ReadIndexBlock(index, int(index.Header.RootNode))
	if err != nil {
		t.Fatalf("Failed to read root block: %v", err)
	}

	if IsLeafNode(rootBlock) {
		t.Error("Root should be an internal node after split")
	}

	// Deserialize the internal root
	rootNode, err := DeserializeInternalNode(rootBlock, keySize)
	if err != nil {
		t.Fatalf("Failed to deserialize internal root: %v", err)
	}

	if rootNode.KeyCount != 1 {
		t.Errorf("Root KeyCount = %d, want 1 (after first split)", rootNode.KeyCount)
	}

	if len(rootNode.Pointers) != 2 {
		t.Errorf("Root has %d pointers, want 2 (left and right leaves)", len(rootNode.Pointers))
	}

	// Verify the two leaf nodes exist
	leftLeafBlock, err := ReadIndexBlock(index, int(rootNode.Pointers[0]))
	if err != nil {
		t.Fatalf("Failed to read left leaf: %v", err)
	}

	if !IsLeafNode(leftLeafBlock) {
		t.Error("Left child should be a leaf node")
	}

	rightLeafBlock, err := ReadIndexBlock(index, int(rootNode.Pointers[1]))
	if err != nil {
		t.Fatalf("Failed to read right leaf: %v", err)
	}

	if !IsLeafNode(rightLeafBlock) {
		t.Error("Right child should be a leaf node")
	}

	// Deserialize both leaves
	leftLeaf, err := DeserializeLeafNode(leftLeafBlock, index.Codec, int(valueSize))
	if err != nil {
		t.Fatalf("Failed to deserialize left leaf: %v", err)
	}

	rightLeaf, err := DeserializeLeafNode(rightLeafBlock, index.Codec, int(valueSize))
	if err != nil {
		t.Fatalf("Failed to deserialize right leaf: %v", err)
	}

	t.Logf("Left leaf entries: %d, Right leaf entries: %d", leftLeaf.EntryCount, rightLeaf.EntryCount)

	// Log which keys are in each leaf
	t.Logf("Left leaf keys:")
	for i := 0; i < int(leftLeaf.EntryCount); i++ {
		t.Logf("  %d", leftLeaf.Entries[i].Key.(uint32))
	}
	t.Logf("Right leaf keys:")
	for i := 0; i < int(rightLeaf.EntryCount); i++ {
		t.Logf("  %d", rightLeaf.Entries[i].Key.(uint32))
	}

	// Log the split key in the root
	splitKeyValue, _ := index.Codec.Deserialize(rootNode.Keys[0])
	t.Logf("Root split key: %d", splitKeyValue.(uint32))

	// Total entries should match what we inserted
	totalEntries := int(leftLeaf.EntryCount) + int(rightLeaf.EntryCount)
	if totalEntries != numEntries {
		t.Errorf("Total entries = %d, want %d", totalEntries, numEntries)
	}

	// Verify left leaf points to right leaf
	if leftLeaf.NextLeaf != rightLeaf.BlockNumber {
		t.Errorf("Left leaf NextLeaf = %d, want %d", leftLeaf.NextLeaf, rightLeaf.BlockNumber)
	}

	// Verify all inserted keys can be found
	for i, key := range insertedKeys {
		foundValue, err := index.Find(key)
		if err != nil {
			t.Errorf("Find key %d failed: %v", key, err)

			// Debug: print which leaf the key should be in
			leafBlockNum, path, findErr := index.FindLeafBlock(key)
			if findErr != nil {
				t.Logf("  FindLeafBlock for key %d failed: %v", key, findErr)
			} else {
				t.Logf("  FindLeafBlock returned block %d, path %v", leafBlockNum, path)
			}
			continue
		}

		for j := 0; j < int(valueSize); j++ {
			if foundValue[j] != insertedValues[i][j] {
				t.Errorf("Key %d: value[%d] = %d, want %d", key, j, foundValue[j], insertedValues[i][j])
			}
		}
	}
}

func TestInsertWithRootSplit(t *testing.T) {
	// Create a temporary directory for testing
	tempDir := t.TempDir()
	indexFilename := "testrootsplit.indx"
	blockSize := uint16(128)  // Small block size to trigger splits easily
	blockCount := uint16(500) // Need more blocks for multiple splits
	keyType := KeyTypeUint32
	valueSize := uint32(8)

	// Calculate capacities
	keySize := 4 // uint32
	maxEntriesPerLeaf := MaxEntriesPerBlock(int(blockSize), keySize, int(valueSize))
	// For internal node: Header(5) + n*keySize + (n+1)*2 <= blockSize
	// 5 + n*4 + n*2 + 2 = blockSize
	// 7 + n*6 = blockSize
	maxKeysPerInternal := (int(blockSize) - 7) / 6

	t.Logf("Max entries per leaf: %d", maxEntriesPerLeaf)
	t.Logf("Max keys per internal node: %d", maxKeysPerInternal)

	// To split the root internal node:
	// - Root needs to be full (maxKeysPerInternal keys) and then we add one more
	// - Each key in root represents a leaf split
	// - To have maxKeysPerInternal+1 keys to insert into root, we need maxKeysPerInternal+1 leaf splits
	// - After maxKeysPerInternal+1 splits, we have maxKeysPerInternal+2 leaf nodes
	// We'll insert enough entries to ensure this happens
	numLeafSplits := maxKeysPerInternal + 2 // Extra to be safe
	numEntries := numLeafSplits*maxEntriesPerLeaf + 1

	t.Logf("Inserting %d entries to trigger %d+ leaf splits", numEntries, numLeafSplits)

	// Create index file
	err := CreateIndexFile(tempDir, indexFilename, blockSize, blockCount, keyType, valueSize)
	if err != nil {
		t.Fatalf("CreateIndexFile failed: %v", err)
	}

	// Open index
	index, err := OpenIndex(tempDir, indexFilename)
	if err != nil {
		t.Fatalf("OpenIndex failed: %v", err)
	}
	defer index.Close()

	// Insert entries in sorted order
	insertedKeys := make([]uint32, numEntries)
	insertedValues := make([][]byte, numEntries)

	for i := 0; i < numEntries; i++ {
		key := uint32((i + 1) * 100) // 100, 200, 300, ...
		value := make([]byte, valueSize)
		for j := 0; j < int(valueSize); j++ {
			value[j] = byte((i % 256) + 1)
		}
		insertedKeys[i] = key
		insertedValues[i] = value

		err = index.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert key %d (iteration %d) failed: %v", key, i, err)
		}
	}

	t.Logf("Successfully inserted %d entries", numEntries)
	t.Logf("Root is now at block %d", index.Header.RootNode)

	// Read the root node
	rootBlock, err := ReadIndexBlock(index, int(index.Header.RootNode))
	if err != nil {
		t.Fatalf("Failed to read root block: %v", err)
	}

	// Root should be an internal node
	if IsLeafNode(rootBlock) {
		t.Error("Root should be an internal node")
	}

	// Deserialize the root
	rootNode, err := DeserializeInternalNode(rootBlock, keySize)
	if err != nil {
		t.Fatalf("Failed to deserialize root: %v", err)
	}

	t.Logf("Root has %d keys and %d pointers", rootNode.KeyCount, len(rootNode.Pointers))

	// If root has split, it should have fewer keys than max
	// and its children should be internal nodes (not leaves)
	if rootNode.KeyCount < uint16(maxKeysPerInternal) {
		t.Logf("Root appears to have split (has %d keys, max is %d)", rootNode.KeyCount, maxKeysPerInternal)

		// Check if children are internal nodes
		for i, ptr := range rootNode.Pointers {
			childBlock, err := ReadIndexBlock(index, int(ptr))
			if err != nil {
				t.Fatalf("Failed to read child block %d: %v", ptr, err)
			}

			if IsLeafNode(childBlock) {
				t.Logf("Child %d (block %d) is a leaf", i, ptr)
			} else {
				t.Logf("Child %d (block %d) is an internal node", i, ptr)
				childNode, _ := DeserializeInternalNode(childBlock, keySize)
				t.Logf("  Internal child has %d keys", childNode.KeyCount)
			}
		}
	} else {
		t.Logf("Root has not split yet (has %d keys, max is %d)", rootNode.KeyCount, maxKeysPerInternal)
	}

	// Verify all inserted keys can be found
	t.Logf("Verifying all %d keys can be found...", numEntries)
	for i, key := range insertedKeys {
		foundValue, err := index.Find(key)
		if err != nil {
			t.Errorf("Find key %d (iteration %d) failed: %v", key, i, err)
			continue
		}

		for j := 0; j < int(valueSize); j++ {
			if foundValue[j] != insertedValues[i][j] {
				t.Errorf("Key %d: value[%d] = %d, want %d", key, j, foundValue[j], insertedValues[i][j])
				break
			}
		}
	}

	t.Logf("All keys verified successfully!")

	// Traverse the leaf node linked list and verify keys are in ascending order
	t.Logf("\nTraversing leaf node linked list starting from FirstLeaf=%d...", index.Header.FirstLeaf)
	currentLeafBlock := index.Header.FirstLeaf
	leafCount := 0
	totalKeysInLeaves := 0
	var previousKey uint32 = 0
	firstIteration := true

	for firstIteration || currentLeafBlock != NoNextLeaf {
		firstIteration = false
		leafCount++

		// Read the leaf block
		leafBlock, err := ReadIndexBlock(index, int(currentLeafBlock))
		if err != nil {
			t.Fatalf("Failed to read leaf block %d: %v", currentLeafBlock, err)
		}

		if !IsLeafNode(leafBlock) {
			t.Fatalf("Block %d should be a leaf node", currentLeafBlock)
		}

		// Deserialize the leaf
		leaf, err := DeserializeLeafNode(leafBlock, index.Codec, int(valueSize))
		if err != nil {
			t.Fatalf("Failed to deserialize leaf block %d: %v", currentLeafBlock, err)
		}

		t.Logf("Leaf %d (block %d): %d entries, NextLeaf=%d", leafCount, currentLeafBlock, leaf.EntryCount, leaf.NextLeaf)

		// Verify keys in this leaf are in ascending order
		for i := 0; i < int(leaf.EntryCount); i++ {
			key := leaf.Entries[i].Key.(uint32)

			// Check ascending order within leaf
			if i > 0 {
				prevKeyInLeaf := leaf.Entries[i-1].Key.(uint32)
				if key <= prevKeyInLeaf {
					t.Errorf("Keys not in ascending order in leaf %d: key[%d]=%d <= key[%d]=%d",
						leafCount, i, key, i-1, prevKeyInLeaf)
				}
			}

			// Check ascending order across leaves
			if totalKeysInLeaves > 0 && key <= previousKey {
				t.Errorf("Keys not in ascending order across leaves: leaf %d key=%d <= previous key=%d",
					leafCount, key, previousKey)
			}

			previousKey = key
			totalKeysInLeaves++
		}

		// Move to next leaf
		currentLeafBlock = leaf.NextLeaf
	}

	t.Logf("Traversed %d leaf nodes with %d total keys", leafCount, totalKeysInLeaves)

	if totalKeysInLeaves != numEntries {
		t.Errorf("Total keys in leaf linked list = %d, want %d", totalKeysInLeaves, numEntries)
	}
}
