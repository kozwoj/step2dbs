package dicindex128

import (
	"testing"
)

func TestSerializeDeserializeLeafNode(t *testing.T) {
	blockSize := uint16(512)
	leaf := &LeafNode{
		BlockNumber: 5,
		NodeType:    LeafNodeType,
		EntryCount:  3,
		NextLeaf:    10,
		Entries: []*IndexEntry128{
			{Hash: HashString128("apple"), DictID: 1, PostingsRef: 100},
			{Hash: HashString128("banana"), DictID: 2, PostingsRef: 200},
			{Hash: HashString128("cherry"), DictID: 3, PostingsRef: 300},
		},
	}

	// Serialize
	block := make([]byte, blockSize)
	err := SerializeLeafNode(leaf, block)
	if err != nil {
		t.Fatalf("Failed to serialize leaf node: %v", err)
	}

	// Deserialize
	deserialized, err := DeserializeLeafNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize leaf node: %v", err)
	}

	// Verify fields
	if deserialized.BlockNumber != leaf.BlockNumber {
		t.Errorf("BlockNumber mismatch: expected %d, got %d", leaf.BlockNumber, deserialized.BlockNumber)
	}
	if deserialized.NodeType != leaf.NodeType {
		t.Errorf("NodeType mismatch: expected %d, got %d", leaf.NodeType, deserialized.NodeType)
	}
	if deserialized.EntryCount != leaf.EntryCount {
		t.Errorf("EntryCount mismatch: expected %d, got %d", leaf.EntryCount, deserialized.EntryCount)
	}
	if deserialized.NextLeaf != leaf.NextLeaf {
		t.Errorf("NextLeaf mismatch: expected %d, got %d", leaf.NextLeaf, deserialized.NextLeaf)
	}

	// Verify entries
	for i := 0; i < int(leaf.EntryCount); i++ {
		if !deserialized.Entries[i].Hash.Equal(leaf.Entries[i].Hash) {
			t.Errorf("Entry %d hash mismatch", i)
		}
		if deserialized.Entries[i].DictID != leaf.Entries[i].DictID {
			t.Errorf("Entry %d DictID mismatch", i)
		}
		if deserialized.Entries[i].PostingsRef != leaf.Entries[i].PostingsRef {
			t.Errorf("Entry %d PostingsRef mismatch", i)
		}
	}
}

func TestMaxEntriesPerBlock(t *testing.T) {
	blockSize := uint16(512)
	maxEntries := MaxEntriesPerBlock(blockSize)
	// (512 - 7) / 24 = 505 / 24 = 21
	expectedMax := (512 - LeafNodeHeaderSize) / EntrySize()
	if maxEntries != expectedMax {
		t.Errorf("MaxEntriesPerBlock: expected %d, got %d", expectedMax, maxEntries)
	}
	t.Logf("Block size %d can hold %d entries", blockSize, maxEntries)
}

func TestInsertEntryToBlock(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create empty leaf node
	leaf := &LeafNode{
		BlockNumber: 1,
		NodeType:    LeafNodeType,
		EntryCount:  0,
		NextLeaf:    0,
		Entries:     []*IndexEntry128{},
	}
	err := SerializeLeafNode(leaf, block)
	if err != nil {
		t.Fatalf("Failed to serialize empty leaf: %v", err)
	}

	// Insert entries
	entries := []*IndexEntry128{
		{Hash: HashString128("banana"), DictID: 2, PostingsRef: 200},
		{Hash: HashString128("apple"), DictID: 1, PostingsRef: 100},
		{Hash: HashString128("cherry"), DictID: 3, PostingsRef: 300},
	}

	for _, entry := range entries {
		err = InsertEntryToBlock(block, entry)
		if err != nil {
			t.Fatalf("Failed to insert entry: %v", err)
		}
	}

	// Deserialize and verify sorted order
	deserialized, err := DeserializeLeafNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if deserialized.EntryCount != 3 {
		t.Errorf("Expected 3 entries, got %d", deserialized.EntryCount)
	}

	// Verify entries are sorted by hash
	for i := 0; i < int(deserialized.EntryCount)-1; i++ {
		cmp := CompareHash128(deserialized.Entries[i].Hash, deserialized.Entries[i+1].Hash)
		if cmp >= 0 {
			t.Errorf("Entries not sorted: entry %d >= entry %d", i, i+1)
		}
	}

	t.Logf("Successfully inserted %d entries in sorted order", deserialized.EntryCount)
}

func TestFindEntryInBlock(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create leaf with entries
	entries := []*IndexEntry128{
		{Hash: HashString128("apple"), DictID: 1, PostingsRef: 100},
		{Hash: HashString128("banana"), DictID: 2, PostingsRef: 200},
		{Hash: HashString128("cherry"), DictID: 3, PostingsRef: 300},
	}

	leaf := &LeafNode{
		BlockNumber: 1,
		NodeType:    LeafNodeType,
		EntryCount:  uint16(len(entries)),
		NextLeaf:    0,
		Entries:     entries,
	}

	err := SerializeLeafNode(leaf, block)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Test finding existing entry
	found, err := FindEntryInBlock(block, HashString128("banana"))
	if err != nil {
		t.Errorf("Failed to find 'banana': %v", err)
	}
	if found.DictID != 2 {
		t.Errorf("Expected DictID 2, got %d", found.DictID)
	}

	// Test finding non-existent entry
	_, err = FindEntryInBlock(block, HashString128("nonexistent"))
	if err != ErrEntryNotFound {
		t.Errorf("Expected ErrEntryNotFound, got %v", err)
	}
}

func TestDeleteEntryFromBlock(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create leaf with entries
	entries := []*IndexEntry128{
		{Hash: HashString128("apple"), DictID: 1, PostingsRef: 100},
		{Hash: HashString128("banana"), DictID: 2, PostingsRef: 200},
		{Hash: HashString128("cherry"), DictID: 3, PostingsRef: 300},
	}

	leaf := &LeafNode{
		BlockNumber: 1,
		NodeType:    LeafNodeType,
		EntryCount:  uint16(len(entries)),
		NextLeaf:    0,
		Entries:     entries,
	}

	err := SerializeLeafNode(leaf, block)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Delete entry
	err = DeleteEntryFromBlock(block, HashString128("banana"))
	if err != nil {
		t.Fatalf("Failed to delete entry: %v", err)
	}

	// Verify deletion
	deserialized, err := DeserializeLeafNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if deserialized.EntryCount != 2 {
		t.Errorf("Expected 2 entries after deletion, got %d", deserialized.EntryCount)
	}

	// Verify deleted entry is gone
	_, err = FindEntryInBlock(block, HashString128("banana"))
	if err != ErrEntryNotFound {
		t.Errorf("Deleted entry should not be found")
	}

	// Test deleting non-existent entry
	err = DeleteEntryFromBlock(block, HashString128("nonexistent"))
	if err != ErrEntryNotFound {
		t.Errorf("Expected ErrEntryNotFound, got %v", err)
	}
}

func TestSplitLeafNodeBlock(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)
	newBlock := make([]byte, blockSize)

	// Create leaf with 10 entries
	entries := make([]*IndexEntry128, 10)
	for i := 0; i < 10; i++ {
		hash := Hash128{High: uint64(i * 100), Low: uint64(i)}
		entries[i] = &IndexEntry128{
			Hash:        hash,
			DictID:      uint32(i + 1),
			PostingsRef: uint32((i + 1) * 10),
		}
	}

	leaf := &LeafNode{
		BlockNumber: 5,
		NodeType:    LeafNodeType,
		EntryCount:  10,
		NextLeaf:    0,
		Entries:     entries,
	}

	err := SerializeLeafNode(leaf, block)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Split the leaf
	leftMaxHash, rightMinHash, err := SplitLeafNodeBlock(block, newBlock, 20)
	if err != nil {
		t.Fatalf("Failed to split: %v", err)
	}

	// Deserialize both leaves
	leftLeaf, err := DeserializeLeafNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize left leaf: %v", err)
	}

	rightLeaf, err := DeserializeLeafNode(newBlock, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize right leaf: %v", err)
	}

	// Verify split
	if leftLeaf.EntryCount != 5 {
		t.Errorf("Expected left leaf to have 5 entries, got %d", leftLeaf.EntryCount)
	}
	if rightLeaf.EntryCount != 5 {
		t.Errorf("Expected right leaf to have 5 entries, got %d", rightLeaf.EntryCount)
	}

	// Verify NextLeaf pointers
	if leftLeaf.NextLeaf != 20 {
		t.Errorf("Left leaf NextLeaf should be 20, got %d", leftLeaf.NextLeaf)
	}
	if rightLeaf.NextLeaf != 0 {
		t.Errorf("Right leaf NextLeaf should be 0, got %d", rightLeaf.NextLeaf)
	}

	// Verify split keys
	if !leftMaxHash.Equal(leftLeaf.Entries[leftLeaf.EntryCount-1].Hash) {
		t.Errorf("leftMaxHash mismatch")
	}
	if !rightMinHash.Equal(rightLeaf.Entries[0].Hash) {
		t.Errorf("rightMinHash mismatch")
	}

	t.Logf("Split successful: left has %d entries, right has %d entries", leftLeaf.EntryCount, rightLeaf.EntryCount)
	t.Logf("Split key: left max = %+v, right min = %+v", leftMaxHash, rightMinHash)
}
