package dicindex128

import (
	"testing"
)

func TestSerializeDeserializeInternalNode(t *testing.T) {
	node := &InternalNode{
		BlockNumber: 10,
		NodeType:    InternalNodeType,
		KeyCount:    3,
		Keys: []Hash128{
			{High: 100, Low: 1},
			{High: 200, Low: 2},
			{High: 300, Low: 3},
		},
		Pointers: []uint16{5, 6, 7, 8}, // KeyCount+1 pointers
	}

	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Serialize
	err := SerializeInternalNode(node, block)
	if err != nil {
		t.Fatalf("Failed to serialize internal node: %v", err)
	}

	// Deserialize
	deserialized, err := DeserializeInternalNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize internal node: %v", err)
	}

	// Verify fields
	if deserialized.BlockNumber != node.BlockNumber {
		t.Errorf("BlockNumber mismatch: expected %d, got %d", node.BlockNumber, deserialized.BlockNumber)
	}
	if deserialized.NodeType != node.NodeType {
		t.Errorf("NodeType mismatch: expected %d, got %d", node.NodeType, deserialized.NodeType)
	}
	if deserialized.KeyCount != node.KeyCount {
		t.Errorf("KeyCount mismatch: expected %d, got %d", node.KeyCount, deserialized.KeyCount)
	}

	// Verify keys
	for i := 0; i < int(node.KeyCount); i++ {
		if !deserialized.Keys[i].Equal(node.Keys[i]) {
			t.Errorf("Key %d mismatch: expected %+v, got %+v", i, node.Keys[i], deserialized.Keys[i])
		}
	}

	// Verify pointers
	for i := 0; i < int(node.KeyCount)+1; i++ {
		if deserialized.Pointers[i] != node.Pointers[i] {
			t.Errorf("Pointer %d mismatch: expected %d, got %d", i, node.Pointers[i], deserialized.Pointers[i])
		}
	}
}

func TestMaxKeysPerInternalBlock(t *testing.T) {
	blockSize := uint16(512)
	maxKeys := MaxKeysPerInternalBlock(blockSize)
	// (512 - 7) / 18 = 505 / 18 = 28
	expectedMax := (512 - 7) / 18
	if maxKeys != expectedMax {
		t.Errorf("MaxKeysPerInternalBlock: expected %d, got %d", expectedMax, maxKeys)
	}
	t.Logf("Block size %d can hold %d keys", blockSize, maxKeys)
}

func TestInsertKeyAndBlockPointer(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create empty internal node
	node := &InternalNode{
		BlockNumber: 0,
		NodeType:    InternalNodeType,
		KeyCount:    0,
		Keys:        []Hash128{},
		Pointers:    []uint16{1}, // One pointer for empty node
	}

	err := SerializeInternalNode(node, block)
	if err != nil {
		t.Fatalf("Failed to serialize empty node: %v", err)
	}

	// Insert keys in non-sorted order
	keys := []Hash128{
		{High: 200, Low: 2},
		{High: 100, Low: 1},
		{High: 300, Low: 3},
	}
	pointers := []uint16{20, 10, 30}

	for i, key := range keys {
		err = InsertKeyAndBlockPointer(block, key, pointers[i])
		if err != nil {
			t.Fatalf("Failed to insert key %d: %v", i, err)
		}
	}

	// Deserialize and verify
	deserialized, err := DeserializeInternalNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if deserialized.KeyCount != 3 {
		t.Errorf("Expected 3 keys, got %d", deserialized.KeyCount)
	}

	// Verify keys are sorted
	for i := 0; i < int(deserialized.KeyCount)-1; i++ {
		if CompareHash128(deserialized.Keys[i], deserialized.Keys[i+1]) >= 0 {
			t.Errorf("Keys not sorted: key %d >= key %d", i, i+1)
		}
	}

	t.Logf("Successfully inserted %d keys in sorted order", deserialized.KeyCount)
}

func TestFindChildBlockPointer(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create internal node with keys
	node := &InternalNode{
		BlockNumber: 0,
		NodeType:    InternalNodeType,
		KeyCount:    3,
		Keys: []Hash128{
			{High: 100, Low: 1},
			{High: 200, Low: 2},
			{High: 300, Low: 3},
		},
		Pointers: []uint16{10, 20, 30, 40}, // 4 pointers for 3 keys
	}

	err := SerializeInternalNode(node, block)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Test finding child pointers
	tests := []struct {
		key             Hash128
		expectedPointer uint16
	}{
		{Hash128{High: 50, Low: 0}, 10},  // Less than all keys
		{Hash128{High: 100, Low: 1}, 10}, // Equal to first key
		{Hash128{High: 150, Low: 1}, 20}, // Between first and second
		{Hash128{High: 200, Low: 2}, 20}, // Equal to second key
		{Hash128{High: 250, Low: 2}, 30}, // Between second and third
		{Hash128{High: 300, Low: 3}, 30}, // Equal to third key
		{Hash128{High: 400, Low: 4}, 40}, // Greater than all keys
	}

	for _, test := range tests {
		pointer, err := FindChildBlockPointer(block, test.key)
		if err != nil {
			t.Errorf("Failed to find pointer for key %+v: %v", test.key, err)
		}
		if pointer != test.expectedPointer {
			t.Errorf("Key %+v: expected pointer %d, got %d", test.key, test.expectedPointer, pointer)
		}
	}
}

func TestDeleteKeyAndBlockPointer(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create internal node with keys
	node := &InternalNode{
		BlockNumber: 0,
		NodeType:    InternalNodeType,
		KeyCount:    3,
		Keys: []Hash128{
			{High: 100, Low: 1},
			{High: 200, Low: 2},
			{High: 300, Low: 3},
		},
		Pointers: []uint16{10, 20, 30, 40}, // 4 pointers for 3 keys
	}

	err := SerializeInternalNode(node, block)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Delete middle key
	err = DeleteKeyAndBlockPointer(block, Hash128{High: 200, Low: 2})
	if err != nil {
		t.Fatalf("Failed to delete key: %v", err)
	}

	// Verify deletion
	deserialized, err := DeserializeInternalNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if deserialized.KeyCount != 2 {
		t.Errorf("Expected 2 keys after deletion, got %d", deserialized.KeyCount)
	}

	// Verify remaining keys
	expectedKeys := []Hash128{
		{High: 100, Low: 1},
		{High: 300, Low: 3},
	}
	for i := 0; i < int(deserialized.KeyCount); i++ {
		if !deserialized.Keys[i].Equal(expectedKeys[i]) {
			t.Errorf("Key %d mismatch after deletion", i)
		}
	}

	// Test deleting non-existent key
	err = DeleteKeyAndBlockPointer(block, Hash128{High: 999, Low: 9})
	if err != ErrKeyNotFound {
		t.Errorf("Expected ErrKeyNotFound, got %v", err)
	}
}

func TestSplitInternalNodeBlock(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)
	newBlock := make([]byte, blockSize)

	// Create internal node with 9 keys
	keys := make([]Hash128, 9)
	pointers := make([]uint16, 10)
	for i := 0; i < 9; i++ {
		keys[i] = Hash128{High: uint64(i * 100), Low: uint64(i)}
		pointers[i] = uint16(i * 10)
	}
	pointers[9] = 90

	node := &InternalNode{
		BlockNumber: 5,
		NodeType:    InternalNodeType,
		KeyCount:    9,
		Keys:        keys,
		Pointers:    pointers,
	}

	err := SerializeInternalNode(node, block)
	if err != nil {
		t.Fatalf("Failed to serialize: %v", err)
	}

	// Split the node
	middleKey, err := SplitInternalNodeBlock(block, newBlock, 20)
	if err != nil {
		t.Fatalf("Failed to split: %v", err)
	}

	// Deserialize both nodes
	leftNode, err := DeserializeInternalNode(block, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize left node: %v", err)
	}

	rightNode, err := DeserializeInternalNode(newBlock, blockSize)
	if err != nil {
		t.Fatalf("Failed to deserialize right node: %v", err)
	}

	// Verify split (9 keys, mid = 4, so left gets 4 keys, right gets 4 keys, middle key promoted)
	if leftNode.KeyCount != 4 {
		t.Errorf("Expected left node to have 4 keys, got %d", leftNode.KeyCount)
	}
	if rightNode.KeyCount != 4 {
		t.Errorf("Expected right node to have 4 keys, got %d", rightNode.KeyCount)
	}

	// Verify middle key
	expectedMiddle := Hash128{High: 400, Low: 4}
	if !middleKey.Equal(expectedMiddle) {
		t.Errorf("Middle key mismatch: expected %+v, got %+v", expectedMiddle, middleKey)
	}

	// Verify left node has keys 0-3
	for i := 0; i < int(leftNode.KeyCount); i++ {
		expected := Hash128{High: uint64(i * 100), Low: uint64(i)}
		if !leftNode.Keys[i].Equal(expected) {
			t.Errorf("Left node key %d mismatch", i)
		}
	}

	// Verify right node has keys 5-8
	for i := 0; i < int(rightNode.KeyCount); i++ {
		expected := Hash128{High: uint64((i + 5) * 100), Low: uint64(i + 5)}
		if !rightNode.Keys[i].Equal(expected) {
			t.Errorf("Right node key %d mismatch", i)
		}
	}

	t.Logf("Split successful: left has %d keys, right has %d keys, middle key = %+v",
		leftNode.KeyCount, rightNode.KeyCount, middleKey)
}

func TestIsInternalBlockFull(t *testing.T) {
	blockSize := uint16(512)
	block := make([]byte, blockSize)

	// Create node with max keys
	maxKeys := MaxKeysPerInternalBlock(blockSize)
	keys := make([]Hash128, maxKeys)
	pointers := make([]uint16, maxKeys+1)
	for i := 0; i < maxKeys; i++ {
		keys[i] = Hash128{High: uint64(i * 100), Low: uint64(i)}
		pointers[i] = uint16(i * 10)
	}
	pointers[maxKeys] = uint16(maxKeys * 10)

	node := &InternalNode{
		BlockNumber: 0,
		NodeType:    InternalNodeType,
		KeyCount:    uint16(maxKeys),
		Keys:        keys,
		Pointers:    pointers,
	}

	err := SerializeInternalNode(node, block)
	if err != nil {
		t.Fatalf("Failed to serialize full node: %v", err)
	}

	// Check if full
	full, err := IsInternalBlockFull(block)
	if err != nil {
		t.Fatalf("Failed to check if full: %v", err)
	}

	if !full {
		t.Errorf("Expected node to be full with %d keys", maxKeys)
	}

	t.Logf("Node with %d keys is correctly identified as full", maxKeys)
}
