package primindex

import (
	"testing"
)

func TestSerializeDeserializeInternalNode(t *testing.T) {
	blockSize := 512
	keySize := 4

	// Create an internal node with some keys and pointers
	codec := Uint32Codec{}
	key1, _ := codec.Serialize(uint32(100))
	key2, _ := codec.Serialize(uint32(200))
	key3, _ := codec.Serialize(uint32(300))
	node := &InternalNode{
		BlockNumber: 3,
		NodeType:    NodeTypeInternal,
		KeyCount:    3,
		Keys:        [][]byte{key1, key2, key3},
		Pointers:    []uint16{10, 20, 30, 40}, // n+1 pointers
	}

	// Serialize
	serialized, err := SerializeInternalNode(node, blockSize, keySize)
	if err != nil {
		t.Fatalf("SerializeInternalNode failed: %v", err)
	}

	// Deserialize
	deserialized, err := DeserializeInternalNode(serialized, keySize)
	if err != nil {
		t.Fatalf("DeserializeInternalNode failed: %v", err)
	}

	// Verify
	if deserialized.BlockNumber != node.BlockNumber {
		t.Errorf("BlockNumber mismatch: got %d, want %d", deserialized.BlockNumber, node.BlockNumber)
	}
	if deserialized.NodeType != node.NodeType {
		t.Errorf("NodeType mismatch: got %d, want %d", deserialized.NodeType, node.NodeType)
	}
	if deserialized.KeyCount != node.KeyCount {
		t.Errorf("KeyCount mismatch: got %d, want %d", deserialized.KeyCount, node.KeyCount)
	}

	// Verify keys
	for i := 0; i < int(node.KeyCount); i++ {
		for j := 0; j < keySize; j++ {
			if deserialized.Keys[i][j] != node.Keys[i][j] {
				t.Errorf("Key[%d][%d] mismatch: got %d, want %d", i, j, deserialized.Keys[i][j], node.Keys[i][j])
			}
		}
	}

	// Verify pointers
	for i := 0; i <= int(node.KeyCount); i++ {
		if deserialized.Pointers[i] != node.Pointers[i] {
			t.Errorf("Pointer[%d] mismatch: got %d, want %d", i, deserialized.Pointers[i], node.Pointers[i])
		}
	}
}

func TestIsInternalBlockFull(t *testing.T) {
	codec := Uint32Codec{}
	blockSize := 512
	keySize := codec.Size()

	// Calculate max keys per block
	maxKeys := (blockSize - 5) / (keySize + 2) // header(5) + keys(keySize each) + pointers(2 each)

	// Create a full internal node
	fullNode := &InternalNode{
		BlockNumber: 1,
		NodeType:    NodeTypeInternal,
		KeyCount:    uint16(maxKeys),
		Keys:        make([][]byte, maxKeys),
		Pointers:    make([]uint16, maxKeys+1),
	}

	fullBlock, _ := SerializeInternalNode(fullNode, blockSize, keySize)
	isFull, err := IsInternalBlockFull(fullBlock, keySize)
	if err != nil {
		t.Fatalf("IsInternalBlockFull failed: %v", err)
	}
	if !isFull {
		t.Error("IsInternalBlockFull returned false for a full block")
	}

	// Create a non-full internal node
	nonFullNode := &InternalNode{
		BlockNumber: 1,
		NodeType:    NodeTypeInternal,
		KeyCount:    uint16(maxKeys - 1),
		Keys:        make([][]byte, maxKeys-1),
		Pointers:    make([]uint16, maxKeys),
	}

	nonFullBlock, _ := SerializeInternalNode(nonFullNode, blockSize, keySize)
	isFull2, err2 := IsInternalBlockFull(nonFullBlock, keySize)
	if err2 != nil {
		t.Fatalf("IsInternalBlockFull failed: %v", err2)
	}
	if isFull2 {
		t.Error("IsInternalBlockFull returned true for a non-full block")
	}
}
