package primindex

import (
	"testing"
)

func TestSerializeDeserializeLeafNode(t *testing.T) {
	codec := Uint32Codec{}
	blockSize := 512
	valueSize := 8

	// Create a leaf node with some entries
	node := &LeafNode{
		BlockNumber: 5,
		NodeType:    NodeTypeLeaf,
		EntryCount:  3,
		NextLeaf:    10,
		PrevLeaf:    3,
		Entries: []*IndexEntry{
			{Key: uint32(100), Value: []byte{1, 2, 3, 4, 5, 6, 7, 8}},
			{Key: uint32(200), Value: []byte{9, 10, 11, 12, 13, 14, 15, 16}},
			{Key: uint32(300), Value: []byte{17, 18, 19, 20, 21, 22, 23, 24}},
		},
	}

	// Serialize
	serialized, err := SerializeLeafNode(node, blockSize, codec, valueSize)
	if err != nil {
		t.Fatalf("SerializeLeafNode failed: %v", err)
	}

	// Deserialize
	deserialized, err := DeserializeLeafNode(serialized, codec, valueSize)
	if err != nil {
		t.Fatalf("DeserializeLeafNode failed: %v", err)
	}

	// Verify
	if deserialized.BlockNumber != node.BlockNumber {
		t.Errorf("BlockNumber mismatch: got %d, want %d", deserialized.BlockNumber, node.BlockNumber)
	}
	if deserialized.NodeType != node.NodeType {
		t.Errorf("NodeType mismatch: got %d, want %d", deserialized.NodeType, node.NodeType)
	}
	if deserialized.EntryCount != node.EntryCount {
		t.Errorf("EntryCount mismatch: got %d, want %d", deserialized.EntryCount, node.EntryCount)
	}
	if deserialized.NextLeaf != node.NextLeaf {
		t.Errorf("NextLeaf mismatch: got %d, want %d", deserialized.NextLeaf, node.NextLeaf)
	}
	if deserialized.PrevLeaf != node.PrevLeaf {
		t.Errorf("PrevLeaf mismatch: got %d, want %d", deserialized.PrevLeaf, node.PrevLeaf)
	}

	for i := 0; i < int(node.EntryCount); i++ {
		if deserialized.Entries[i].Key.(uint32) != node.Entries[i].Key.(uint32) {
			t.Errorf("Entry[%d] key mismatch: got %d, want %d", i, deserialized.Entries[i].Key, node.Entries[i].Key)
		}
		for j := 0; j < valueSize; j++ {
			if deserialized.Entries[i].Value[j] != node.Entries[i].Value[j] {
				t.Errorf("Entry[%d] value[%d] mismatch: got %d, want %d", i, j, deserialized.Entries[i].Value[j], node.Entries[i].Value[j])
			}
		}
	}
}

func TestMaxEntriesPerBlock(t *testing.T) {
	tests := []struct {
		blockSize int
		keySize   int
		valueSize int
		expected  int
	}{
		{512, 4, 8, (512 - 9) / 12},    // uint32 key + 8 byte value
		{1024, 8, 16, (1024 - 9) / 24}, // uint64 key + 16 byte value
	}

	for _, tt := range tests {
		got := MaxEntriesPerBlock(tt.blockSize, tt.keySize, tt.valueSize)
		if got != tt.expected {
			t.Errorf("MaxEntriesPerBlock(%d, %d, %d) = %d, want %d",
				tt.blockSize, tt.keySize, tt.valueSize, got, tt.expected)
		}
	}
}
