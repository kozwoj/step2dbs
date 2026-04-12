package dicindex128

import (
	"errors"
)

var (
	ErrInternalNodeTooShort = errors.New("internal node block too short")
	ErrInternalNodeFull     = errors.New("internal node block is full")
	ErrKeyNotFound          = errors.New("key not found")
)

/*
InternalNode represents a B+ tree internal node stored in a block of the index file. An internal node contains
Hash128 keys and references (block numbers) of corresponding child nodes.

Structure:
- BlockNumber: uint16 (2 bytes) - block number of this node
- NodeType: uint8 (1 byte) - type of node (internal = 1)
- KeyCount: uint16 (2 bytes) - number of keys stored in the node
- Keys: []Hash128 - each key is 16 bytes (8 high + 8 low)
- Pointers: []uint16 - KeyCount+1 block pointers (2 bytes each)

Header size: 5 bytes (2 + 1 + 2)
Each key: 16 bytes (Hash128)
Each pointer: 2 bytes (uint16)

Note:
- There are KeyCount+1 pointers because each key separates two child nodes
- Maximum keys per block: (BlockSize - 5 - 2) / 18, where 18 = 16 (key) + 2 (pointer)
*/
type InternalNode struct {
	BlockNumber uint16
	NodeType    uint8
	KeyCount    uint16
	Keys        []Hash128 // 128-bit hash keys
	Pointers    []uint16  // child block pointers
}

const InternalNodeType = uint8(1)
const InternalNodeHeaderSize = 5 // BlockNumber(2) + NodeType(1) + KeyCount(2)

/*
MaxKeysPerInternalBlock returns the maximum number of keys that can fit in an internal block.
Parameters:
- blockSize: size of the block in bytes
Returns:
- int: maximum number of keys

Calculation: (blockSize - 5) / 18, where 18 = 16 (Hash128 key) + 2 (pointer)
We need space for: header(5) + keys(16*n) + pointers(2*(n+1))
= 5 + 16n + 2n + 2 = 7 + 18n
So: 18n <= blockSize - 7, n <= (blockSize - 7) / 18
*/
func MaxKeysPerInternalBlock(blockSize uint16) int {
	return (int(blockSize) - 7) / 18
}

/*
SerializeInternalNode serializes the InternalNode struct into the given block.
Parameters:
- node: pointer to InternalNode struct
- block: byte slice representing the block to serialize into
Returns:
- error: if any (e.g. block too short)
*/
func SerializeInternalNode(node *InternalNode, block []byte) error {
	// Compute the size needed: 5 (header) + 16*KeyCount (keys) + 2*(KeyCount+1) (pointers)
	requiredSize := 5 + (16 * node.KeyCount) + (2 * (node.KeyCount + 1))
	if uint16(len(block)) < requiredSize {
		return ErrInternalNodeTooShort
	}

	// Serialize header (5 bytes)
	block[0] = byte(node.BlockNumber)
	block[1] = byte(node.BlockNumber >> 8)
	block[2] = byte(node.NodeType)
	block[3] = byte(node.KeyCount)
	block[4] = byte(node.KeyCount >> 8)

	// Serialize Keys (16 bytes each)
	offset := 5
	for i := uint16(0); i < node.KeyCount; i++ {
		// Serialize Hash128: High (8 bytes) + Low (8 bytes)
		block[offset] = byte(node.Keys[i].High)
		block[offset+1] = byte(node.Keys[i].High >> 8)
		block[offset+2] = byte(node.Keys[i].High >> 16)
		block[offset+3] = byte(node.Keys[i].High >> 24)
		block[offset+4] = byte(node.Keys[i].High >> 32)
		block[offset+5] = byte(node.Keys[i].High >> 40)
		block[offset+6] = byte(node.Keys[i].High >> 48)
		block[offset+7] = byte(node.Keys[i].High >> 56)
		block[offset+8] = byte(node.Keys[i].Low)
		block[offset+9] = byte(node.Keys[i].Low >> 8)
		block[offset+10] = byte(node.Keys[i].Low >> 16)
		block[offset+11] = byte(node.Keys[i].Low >> 24)
		block[offset+12] = byte(node.Keys[i].Low >> 32)
		block[offset+13] = byte(node.Keys[i].Low >> 40)
		block[offset+14] = byte(node.Keys[i].Low >> 48)
		block[offset+15] = byte(node.Keys[i].Low >> 56)
		offset += 16
	}

	// Serialize Pointers (2 bytes each)
	for i := uint16(0); i < node.KeyCount+1; i++ {
		block[offset] = byte(node.Pointers[i])
		block[offset+1] = byte(node.Pointers[i] >> 8)
		offset += 2
	}

	return nil
}

/*
DeserializeInternalNode deserializes the given block into an InternalNode struct.
Parameters:
- block: byte slice representing the block to deserialize from
- blockSize: size of the block in bytes
Returns:
- *InternalNode: pointer to deserialized InternalNode struct
- error: if any (e.g. block too short)
*/
func DeserializeInternalNode(block []byte, blockSize uint16) (*InternalNode, error) {
	if len(block) < InternalNodeHeaderSize {
		return nil, ErrInternalNodeTooShort
	}

	node := &InternalNode{}

	// Deserialize header (5 bytes)
	node.BlockNumber = uint16(block[0]) | uint16(block[1])<<8
	node.NodeType = uint8(block[2])
	node.KeyCount = uint16(block[3]) | uint16(block[4])<<8

	// Deserialize Keys (16 bytes each)
	node.Keys = make([]Hash128, node.KeyCount)
	offset := 5
	for i := uint16(0); i < node.KeyCount; i++ {
		high := uint64(block[offset]) | uint64(block[offset+1])<<8 | uint64(block[offset+2])<<16 | uint64(block[offset+3])<<24 |
			uint64(block[offset+4])<<32 | uint64(block[offset+5])<<40 | uint64(block[offset+6])<<48 | uint64(block[offset+7])<<56
		low := uint64(block[offset+8]) | uint64(block[offset+9])<<8 | uint64(block[offset+10])<<16 | uint64(block[offset+11])<<24 |
			uint64(block[offset+12])<<32 | uint64(block[offset+13])<<40 | uint64(block[offset+14])<<48 | uint64(block[offset+15])<<56
		node.Keys[i] = Hash128{High: high, Low: low}
		offset += 16
	}

	// Deserialize Pointers (2 bytes each)
	node.Pointers = make([]uint16, node.KeyCount+1)
	for i := uint16(0); i < node.KeyCount+1; i++ {
		node.Pointers[i] = uint16(block[offset]) | uint16(block[offset+1])<<8
		offset += 2
	}

	return node, nil
}

/*
IsInternalBlockFull checks if the internal node is full.
Parameters:
- block: byte slice of the block storing the internal node
Returns:
- bool: true if full, false otherwise
- error: if any (e.g. block too short)
*/
func IsInternalBlockFull(block []byte) (bool, error) {
	if len(block) < InternalNodeHeaderSize {
		return false, ErrInternalNodeTooShort
	}

	blockSize := uint16(len(block))
	keyCount := uint16(block[3]) | uint16(block[4])<<8

	// Used space: 5 (header) + 16*keyCount (keys) + 2*(keyCount+1) (pointers)
	usedSpace := 5 + (16 * keyCount) + (2 * (keyCount + 1))
	availableSpace := blockSize - usedSpace

	// Need 18 bytes for one more key+pointer (16 for Hash128 key + 2 for pointer)
	if availableSpace < 18 {
		return true, nil
	}

	return false, nil
}

/*
InsertKeyAndBlockPointer inserts a key and corresponding child pointer at the correct position.
Parameters:
- nodeBlock: the block slice containing the internal node
- key: Hash128 key to insert
- pointer: uint16 block number of the child node
Returns:
- error: if any (e.g. block too short, node full)
*/
func InsertKeyAndBlockPointer(nodeBlock []byte, key Hash128, pointer uint16) error {
	// Deserialize the node
	blockSize := uint16(len(nodeBlock))
	node, err := DeserializeInternalNode(nodeBlock, blockSize)
	if err != nil {
		return err
	}

	// Check if node is full
	full, err := IsInternalBlockFull(nodeBlock)
	if err != nil {
		return err
	}
	if full {
		return ErrInternalNodeFull
	}

	// Find the position to insert the key (keys are sorted)
	pos := uint16(0)
	for pos < node.KeyCount && CompareHash128(node.Keys[pos], key) < 0 {
		pos++
	}

	// Insert the key at the found position
	node.Keys = append(node.Keys, Hash128{})
	copy(node.Keys[pos+1:], node.Keys[pos:])
	node.Keys[pos] = key

	// Insert the pointer after the key position
	node.Pointers = append(node.Pointers, 0)
	copy(node.Pointers[pos+2:], node.Pointers[pos+1:])
	node.Pointers[pos+1] = pointer

	node.KeyCount++

	// Serialize the updated node back into the block
	return SerializeInternalNode(node, nodeBlock)
}

/*
FindChildBlockPointer finds the correct child block pointer to follow for the given key.
Parameters:
- nodeBlock: the block slice containing the internal node
- key: Hash128 key to search for
Returns:
- uint16: block pointer of the child node to follow
- error: if any (e.g. block too short)
*/
func FindChildBlockPointer(nodeBlock []byte, key Hash128) (uint16, error) {
	// Deserialize the node
	blockSize := uint16(len(nodeBlock))
	node, err := DeserializeInternalNode(nodeBlock, blockSize)
	if err != nil {
		return 0, err
	}

	// If no keys, return the first (only) pointer
	if node.KeyCount == 0 {
		return node.Pointers[0], nil
	}

	// Find the appropriate child pointer
	for i := uint16(0); i < node.KeyCount; i++ {
		if CompareHash128(key, node.Keys[i]) <= 0 {
			return node.Pointers[i], nil
		}
	}

	// If key is greater than all keys, return rightmost pointer
	return node.Pointers[node.KeyCount], nil
}

/*
DeleteKeyAndBlockPointer removes a key and its associated block pointer.
Parameters:
- nodeBlock: the block slice containing the internal node
- key: Hash128 key to delete
Returns:
- error: if any (e.g. block too short, key not found)
*/
func DeleteKeyAndBlockPointer(nodeBlock []byte, key Hash128) error {
	// Deserialize the node
	blockSize := uint16(len(nodeBlock))
	node, err := DeserializeInternalNode(nodeBlock, blockSize)
	if err != nil {
		return err
	}

	// Find the key position
	pos := -1
	for i := 0; i < int(node.KeyCount); i++ {
		if node.Keys[i].Equal(key) {
			pos = i
			break
		}
	}

	if pos == -1 {
		return ErrKeyNotFound
	}

	// Remove key by shifting subsequent keys left
	copy(node.Keys[pos:], node.Keys[pos+1:])
	node.Keys = node.Keys[:len(node.Keys)-1]

	// Remove pointer (the pointer after the key at pos+1) by shifting subsequent pointers left
	copy(node.Pointers[pos+1:], node.Pointers[pos+2:])
	node.Pointers = node.Pointers[:len(node.Pointers)-1]

	// Decrement key count
	node.KeyCount--

	// Serialize the updated node back into the block
	return SerializeInternalNode(node, nodeBlock)
}

/*
SplitInternalNodeBlock splits a full internal node into two nodes.
Parameters:
- nodeBlock: the block slice containing the internal node to split
- newBlock: the block slice for the new right node
- blockNumber: block number to assign to the new right node
Returns:
- middleKey: the middle Hash128 key that will be promoted to parent
- error: if any (e.g. block too short)
*/
func SplitInternalNodeBlock(nodeBlock []byte, newBlock []byte, blockNumber uint16) (Hash128, error) {
	// Deserialize the original node
	blockSize := uint16(len(nodeBlock))
	node, err := DeserializeInternalNode(nodeBlock, blockSize)
	if err != nil {
		return Hash128{}, err
	}

	// Calculate the middle point
	mid := int(node.KeyCount) / 2

	// The middle key to promote
	middleKey := node.Keys[mid]

	// Create new right node with keys after the middle
	newNode := &InternalNode{
		BlockNumber: blockNumber,
		NodeType:    InternalNodeType,
		KeyCount:    uint16(int(node.KeyCount) - mid - 1),
		Keys:        make([]Hash128, int(node.KeyCount)-mid-1),
		Pointers:    make([]uint16, int(node.KeyCount)-mid),
	}
	copy(newNode.Keys, node.Keys[mid+1:])
	copy(newNode.Pointers, node.Pointers[mid+1:])

	// Update original left node to keep keys before the middle
	node.Keys = node.Keys[:mid]
	node.Pointers = node.Pointers[:mid+1]
	node.KeyCount = uint16(mid)

	// Serialize both nodes
	err = SerializeInternalNode(node, nodeBlock)
	if err != nil {
		return Hash128{}, err
	}

	err = SerializeInternalNode(newNode, newBlock)
	if err != nil {
		return Hash128{}, err
	}

	return middleKey, nil
}
