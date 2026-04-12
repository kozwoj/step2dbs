package primindex

import (
	"bytes"
	"encoding/binary"
	"errors"
)

var (
	ErrKeyNotFound          = errors.New("key not found in internal node")
	ErrInternalNodeOverflow = errors.New("internal node data overflow")
)

// InternalNode represents a B+-tree internal node with keys and child pointers
type InternalNode struct {
	BlockNumber uint16
	NodeType    uint8 // == 1 for internal node
	KeyCount    uint16
	Keys        [][]byte // serialized key values
	Pointers    []uint16 // child block pointers
}

/*
SerializeInternalNode serializes an InternalNode into a byte slice for storage.
Parameters:
- node: pointer to InternalNode struct
- blockSize: size of the block in bytes
- keySize: size of the key in bytes
Returns:
- []byte: serialized byte slice of length blockSize
- error: if any (e.g. node overflow)
*/
func SerializeInternalNode(node *InternalNode, blockSize int, keySize int) ([]byte, error) {
	buf := make([]byte, blockSize)

	// Header: BlockNumber (2) + NodeType (1) + KeyCount (2) = 5 bytes
	binary.LittleEndian.PutUint16(buf[0:2], node.BlockNumber)
	buf[2] = node.NodeType
	binary.LittleEndian.PutUint16(buf[3:5], node.KeyCount)

	// Keys start at offset 5
	offset := 5

	// Serialize keys
	for i := 0; i < int(node.KeyCount); i++ {
		if offset+keySize > blockSize {
			return nil, ErrNodeOverflow
		}
		copy(buf[offset:offset+keySize], node.Keys[i])
		offset += keySize
	}

	// Serialize pointers (KeyCount + 1 pointers, each 2 bytes)
	pointerCount := int(node.KeyCount) + 1
	for i := 0; i < pointerCount; i++ {
		if offset+2 > blockSize {
			return nil, ErrNodeOverflow
		}
		binary.LittleEndian.PutUint16(buf[offset:offset+2], node.Pointers[i])
		offset += 2
	}

	return buf, nil
}

/*
DeserializeInternalNode deserializes a byte slice into an InternalNode struct.
Parameters:
- data: byte slice of block data
- keySize: size of the key in bytes
Returns:
- *InternalNode: deserialized InternalNode struct
- error: if any (e.g. invalid node type)
*/
func DeserializeInternalNode(data []byte, keySize int) (*InternalNode, error) {
	if len(data) < 5 {
		return nil, ErrBlockTooShort
	}

	node := &InternalNode{}
	node.BlockNumber = binary.LittleEndian.Uint16(data[0:2])
	node.NodeType = data[2]
	node.KeyCount = binary.LittleEndian.Uint16(data[3:5])

	if node.NodeType != NodeTypeInternal {
		return nil, ErrInvalidNodeType
	}

	// Deserialize keys
	offset := 5
	node.Keys = make([][]byte, node.KeyCount)

	for i := 0; i < int(node.KeyCount); i++ {
		if offset+keySize > len(data) {
			return nil, ErrBlockTooShort
		}
		key := make([]byte, keySize)
		copy(key, data[offset:offset+keySize])
		node.Keys[i] = key
		offset += keySize
	}

	// Deserialize pointers (KeyCount + 1 pointers)
	pointerCount := int(node.KeyCount) + 1
	node.Pointers = make([]uint16, pointerCount)

	for i := 0; i < pointerCount; i++ {
		if offset+2 > len(data) {
			return nil, ErrBlockTooShort
		}
		node.Pointers[i] = binary.LittleEndian.Uint16(data[offset : offset+2])
		offset += 2
	}

	return node, nil
}

/*
IsInternalBlockFull checks if the internal node block is full and cannot accept another key+pointer.
Parameters:
- data: byte slice of block data
- keySize: size of the key in bytes
Returns:
- bool: true if the block is full, false otherwise
- error: if any (e.g. block too short)
*/
func IsInternalBlockFull(data []byte, keySize int) (bool, error) {
	if len(data) < 5 {
		return false, ErrBlockTooShort
	}

	blockSize := len(data)
	keyCount := binary.LittleEndian.Uint16(data[3:5])

	// Calculate used space:
	// Header: 5 bytes
	// Keys: keySize * keyCount bytes
	// Pointers: 2 * (keyCount + 1) bytes
	usedSpace := 5 + (keySize * int(keyCount)) + (2 * (int(keyCount) + 1))

	// Calculate available space
	availableSpace := blockSize - usedSpace

	// Need keySize bytes for one more key + 2 bytes for one more pointer
	requiredSpace := keySize + 2

	if availableSpace < requiredSpace {
		return true, nil
	}

	return false, nil
}

/*
InsertKeyAndBlockPointer inserts a key and corresponding child pointer at the correct position in sorted order.
Parameters:
- nodeBlock: the block slice containing the internal node
- key: serialized key to insert
- pointer: uint16 block number of the child node
- codec: KeyCodec for comparing keys
- keySize: size of the key in bytes
Returns:
- error: if any (e.g. block too short, node full)
*/
func InsertKeyAndBlockPointer(nodeBlock []byte, key []byte, pointer uint16, codec KeyCodec, keySize int) error {
	// Deserialize the node
	node, err := DeserializeInternalNode(nodeBlock, keySize)
	if err != nil {
		return err
	}

	// Check if node is full
	full, err := IsInternalBlockFull(nodeBlock, keySize)
	if err != nil {
		return err
	}
	if full {
		return ErrInternalNodeOverflow
	}

	// Find the position to insert the key (keys are sorted)
	pos := 0
	for pos < int(node.KeyCount) && codec.Compare(node.Keys[pos], key) < 0 {
		pos++
	}

	// Insert the key at the found position
	node.Keys = append(node.Keys, nil)
	copy(node.Keys[pos+1:], node.Keys[pos:])
	node.Keys[pos] = key

	// Insert the pointer after the key position
	// In a B+ tree, pointer[i] points to children with keys < Keys[i]
	// and pointer[i+1] points to children with keys >= Keys[i]
	node.Pointers = append(node.Pointers, 0)
	copy(node.Pointers[pos+2:], node.Pointers[pos+1:])
	node.Pointers[pos+1] = pointer

	node.KeyCount++

	// Serialize the updated node back into the block
	blockSize := len(nodeBlock)
	serialized, err := SerializeInternalNode(node, blockSize, keySize)
	if err != nil {
		return err
	}
	copy(nodeBlock, serialized)

	return nil
}

/*
FindChildBlockPointer finds the correct child block pointer to follow for the given key.
Parameters:
- nodeBlock: the block slice containing the internal node
- key: serialized key to search for
- codec: KeyCodec for comparing keys
- keySize: size of the key in bytes
Returns:
- uint16: block pointer of the child node to follow
- error: if any (e.g. block too short)
*/
func FindChildBlockPointer(nodeBlock []byte, key []byte, codec KeyCodec, keySize int) (uint16, error) {
	// Deserialize the node
	node, err := DeserializeInternalNode(nodeBlock, keySize)
	if err != nil {
		return 0, err
	}

	// If no keys, return the first (only) pointer
	if node.KeyCount == 0 {
		return node.Pointers[0], nil
	}

	// Find the appropriate child pointer
	// In a B+ tree, Keys[i] represents the minimum key in the right subtree (Pointers[i+1])
	// If key < Keys[i], go to Pointers[i] (left)
	// If key >= Keys[i], continue searching or go to Pointers[i+1] (right)
	for i := 0; i < int(node.KeyCount); i++ {
		if codec.Compare(key, node.Keys[i]) < 0 {
			return node.Pointers[i], nil
		}
	}

	// If key is greater than all keys, return rightmost pointer
	return node.Pointers[node.KeyCount], nil
}

/*
DeleteKeyAndBlockPointer removes a key and its associated block pointer from an internal node.
Parameters:
- nodeBlock: the block slice containing the internal node
- key: serialized key to delete
- codec: KeyCodec for comparing keys
- keySize: size of the key in bytes
Returns:
- error: if any (e.g. block too short, key not found)

Note: This function performs the deletion but does NOT handle underflow conditions.
The caller is responsible for detecting and handling cases where KeyCount becomes 0
or falls below the minimum for non-root nodes (tree rebalancing, merging, etc.).
*/
func DeleteKeyAndBlockPointer(nodeBlock []byte, key []byte, codec KeyCodec, keySize int) error {
	// Deserialize the node
	node, err := DeserializeInternalNode(nodeBlock, keySize)
	if err != nil {
		return err
	}

	// Find the key position using codec.Compare
	pos := -1
	for i := 0; i < int(node.KeyCount); i++ {
		if bytes.Equal(node.Keys[i], key) {
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

	// Remove pointer at pos+1 (the right child of the deleted key) by shifting subsequent pointers left
	copy(node.Pointers[pos+1:], node.Pointers[pos+2:])
	node.Pointers = node.Pointers[:len(node.Pointers)-1]

	// Decrement key count
	node.KeyCount--

	// Serialize the updated node back into the block
	blockSize := len(nodeBlock)
	serialized, err := SerializeInternalNode(node, blockSize, keySize)
	if err != nil {
		return err
	}
	copy(nodeBlock, serialized)

	return nil
}

/*
SplitInternalNodeBlock splits a full internal node into two nodes.
Parameters:
- nodeBlock: the block slice containing the internal node to split
- newBlock: the block slice for the new right node
- newBlockNumber: block number to assign to the new right node
- keySize: size of the key in bytes
Returns:
- middleKey: the middle key (serialized) that will be promoted to parent
- error: if any (e.g. block too short)

Note: In B+ tree internal node splits, the middle key is promoted to the parent.
The left node keeps keys [0..mid-1], the right node gets keys [mid+1..end],
and the middle key at position mid goes up to the parent.
*/
func SplitInternalNodeBlock(nodeBlock []byte, newBlock []byte, newBlockNumber uint16, keySize int) ([]byte, error) {
	// Deserialize the original node
	node, err := DeserializeInternalNode(nodeBlock, keySize)
	if err != nil {
		return nil, err
	}

	// Calculate the middle point
	mid := int(node.KeyCount) / 2

	// The middle key to promote to parent
	middleKey := make([]byte, keySize)
	copy(middleKey, node.Keys[mid])

	// Create new right node with keys after the middle
	newNode := &InternalNode{
		BlockNumber: newBlockNumber,
		NodeType:    NodeTypeInternal,
		KeyCount:    uint16(int(node.KeyCount) - mid - 1),
		Keys:        make([][]byte, int(node.KeyCount)-mid-1),
		Pointers:    make([]uint16, int(node.KeyCount)-mid),
	}

	// Copy keys after middle to new node
	for i := 0; i < int(newNode.KeyCount); i++ {
		newNode.Keys[i] = make([]byte, keySize)
		copy(newNode.Keys[i], node.Keys[mid+1+i])
	}

	// Copy pointers after middle to new node (mid+1 onward)
	copy(newNode.Pointers, node.Pointers[mid+1:])

	// Update original left node to keep keys before the middle
	node.Keys = node.Keys[:mid]
	node.Pointers = node.Pointers[:mid+1]
	node.KeyCount = uint16(mid)

	// Serialize both nodes
	blockSize := len(nodeBlock)
	leftSerialized, err := SerializeInternalNode(node, blockSize, keySize)
	if err != nil {
		return nil, err
	}
	copy(nodeBlock, leftSerialized)

	rightSerialized, err := SerializeInternalNode(newNode, blockSize, keySize)
	if err != nil {
		return nil, err
	}
	copy(newBlock, rightSerialized)

	return middleKey, nil
}
