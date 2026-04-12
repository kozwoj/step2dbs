package primindex

import (
	"encoding/binary"
	"errors"
)

const (
	NodeTypeInternal uint8 = 1
	NodeTypeLeaf     uint8 = 2
	NodeTypeEmpty    uint8 = 3

	NoNextLeaf uint16 = 0xFFFF // Sentinel value indicating end of leaf linked list
	NoPrevLeaf uint16 = 0xFFFF // Sentinel value indicating no previous leaf (first leaf in the chain)
)

var (
	ErrInvalidNodeType = errors.New("invalid node type")
	ErrNodeOverflow    = errors.New("node data overflow")
)

// LeafNode represents a B+-tree leaf node that holds index entries
type LeafNode struct {
	BlockNumber uint16
	NodeType    uint8 // == 2 for leaf node
	EntryCount  uint16
	NextLeaf    uint16
	PrevLeaf    uint16
	Entries     []*IndexEntry
}

/*
SerializeLeafNode serializes a LeafNode into a byte slice for storage.
Parameters:
- node: pointer to LeafNode struct
- blockSize: size of the block in bytes
- codec: KeyCodec for serializing keys
- valueSize: size of the value in bytes
Returns:
- []byte: serialized byte slice of length blockSize
- error: if any (e.g. node overflow)
*/
func SerializeLeafNode(node *LeafNode, blockSize int, codec KeyCodec, valueSize int) ([]byte, error) {
	buf := make([]byte, blockSize)

	// Header: BlockNumber (2) + NodeType (1) + EntryCount (2) + NextLeaf (2) + PrevLeaf (2) = 9 bytes
	binary.LittleEndian.PutUint16(buf[0:2], node.BlockNumber)
	buf[2] = node.NodeType
	binary.LittleEndian.PutUint16(buf[3:5], node.EntryCount)
	binary.LittleEndian.PutUint16(buf[5:7], node.NextLeaf)
	binary.LittleEndian.PutUint16(buf[7:9], node.PrevLeaf)

	// Entries start at offset 9
	offset := 9
	keySize := codec.Size()
	entrySize := keySize + valueSize

	for i := 0; i < int(node.EntryCount); i++ {
		if offset+entrySize > blockSize {
			return nil, ErrNodeOverflow
		}

		entryBytes, err := EncodeIndexEntry(node.Entries[i], codec)
		if err != nil {
			return nil, err
		}
		copy(buf[offset:offset+entrySize], entryBytes)
		offset += entrySize
	}

	return buf, nil
}

/*
DeserializeLeafNode deserializes a byte slice into a LeafNode struct.
Parameters:
- data: byte slice of block data
- codec: KeyCodec for deserializing keys
- valueSize: size of the value in bytes
Returns:
- *LeafNode: deserialized LeafNode struct
- error: if any (e.g. invalid node type)
*/
func DeserializeLeafNode(data []byte, codec KeyCodec, valueSize int) (*LeafNode, error) {
	if len(data) < 9 {
		return nil, ErrBlockTooShort
	}

	node := &LeafNode{}
	node.BlockNumber = binary.LittleEndian.Uint16(data[0:2])
	node.NodeType = data[2]
	node.EntryCount = binary.LittleEndian.Uint16(data[3:5])
	node.NextLeaf = binary.LittleEndian.Uint16(data[5:7])
	node.PrevLeaf = binary.LittleEndian.Uint16(data[7:9])

	if node.NodeType != NodeTypeLeaf {
		return nil, ErrInvalidNodeType
	}

	// Deserialize entries
	offset := 9
	keySize := codec.Size()
	entrySize := keySize + valueSize
	node.Entries = make([]*IndexEntry, node.EntryCount)

	for i := 0; i < int(node.EntryCount); i++ {
		if offset+entrySize > len(data) {
			return nil, ErrBlockTooShort
		}

		entry, err := DecodeIndexEntry(data[offset:offset+entrySize], codec, valueSize)
		if err != nil {
			return nil, err
		}
		node.Entries[i] = entry
		offset += entrySize
	}

	return node, nil
}

/*
MaxEntriesPerBlock returns the maximum number of entries that can fit in a leaf block.
Parameters:
- blockSize: size of the block in bytes
- keySize: size of the key in bytes
- valueSize: size of the value in bytes
Returns:
- int: maximum number of entries
*/
func MaxEntriesPerBlock(blockSize int, keySize int, valueSize int) int {
	headerSize := 9 // BlockNumber(2) + NodeType(1) + EntryCount(2) + NextLeaf(2) + PrevLeaf(2)
	entrySize := keySize + valueSize
	return (blockSize - headerSize) / entrySize
}

/*
IsLeafNode checks if a block represents a leaf node by examining the NodeType byte.
Parameters:
- block: byte slice representing the node block
Returns:
- bool: true if the block is a leaf node
*/
func IsLeafNode(block []byte) bool {
	if len(block) < 3 {
		return false
	}
	return block[2] == NodeTypeLeaf
}

/*
IsLeafBlockFull checks if a leaf block has space for a new entry.
Parameters:
- block: byte slice representing the leaf node block
- keySize: size of the key in bytes
- valueSize: size of the value in bytes
Returns:
- bool: true if the block is full
*/
func IsLeafBlockFull(block []byte, keySize int, valueSize int) bool {
	if len(block) < 7 {
		return true
	}
	entryCount := binary.LittleEndian.Uint16(block[3:5])
	maxEntries := MaxEntriesPerBlock(len(block), keySize, valueSize)
	return int(entryCount) >= maxEntries
}

/*
GetEntryI retrieves the i-th entry from the leaf node.
Parameters:
- leaf: pointer to LeafNode struct
- i: entry index (0-based)
Returns:
- *IndexEntry: pointer to the i-th entry
- error: if index is out of bounds
*/
func GetEntryI(leaf *LeafNode, i int) (*IndexEntry, error) {
	if i < 0 || i >= int(leaf.EntryCount) {
		return nil, errors.New("entry index out of bounds")
	}
	return leaf.Entries[i], nil
}

/*
InsertEntryAt inserts a new entry at the specified position in the leaf node.
Parameters:
- leaf: pointer to LeafNode struct
- entry: pointer to IndexEntry to insert
- pos: position to insert at (0-based)
Returns:
- error: if any (e.g. position out of bounds)

Note: This function does NOT check if the node is full. The caller must ensure
there is sufficient space before calling this function.
*/
func InsertEntryAt(leaf *LeafNode, entry *IndexEntry, pos int) error {
	if pos < 0 || pos > int(leaf.EntryCount) {
		return errors.New("entry index out of bounds")
	}

	// Insert entry at position by shifting existing entries
	leaf.Entries = append(leaf.Entries[:pos], append([]*IndexEntry{entry}, leaf.Entries[pos:]...)...)
	leaf.EntryCount++

	return nil
}

/*
InsertEntryToBlock inserts a new entry into the leaf node block in sorted order by key.
Parameters:
- nodeBlock: byte slice representing the leaf node block
- newEntry: pointer to IndexEntry to insert
- codec: KeyCodec for comparing keys
- valueSize: size of the value in bytes
Returns:
- error: if any (e.g. node full, deserialization failed)
*/
func InsertEntryToBlock(nodeBlock []byte, newEntry *IndexEntry, codec KeyCodec, valueSize int) error {
	leafNode, err := DeserializeLeafNode(nodeBlock, codec, valueSize)
	if err != nil {
		return err
	}

	// Serialize the new entry's key for comparison
	newKeyBytes, err := codec.Serialize(newEntry.Key)
	if err != nil {
		return err
	}

	// If leaf node has no entries yet, insert at position 0
	if leafNode.EntryCount == 0 {
		err = InsertEntryAt(leafNode, newEntry, 0)
		if err != nil {
			return err
		}
		// Serialize back to node block
		blockSize := len(nodeBlock)
		serialized, err := SerializeLeafNode(leafNode, blockSize, codec, valueSize)
		if err != nil {
			return err
		}
		copy(nodeBlock, serialized)
		return nil
	}

	// Find insertion position (entries are sorted by key)
	insertPos := int(leafNode.EntryCount) // default to append at the end
	for i := 0; i < int(leafNode.EntryCount); i++ {
		entryI, err := GetEntryI(leafNode, i)
		if err != nil {
			return err
		}
		// Serialize current entry's key for comparison
		entryKeyBytes, err := codec.Serialize(entryI.Key)
		if err != nil {
			return err
		}
		// Stop when newKey < entryKey
		if codec.Compare(newKeyBytes, entryKeyBytes) < 0 {
			insertPos = i
			break
		}
	}

	// Insert entry at insertPos
	err = InsertEntryAt(leafNode, newEntry, insertPos)
	if err != nil {
		return err
	}

	// Serialize back to node block
	blockSize := len(nodeBlock)
	serialized, err := SerializeLeafNode(leafNode, blockSize, codec, valueSize)
	if err != nil {
		return err
	}
	copy(nodeBlock, serialized)
	return nil
}

/*
GetMinimumKeyFromLeaf returns the minimum (first) key from a leaf node block.
Parameters:
- leafBlock: byte slice representing the leaf node block
- codec: KeyCodec for deserializing keys
- valueSize: size of the value in bytes
Returns:
- []byte: serialized minimum key
- error: if the leaf is empty or deserialization failed
*/
func GetMinimumKeyFromLeaf(leafBlock []byte, codec KeyCodec, valueSize int) ([]byte, error) {
	leafNode, err := DeserializeLeafNode(leafBlock, codec, valueSize)
	if err != nil {
		return nil, err
	}

	if leafNode.EntryCount == 0 {
		return nil, errors.New("cannot get minimum key from empty leaf")
	}

	// First entry has the minimum key (entries are sorted)
	firstEntry := leafNode.Entries[0]
	minimumKey, err := codec.Serialize(firstEntry.Key)
	if err != nil {
		return nil, err
	}

	return minimumKey, nil
}

/*
DeleteEntryFromBlock deletes an entry with the specified key from the leaf node block.
Parameters:
- nodeBlock: byte slice representing the leaf node block
- key: key value of the entry to delete (will be serialized using codec)
- codec: KeyCodec for comparing keys
- valueSize: size of the value in bytes
Returns:
- error: if the entry is not found or deserialization failed

Note: This function performs the deletion but does NOT handle underflow conditions.
The caller is responsible for detecting and handling cases where EntryCount becomes too low
(tree rebalancing, merging with siblings, etc.).
*/
func DeleteEntryFromBlock(nodeBlock []byte, key interface{}, codec KeyCodec, valueSize int) error {
	// Deserialize leaf node
	leafNode, err := DeserializeLeafNode(nodeBlock, codec, valueSize)
	if err != nil {
		return err
	}

	// Serialize the key to search for
	keyBytes, err := codec.Serialize(key)
	if err != nil {
		return err
	}

	// Find and remove the entry
	found := false
	for i := 0; i < int(leafNode.EntryCount); i++ {
		entryKeyBytes, err := codec.Serialize(leafNode.Entries[i].Key)
		if err != nil {
			return err
		}
		// Check if keys match
		if codec.Compare(keyBytes, entryKeyBytes) == 0 {
			// Remove entry at index i
			leafNode.Entries = append(leafNode.Entries[:i], leafNode.Entries[i+1:]...)
			leafNode.EntryCount--
			found = true
			break
		}
	}

	if !found {
		return errors.New("entry not found")
	}

	// Serialize back to node block
	blockSize := len(nodeBlock)
	serialized, err := SerializeLeafNode(leafNode, blockSize, codec, valueSize)
	if err != nil {
		return err
	}
	copy(nodeBlock, serialized)
	return nil
}

/*
FindEntryInBlock searches for an entry with the given key in the leaf node block.
Parameters:
- nodeBlock: byte slice representing the leaf node block
- key: key value to search for
- codec: KeyCodec for comparing keys
- valueSize: size of the value in bytes
Returns:
- *IndexEntry: pointer to the found entry
- error: if entry not found or deserialization failed
*/
func FindEntryInBlock(nodeBlock []byte, key interface{}, codec KeyCodec, valueSize int) (*IndexEntry, error) {
	// Deserialize leaf node
	leafNode, err := DeserializeLeafNode(nodeBlock, codec, valueSize)
	if err != nil {
		return nil, err
	}

	// Check if leaf node is empty
	if leafNode.EntryCount == 0 {
		return nil, errors.New("entry not found")
	}

	// Serialize the search key
	keyBytes, err := codec.Serialize(key)
	if err != nil {
		return nil, err
	}

	// Search for entry with matching key
	for i := 0; i < int(leafNode.EntryCount); i++ {
		entryI, err := GetEntryI(leafNode, i)
		if err != nil {
			return nil, err
		}
		entryKeyBytes, err := codec.Serialize(entryI.Key)
		if err != nil {
			return nil, err
		}
		if codec.Compare(keyBytes, entryKeyBytes) == 0 {
			return entryI, nil
		}
	}

	return nil, errors.New("entry not found")
}

/*
SplitLeafNodeBlock splits a full leaf node block into two blocks.
Parameters:
- nodeBlock: byte slice containing the full leaf node data
- newBlock: byte slice where the new leaf node data will be stored
- newBlockNumber: uint16 block number to assign to the new leaf node
- codec: KeyCodec for serializing keys
- valueSize: size of the value in bytes
Returns:
- leftMaxKey: serialized key of the largest entry in the original leaf node
- rightMinKey: serialized key of the smallest entry in the new leaf node
- error: if any

Note: This function splits entries at the midpoint. The left node gets the lower half
and the right node gets the upper half. The NextLeaf pointer is updated to maintain
the linked list of leaf nodes. The function does NOT allocate a block for the new node -
this must be done by the caller.
*/
func SplitLeafNodeBlock(nodeBlock []byte, newBlock []byte, newBlockNumber uint16, codec KeyCodec, valueSize int) (leftMaxKey []byte, rightMinKey []byte, err error) {
	// Deserialize the original leaf node
	leafNode, err := DeserializeLeafNode(nodeBlock, codec, valueSize)
	if err != nil {
		return nil, nil, err
	}

	// Calculate split point (middle of entries)
	midPoint := int(leafNode.EntryCount) / 2

	// Split entries into left and right halves
	leftEntries := leafNode.Entries[:midPoint]
	rightEntries := leafNode.Entries[midPoint:]

	// Create left leaf node (original block)
	leftLeaf := &LeafNode{
		BlockNumber: leafNode.BlockNumber,
		NodeType:    NodeTypeLeaf,
		EntryCount:  uint16(len(leftEntries)),
		NextLeaf:    newBlockNumber,    // point to new right leaf
		PrevLeaf:    leafNode.PrevLeaf, // preserve original's prev pointer
		Entries:     leftEntries,
	}

	// Create right leaf node (new block)
	rightLeaf := &LeafNode{
		BlockNumber: newBlockNumber,
		NodeType:    NodeTypeLeaf,
		EntryCount:  uint16(len(rightEntries)),
		NextLeaf:    leafNode.NextLeaf,    // inherit original's next pointer
		PrevLeaf:    leafNode.BlockNumber, // points back to left leaf
		Entries:     rightEntries,
	}

	// Get max key from left and min key from right (serialized for parent insertion)
	leftMaxKey, err = codec.Serialize(leftEntries[len(leftEntries)-1].Key)
	if err != nil {
		return nil, nil, err
	}

	rightMinKey, err = codec.Serialize(rightEntries[0].Key)
	if err != nil {
		return nil, nil, err
	}

	// Serialize both leaf nodes
	blockSize := len(nodeBlock)
	leftSerialized, err := SerializeLeafNode(leftLeaf, blockSize, codec, valueSize)
	if err != nil {
		return nil, nil, err
	}
	copy(nodeBlock, leftSerialized)

	rightSerialized, err := SerializeLeafNode(rightLeaf, blockSize, codec, valueSize)
	if err != nil {
		return nil, nil, err
	}
	copy(newBlock, rightSerialized)

	return leftMaxKey, rightMinKey, nil
}
