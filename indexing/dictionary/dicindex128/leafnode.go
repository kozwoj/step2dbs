package dicindex128

import (
	"errors"
)

var (
	ErrBlockSizeTooSmall     = errors.New("block size too small")
	ErrLeafNodeFull          = errors.New("leaf node full")
	ErrEntryIndexOutOfBounds = errors.New("entry index out of bounds")
	ErrEntryNotFound         = errors.New("entry not found")
)

/* LeafNode represents a B+ tree leaf node that fits into an index block of BlockSize bytes. Each leaf stores
a fixed number of 24-byte entries.

Structure:
- BlockNumber  uint16     // 2 bytes: block number of this leaf node
- NodeType     uint8      // 1 byte: node type (== 2 for leaf node)
- EntryCount   uint16     // 2 bytes: current number of entries in the node
- NextLeaf     uint16     // 2 bytes: block number of next leaf node (0 if none)
- Entries      []IndexEntry128 // fixed 24-byte entries

Header size: 7 bytes (2 + 1 + 2 + 2)
Each entry: 24 bytes fixed
Maximum entries per block: (BlockSize - 7) / 24

Note:
- Unlike dicindex, entries are fixed 24 bytes (no variable-length strings)
- No Offsets array needed - entries start at byte 7 and are sequential
- Simpler serialization/deserialization due to fixed-length entries
*/

type LeafNode struct {
	BlockNumber uint16
	NodeType    uint8
	EntryCount  uint16
	NextLeaf    uint16
	Entries     []*IndexEntry128 // fixed 24-byte entries
}

const LeafNodeType = uint8(2)
const LeafNodeHeaderSize = 7 // BlockNumber(2) + NodeType(1) + EntryCount(2) + NextLeaf(2)

/*
MaxEntriesPerBlock returns the maximum number of entries that can fit in a leaf block.
Parameters:
- blockSize: size of the block in bytes
Returns:
- int: maximum number of entries
*/
func MaxEntriesPerBlock(blockSize uint16) int {
	return (int(blockSize) - LeafNodeHeaderSize) / EntrySize()
}

/*
SerializeLeafNode writes LeafNode struct into a byte slice representing the block data.
Parameters:
- leaf: pointer to LeafNode struct
- block: byte slice representing the leaf node block
Returns:
- error: if any (e.g. block size too small)
*/
func SerializeLeafNode(leaf *LeafNode, block []byte) error {
	requiredSize := LeafNodeHeaderSize + int(leaf.EntryCount)*EntrySize()
	if requiredSize > len(block) {
		return ErrBlockSizeTooSmall
	}

	// Write header (7 bytes)
	block[0] = byte(leaf.BlockNumber)
	block[1] = byte(leaf.BlockNumber >> 8)
	block[2] = byte(leaf.NodeType)
	block[3] = byte(leaf.EntryCount)
	block[4] = byte(leaf.EntryCount >> 8)
	block[5] = byte(leaf.NextLeaf)
	block[6] = byte(leaf.NextLeaf >> 8)

	// Write entries sequentially starting at byte 7
	pos := LeafNodeHeaderSize
	for i := 0; i < int(leaf.EntryCount); i++ {
		entryBytes := EncodeIndexEntry128(leaf.Entries[i])
		copy(block[pos:pos+EntrySize()], entryBytes)
		pos += EntrySize()
	}

	return nil
}

/*
DeserializeLeafNode reads a byte slice (block data) and reconstructs a LeafNode struct.
Parameters:
- block: byte slice representing the leaf node block
- blockSize: size of the block in bytes
Returns:
- *LeafNode: pointer to deserialized LeafNode struct
- error: if any (e.g. block too short)
*/
func DeserializeLeafNode(block []byte, blockSize uint16) (*LeafNode, error) {
	if len(block) < LeafNodeHeaderSize {
		return nil, ErrBlockSizeTooSmall
	}

	// Read header (7 bytes)
	leaf := &LeafNode{
		BlockNumber: uint16(block[0]) | uint16(block[1])<<8,
		NodeType:    uint8(block[2]),
		EntryCount:  uint16(block[3]) | uint16(block[4])<<8,
		NextLeaf:    uint16(block[5]) | uint16(block[6])<<8,
		Entries:     make([]*IndexEntry128, 0),
	}

	// Read entries sequentially starting at byte 7
	pos := LeafNodeHeaderSize
	for i := 0; i < int(leaf.EntryCount); i++ {
		if pos+EntrySize() > len(block) {
			return nil, ErrBlockSizeTooSmall
		}
		entry, err := DecodeIndexEntry128(block[pos : pos+EntrySize()])
		if err != nil {
			return nil, err
		}
		leaf.Entries = append(leaf.Entries, entry)
		pos += EntrySize()
	}

	return leaf, nil
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
	return block[2] == LeafNodeType
}

/*
IsLeafBlockFull checks if a leaf block has space for a new entry.
Parameters:
- block: byte slice representing the leaf node block
- newEntry: pointer to IndexEntry128 to be inserted
Returns:
- bool: true if the block is full
*/
func IsLeafBlockFull(block []byte, newEntry *IndexEntry128) bool {
	if len(block) < LeafNodeHeaderSize {
		return true
	}
	entryCount := uint16(block[3]) | uint16(block[4])<<8
	maxEntries := MaxEntriesPerBlock(uint16(len(block)))
	return int(entryCount) >= maxEntries
}

/*
GetEntryI retrieves the i-th entry from the leaf node.
Parameters:
- leaf: pointer to LeafNode struct
- i: entry index (0-based)
Returns:
- *IndexEntry128: pointer to the i-th entry
- error: if index is out of bounds
*/
func GetEntryI(leaf *LeafNode, i int) (*IndexEntry128, error) {
	if i < 0 || i >= int(leaf.EntryCount) {
		return nil, ErrEntryIndexOutOfBounds
	}
	return leaf.Entries[i], nil
}

/*
InsertEntryAt inserts a new entry at the specified position in the leaf node.
Parameters:
- leaf: pointer to LeafNode struct
- entry: pointer to IndexEntry128 to insert
- pos: position to insert at (0-based)
Returns:
- error: if any (e.g. node full)
*/
func InsertEntryAt(leaf *LeafNode, entry *IndexEntry128, pos int) error {
	if pos < 0 || pos > int(leaf.EntryCount) {
		return ErrEntryIndexOutOfBounds
	}

	// Insert entry at position by shifting existing entries
	leaf.Entries = append(leaf.Entries[:pos], append([]*IndexEntry128{entry}, leaf.Entries[pos:]...)...)
	leaf.EntryCount++

	return nil
}

/*
InsertEntryToBlock inserts a new entry into the leaf node block in sorted order by hash.
Parameters:
- nodeBlock: byte slice representing the leaf node block
- newEntry: pointer to IndexEntry128 to insert
Returns:
- error: if any (e.g. node full, deserialization failed)
*/
func InsertEntryToBlock(nodeBlock []byte, newEntry *IndexEntry128) error {
	blockSize := uint16(len(nodeBlock))
	leafNode, err := DeserializeLeafNode(nodeBlock, blockSize)
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
		return SerializeLeafNode(leafNode, nodeBlock)
	}

	// Find insertion position (entries are sorted by hash)
	insertPos := int(leafNode.EntryCount) // default to append at the end
	for i := 0; i < int(leafNode.EntryCount); i++ {
		entryI, err := GetEntryI(leafNode, i)
		if err != nil {
			return err
		}
		cmp := CompareHash128(newEntry.Hash, entryI.Hash)
		// Stop when newEntry.Hash < entryI.Hash
		if cmp < 0 {
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
	err = SerializeLeafNode(leafNode, nodeBlock)
	if err != nil {
		return err
	}
	return nil
}

/*
DeleteEntryFromBlock deletes an entry with the specified hash from the leaf node block.
Parameters:
- nodeBlock: byte slice representing the leaf node block
- hash: Hash128 value of the entry to delete
Returns:
- error: if the entry is not found
*/
func DeleteEntryFromBlock(nodeBlock []byte, hash Hash128) error {
	// Deserialize leaf node
	blockSize := uint16(len(nodeBlock))
	leafNode, err := DeserializeLeafNode(nodeBlock, blockSize)
	if err != nil {
		return err
	}

	// Find and remove the entry
	found := false
	for i := 0; i < int(leafNode.EntryCount); i++ {
		if leafNode.Entries[i].Hash.Equal(hash) {
			// Remove entry at index i
			leafNode.Entries = append(leafNode.Entries[:i], leafNode.Entries[i+1:]...)
			leafNode.EntryCount--
			found = true
			break
		}
	}

	if !found {
		return ErrEntryNotFound
	}

	// Serialize back to node block
	return SerializeLeafNode(leafNode, nodeBlock)
}

/*
FindEntryInBlock searches for an entry with the given hash in the leaf node block.
Parameters:
- nodeBlock: byte slice representing the leaf node block
- hash: Hash128 value to search for
Returns:
- *IndexEntry128: pointer to the found entry
- error: if entry not found or deserialization failed
*/
func FindEntryInBlock(nodeBlock []byte, hash Hash128) (*IndexEntry128, error) {
	// Deserialize leaf node
	blockSize := uint16(len(nodeBlock))
	leafNode, err := DeserializeLeafNode(nodeBlock, blockSize)
	if err != nil {
		return nil, err
	}

	// Check if leaf node is empty
	if leafNode.EntryCount == 0 {
		return nil, ErrEntryNotFound
	}

	// Search for entry with matching hash
	for i := 0; i < int(leafNode.EntryCount); i++ {
		entryI, err := GetEntryI(leafNode, i)
		if err != nil {
			return nil, err
		}
		if entryI.Hash.Equal(hash) {
			return entryI, nil
		}
	}

	return nil, ErrEntryNotFound
}

/*
SplitLeafNodeBlock splits a full leaf node block into two blocks:
- Stores the lower half of the entries in the original block
- Creates a new entity with the upper half of the entries in the new block
- Updates the NextLeaf pointer of the original leaf node to point to the new block
- Returns the largest hash in the original block and the smallest hash in the new block

Parameters:
- nodeBlock: byte slice containing the full leaf node data
- newBlock: byte slice where the new leaf node data will be stored
- newBlockNumber: uint16 block number to assign to the new leaf node
Returns:
- leftMaxHash: Hash128 of the largest entry in the original leaf node
- rightMinHash: Hash128 of the smallest entry in the new leaf node
- error: if any

Note: The function does NOT allocate a block for the new node - this must be done by the caller.
*/
func SplitLeafNodeBlock(nodeBlock []byte, newBlock []byte, newBlockNumber uint16) (leftMaxHash Hash128, rightMinHash Hash128, err error) {
	blockSize := uint16(len(nodeBlock))
	
	// Deserialize the original leaf node
	leafNode, err := DeserializeLeafNode(nodeBlock, blockSize)
	if err != nil {
		return Hash128{}, Hash128{}, err
	}

	// Calculate split point (middle of entries)
	midPoint := int(leafNode.EntryCount) / 2

	// Split entries into left and right halves
	leftEntries := leafNode.Entries[:midPoint]
	rightEntries := leafNode.Entries[midPoint:]

	// Create left leaf node (original block)
	leftLeaf := &LeafNode{
		BlockNumber: leafNode.BlockNumber,
		NodeType:    LeafNodeType,
		EntryCount:  uint16(len(leftEntries)),
		NextLeaf:    newBlockNumber, // point to new right leaf
		Entries:     leftEntries,
	}

	// Create right leaf node (new block)
	rightLeaf := &LeafNode{
		BlockNumber: newBlockNumber,
		NodeType:    LeafNodeType,
		EntryCount:  uint16(len(rightEntries)),
		NextLeaf:    leafNode.NextLeaf, // inherit original's next pointer
		Entries:     rightEntries,
	}

	// Get max hash from left and min hash from right
	leftMaxHash = leftEntries[len(leftEntries)-1].Hash
	rightMinHash = rightEntries[0].Hash

	// Serialize both leaf nodes
	err = SerializeLeafNode(leftLeaf, nodeBlock)
	if err != nil {
		return Hash128{}, Hash128{}, err
	}

	err = SerializeLeafNode(rightLeaf, newBlock)
	if err != nil {
		return Hash128{}, Hash128{}, err
	}

	return leftMaxHash, rightMinHash, nil
}
