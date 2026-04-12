package primindex

import (
	"errors"
	"os"
	"path/filepath"
)

// Custom error variables for index file operations
var (
	ErrBlockTooShort   = errors.New("index file block too short")
	ErrFileCreate      = errors.New("failed to create index file")
	ErrHeaderSerialize = errors.New("failed to serialize index file header")
	ErrHeaderWrite     = errors.New("failed to write index file header")
	ErrFileTruncate    = errors.New("failed to expand index file to required size")
	ErrDirNotExist     = errors.New("directory does not exist")
	ErrHeaderOverflow  = errors.New("index file header overflow")
	ErrInvalidKeyType  = errors.New("invalid key type")
)

// Index represents an open primary index file.
type Index struct {
	Header *IndexHeader
	Codec  KeyCodec
	File   *os.File
}

/*
IndexHeader represents the metadata stored in block 0 of the index file.
Fields:
- BlockSize       uint16 // block size in bytes (usually 512)
- FileLength      uint16 // total length of file in blocks
- KeyType         uint8  // type of key (uint8, uint16, uint32, uint64, or fixed bytes)
- ValueSize       uint32 // size of value in bytes
- RootNode        uint16 // block number of root node
- FirstLeaf       uint16 // block number of first leaf node
- NextEmptyBlock  uint16 // next empty/reusable block number

The size of the IndexHeader struct is 15 bytes, and is written at the start of the file. Hence
block N starts at offset (N * BlockSize + 15) in the file.
*/
type IndexHeader struct {
	BlockSize      uint16
	FileLength     uint16 // in blocks
	KeyType        uint8
	ValueSize      uint32
	RootNode       uint16
	FirstLeaf      uint16
	NextEmptyBlock uint16 // next empty/reusable block number
}

const IndexHeaderSize = 15

/*
SerializeIndexHeader serializes the IndexHeader struct into the first 15 bytes of block 0.
Parameters:
- header: pointer to IndexHeader struct
Returns:
- []byte: serialized 15-byte slice with header data
- error: if any
*/
func SerializeIndexHeader(header *IndexHeader) ([]byte, error) {
	buf := make([]byte, IndexHeaderSize)
	buf[0] = byte(header.BlockSize)
	buf[1] = byte(header.BlockSize >> 8)
	buf[2] = byte(header.FileLength)
	buf[3] = byte(header.FileLength >> 8)
	buf[4] = byte(header.KeyType)
	buf[5] = byte(header.ValueSize)
	buf[6] = byte(header.ValueSize >> 8)
	buf[7] = byte(header.ValueSize >> 16)
	buf[8] = byte(header.ValueSize >> 24)
	buf[9] = byte(header.RootNode)
	buf[10] = byte(header.RootNode >> 8)
	buf[11] = byte(header.FirstLeaf)
	buf[12] = byte(header.FirstLeaf >> 8)
	buf[13] = byte(header.NextEmptyBlock)
	buf[14] = byte(header.NextEmptyBlock >> 8)
	return buf, nil
}

/*
DeserializeIndexHeader deserializes the first 15 bytes of block 0 into an IndexHeader struct.
Parameters:
- data: byte slice of length at least 15 bytes (the block data)
Returns:
- *IndexHeader: deserialized IndexHeader struct
- error: if any (e.g. block too short)
*/
func DeserializeIndexHeader(data []byte) (*IndexHeader, error) {
	if len(data) < IndexHeaderSize {
		return nil, ErrBlockTooShort
	}
	header := &IndexHeader{}
	header.BlockSize = uint16(data[0]) | uint16(data[1])<<8
	header.FileLength = uint16(data[2]) | uint16(data[3])<<8
	header.KeyType = uint8(data[4])
	header.ValueSize = uint32(data[5]) | uint32(data[6])<<8 | uint32(data[7])<<16 | uint32(data[8])<<24
	header.RootNode = uint16(data[9]) | uint16(data[10])<<8
	header.FirstLeaf = uint16(data[11]) | uint16(data[12])<<8
	header.NextEmptyBlock = uint16(data[13]) | uint16(data[14])<<8
	return header, nil
}

/*
WriteIndexBlock writes a block to index file.
Parameters:
- index: pointer to Index struct
- blockNumber: block number to write to
- data: []byte slice of data of the length of block size
Returns:
- error: if any (e.g. write failed)
*/
func WriteIndexBlock(index *Index, blockNumber int, data []byte) error {
	blockSize := int(index.Header.BlockSize)
	offset := blockNumber*blockSize + IndexHeaderSize
	_, err := index.File.WriteAt(data[:], int64(offset))
	return err
}

/*
ReadIndexBlock reads a block from index file.
Parameters:
- index: pointer to Index struct
- blockNumber: block number to read from
Returns:
- []byte: byte slice of block data of the length of block size
- error: if any (e.g. read failed)
*/
func ReadIndexBlock(index *Index, blockNumber int) ([]byte, error) {
	blockSize := int(index.Header.BlockSize)
	offset := blockNumber*blockSize + IndexHeaderSize
	data := make([]byte, blockSize)
	_, err := index.File.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	return data, nil
}

/*
CreateIndexFile creates a new index file with the specified parameters.
Parameters:
- path: directory path for the index file
- filename: full name of the index file (e.g., "index.dat" or "myindex.indx")
- blockSize: the size of the index blocks in bytes
- blockCount: number of preallocated blocks
- keyType: type of key (KeyTypeUint8, KeyTypeUint16, etc.)
- valueSize: the length (number of bytes) of the value part of the leaf node entry
Returns:
- error: if any (e.g. file creation failed)
*/
func CreateIndexFile(path string, filename string, blockSize uint16, blockCount uint16, keyType KeyType, valueSize uint32) error {
	// Check if directory exists
	if _, err := os.Stat(path); os.IsNotExist(err) {
		return ErrDirNotExist
	}

	// Validate key type
	if keyType < KeyTypeUint8 || keyType > KeyTypePrefixBytes8 {
		return ErrInvalidKeyType
	}

	// Create the index file
	indexFilePath := filepath.Join(path, filename)
	file, err := os.Create(indexFilePath)
	if err != nil {
		return ErrFileCreate
	}
	defer file.Close()

	// Initialize header
	// The root node is initially a leaf node at block 0 (first block after header)
	header := &IndexHeader{
		BlockSize:      blockSize,
		FileLength:     2, // Initially only 2 blocks in use: block 0 (root) and block 1 (empty)
		KeyType:        uint8(keyType),
		ValueSize:      valueSize,
		RootNode:       0, // root node is at block 0
		FirstLeaf:      0, // first leaf is also at block 0 (root is initially a leaf)
		NextEmptyBlock: 1, // first available block for allocation is block 1
	}

	// Serialize and write header
	headerData, err := SerializeIndexHeader(header)
	if err != nil {
		return ErrHeaderSerialize
	}

	if _, err := file.WriteAt(headerData, 0); err != nil {
		return ErrHeaderWrite
	}

	// Create initial empty root/leaf node at block 0
	// Get codec for the key type
	codec := KeyCodecFactory(keyType)

	// Create an empty leaf node
	initialLeaf := &LeafNode{
		BlockNumber: 0,
		NodeType:    NodeTypeLeaf, // 2 for leaf node
		EntryCount:  0,
		NextLeaf:    NoNextLeaf, // no next leaf (end of list)
		PrevLeaf:    NoPrevLeaf, // no previous leaf (start of list)
		Entries:     []*IndexEntry{},
	}

	// Serialize the leaf node
	leafBlock, err := SerializeLeafNode(initialLeaf, int(blockSize), codec, int(valueSize))
	if err != nil {
		return err
	}

	// Write the initial leaf node to block 0 (offset = 0 * blockSize + IndexHeaderSize)
	offset := int64(IndexHeaderSize)
	if _, err := file.WriteAt(leafBlock, offset); err != nil {
		return err
	}

	// Initialize the first empty block (block 1)
	// This ensures NextEmptyBlock always points to a valid EmptyNode
	firstEmptyNode := &EmptyNode{
		BlockNumber:    1,
		NodeType:       NodeTypeEmpty,    // 3 for empty node
		NextEmptyBlock: NoNextEmptyBlock, // Initially, no more empty blocks in chain
	}

	// Serialize the empty node
	emptyBlock, err := SerializeEmptyNode(firstEmptyNode, int(blockSize))
	if err != nil {
		return err
	}

	// Write the empty node to block 1 (offset = 1 * blockSize + IndexHeaderSize)
	offset = int64(IndexHeaderSize) + int64(blockSize)
	if _, err := file.WriteAt(emptyBlock, offset); err != nil {
		return err
	}

	// Expand file to required size (header + blockCount * blockSize)
	fileSize := int64(IndexHeaderSize) + int64(blockCount)*int64(blockSize)
	if err := file.Truncate(fileSize); err != nil {
		return ErrFileTruncate
	}

	return nil
}

/*
GetEmptyBlock allocates a new block from the empty chain, properly managing
the reusable block list and extending the file if necessary.

Returns:
  - blockNumber: the block number that was allocated
  - error: if any (e.g. failed to read/write blocks)

Algorithm:
 1. Read the current empty block from NextEmptyBlock
 2. If chain has more blocks (emptyNode.NextEmptyBlock != NoNextEmptyBlock):
    - Return this block
    - Update header.NextEmptyBlock to point to next in chain
 3. If this is the last block in chain (emptyNode.NextEmptyBlock == NoNextEmptyBlock):
    - Return this block
    - Allocate a new empty block at position (FileLength)
    - Check if we need to extend the file
    - Initialize the new empty block
    - Update header.NextEmptyBlock to point to new empty block
    - Update header.FileLength if file was extended
*/
func GetEmptyBlock(idx *Index) (uint16, error) {
	// Read the current first empty block
	currentEmptyBlockNum := idx.Header.NextEmptyBlock

	if currentEmptyBlockNum == NoNextEmptyBlock {
		return 0, errors.New("no empty blocks available")
	}

	currentEmptyBlock, err := ReadIndexBlock(idx, int(currentEmptyBlockNum))
	if err != nil {
		return 0, err
	}

	currentEmpty, err := DeserializeEmptyNode(currentEmptyBlock)
	if err != nil {
		return 0, err
	}

	// Check if there are more blocks in the chain
	if currentEmpty.NextEmptyBlock != NoNextEmptyBlock {
		// Case 1: Chain has at least 2 blocks - just advance the pointer
		idx.Header.NextEmptyBlock = currentEmpty.NextEmptyBlock

		// Write updated header
		headerData, err := SerializeIndexHeader(idx.Header)
		if err != nil {
			return 0, err
		}
		_, err = idx.File.WriteAt(headerData, 0)
		if err != nil {
			return 0, err
		}

		return currentEmptyBlockNum, nil
	}

	// Case 2: This is the last block in chain - need to allocate a new empty block
	allocatedBlockNum := currentEmptyBlockNum

	// New empty block will be at position FileLength
	newEmptyBlockNum := idx.Header.FileLength

	// Check actual file size to see if we need to extend
	fileInfo, err := idx.File.Stat()
	if err != nil {
		return 0, err
	}
	actualFileSize := fileInfo.Size()
	requiredFileSize := int64(IndexHeaderSize) + int64(newEmptyBlockNum+1)*int64(idx.Header.BlockSize)

	if requiredFileSize > actualFileSize {
		// Extend the file beyond pre-allocated space
		err := idx.File.Truncate(requiredFileSize)
		if err != nil {
			return 0, err
		}
	}

	// Update FileLength to reflect the new block in use
	idx.Header.FileLength = newEmptyBlockNum + 1

	// Initialize the new empty block
	newEmpty := &EmptyNode{
		BlockNumber:    newEmptyBlockNum,
		NodeType:       NodeTypeEmpty,
		NextEmptyBlock: NoNextEmptyBlock, // End of chain
	}

	newEmptyBlock, err := SerializeEmptyNode(newEmpty, int(idx.Header.BlockSize))
	if err != nil {
		return 0, err
	}

	err = WriteIndexBlock(idx, int(newEmptyBlockNum), newEmptyBlock)
	if err != nil {
		return 0, err
	}

	// Update header to point to the new empty block
	idx.Header.NextEmptyBlock = newEmptyBlockNum

	// Write updated header
	headerData, err := SerializeIndexHeader(idx.Header)
	if err != nil {
		return 0, err
	}
	_, err = idx.File.WriteAt(headerData, 0)
	if err != nil {
		return 0, err
	}

	return allocatedBlockNum, nil
}
