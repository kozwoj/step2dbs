package dicindex128

import (
	"errors"
	"os"
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
)

// Index represents an open dictionary index file with 128-bit hashes.
type Index struct {
	Header *IndexHeader
	File   *os.File
}

/*
IndexHeader represents the metadata stored in block 0 of the index file.
Fields:
- BlockSize       uint16 // block size in bytes (usually 512)
- FileLength      uint16 // total length of file in blocks
- RootNode        uint16 // block number of root node
- FirstLeaf       uint16 // block number of first leaf node
- NextEmptyBlock  uint16 // next empty/reusable block number

The size of the IndexHeader struct is 10 bytes, and is written at the start of the file. Hence
block N starts at offset (N * BlockSize + 10) in the file.

We are assuming that dictionaries will only grow, so indexes will also only grow. Hence the next empty
block is simply the next block after the current last block in the file. We could implement block reuse in the future
by maintaining a linked list of deleted blocks starting at NextEmptyBlock.
*/
type IndexHeader struct {
	BlockSize      uint16
	FileLength     uint16 // in blocks
	RootNode       uint16
	FirstLeaf      uint16
	NextEmptyBlock uint16 // next empty/reusable block number
}

const IndexHeaderSize = 10

/*
SerializeIndexHeader serializes the IndexHeader struct into the first 10 bytes of block 0.
Parameters:
- header: pointer to IndexHeader struct
Returns:
- []byte: serialized 10-byte slice with header data
- error: if any
*/
func SerializeIndexHeader(header *IndexHeader) ([]byte, error) {
	// Serialize IndexHeader as 10 bytes: BlockSize, FileLength, RootNode, FirstLeaf, NextEmptyBlock
	buf := make([]byte, IndexHeaderSize)
	buf[0] = byte(header.BlockSize)
	buf[1] = byte(header.BlockSize >> 8)
	buf[2] = byte(header.FileLength)
	buf[3] = byte(header.FileLength >> 8)
	buf[4] = byte(header.RootNode)
	buf[5] = byte(header.RootNode >> 8)
	buf[6] = byte(header.FirstLeaf)
	buf[7] = byte(header.FirstLeaf >> 8)
	buf[8] = byte(header.NextEmptyBlock)
	buf[9] = byte(header.NextEmptyBlock >> 8)
	return buf, nil
}

/*
DeserializeIndexHeader deserializes the first 10 bytes of block 0 into an IndexHeader struct.
Parameters:
- data: byte slice of length at least 10 bytes (the block data)
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
	header.RootNode = uint16(data[4]) | uint16(data[5])<<8
	header.FirstLeaf = uint16(data[6]) | uint16(data[7])<<8
	header.NextEmptyBlock = uint16(data[8]) | uint16(data[9])<<8
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
	offset := blockNumber*blockSize + IndexHeaderSize // +10 for file header size
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
	offset := blockNumber*blockSize + IndexHeaderSize // +10 for file header size
	data := make([]byte, blockSize)
	_, err := index.File.ReadAt(data, int64(offset))
	if err != nil {
		return nil, err
	}
	return data, nil
}
