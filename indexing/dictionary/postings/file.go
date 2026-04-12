package postings

import (
	"encoding/binary"
	"errors"
	"os"
)

/*
the file contains the following functions operating on the postings file
- CreatePostingsFile - creates a postings file with the initial header and required initial number of blocks
- OpenPostingsFile - opens an existing postings file and returns the file handle and file header
- ReadPostingsBlock - reads a postings block from the file and creates PostingBlock struct
- WritePostingsBlock - serializes and writes a PostingBlock struct to file block
- GetNewPostingBlock - gets a new postings block, either from the free list or by expanding the file
- AddFreePostingBlock - adds a postings block to the list of free blocks for reuse
*/

// NoBlock is a sentinel value indicating "no block" or "end of list"
const NoBlock = ^uint32(0) // 0xFFFFFFFF or 4294967295

// Custom error variables for postings file operations
var (
	ErrPostingsFileCreate     = errors.New("failed to create postings file")
	ErrPostingsFileOpen       = errors.New("failed to open postings file")
	ErrPostingsBlockRead      = errors.New("failed to read postings block")
	ErrPostingsBlockWrite     = errors.New("failed to write postings block")
	ErrPostingsFileTruncate   = errors.New("failed to expand postings file to required size")
	ErrIntegrityCheckFailed   = errors.New("integrity check failed: dictID mismatch")
)

/*
WritePostingsFileHeader writes the postings file to the beginning of the file.
Parameters:
- file: *os.File pointer to the postings file
- header: *PostingFileHeader pointer to the header struct to write
Returns:
- error: if any
*/
func WritePostingsFileHeader(file *os.File, header *PostingsFileHeader) error {
	buf := make([]byte, 16)
	// Serialize PostingsFileHeader as 16 bytes: BlockSize, FirstFreeBlock, NumberOfPostings, Format (little-endian)
	binary.LittleEndian.PutUint32(buf[0:4], header.BlockSize)
	binary.LittleEndian.PutUint32(buf[4:8], header.FirstFreeBlock)
	binary.LittleEndian.PutUint32(buf[8:12], header.NumberOfPostings)
	binary.LittleEndian.PutUint32(buf[12:16], uint32(header.Format))
	_, err := file.WriteAt(buf, 0)
	return err
}

/*
ReadPostingsFileHeader reads the postings file header from the file.
Parameters:
- file: *os.File pointer to the postings file
Returns:
- *PostingsFileHeader: pointer to the header struct
- error: if any
*/
func ReadPostingsFileHeader(file *os.File) (*PostingsFileHeader, error) {
	buf := make([]byte, 16)
	_, err := file.ReadAt(buf, 0)
	if err != nil {
		return nil, err
	}
	header := &PostingsFileHeader{
		BlockSize:        binary.LittleEndian.Uint32(buf[0:4]),
		FirstFreeBlock:   binary.LittleEndian.Uint32(buf[4:8]),
		NumberOfPostings: binary.LittleEndian.Uint32(buf[8:12]),
		Format:           PostingsFormat(binary.LittleEndian.Uint32(buf[12:16])),
	}
	return header, nil
}

/*
CreatePostingsFile creates a new postings file with the given path. The file will be creates with a space for
the requested number of blocks.
Parameters:
- filePath: string path to the postings file to create
- blockSize: uint32 size of each block
- initialSize: uint32 initial number of blocks to allocate
- format: PostingsFormat format to use for storing record IDs (FormatSlice or FormatBitmap)
Returns:
- *os.File: pointer to the created postings file
- error: if any

Note: block 0 starts at offset 16 in the file due to the header size.
*/
func CreatePostingsFile(filePath string, blockSize uint32, initialSize uint32, format PostingsFormat) (*os.File, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, ErrPostingsFileCreate
	}
	header := PostingsFileHeader{
		BlockSize:        blockSize,
		FirstFreeBlock:   NoBlock,
		NumberOfPostings: 0,
		Format:           format,
	}
	// allocate space for initialSize blocks in the file
	requiredSize := int64(initialSize)*int64(blockSize) + 16 // +16 for header
	err = file.Truncate(requiredSize)
	if err != nil {
		return nil, ErrPostingsFileTruncate
	}
	// create a linked list of free blocks in reverse order - block 0 is first free block
	blockHeader := PostingBlock{
		Header:    PostingBlockHeader{},
		RecordIDs: nil,
	}
	// write empty blocks with next pointers
	for i := uint32(0); i < initialSize; i++ {
		blockHeader.Header.BlockNumber = i
		if i == initialSize-1 {
			blockHeader.Header.NextBlock = NoBlock // end of free list
		} else {
			blockHeader.Header.NextBlock = i + 1
		}
		err = WritePostingsBlock(file, blockSize, &blockHeader)
		if err != nil {
			return nil, err
		}
	}
	// add the file header
	header.FirstFreeBlock = 0 // first free block is block 0
	err = WritePostingsFileHeader(file, &header)
	if err != nil {
		return nil, err
	}
	return file, nil
}

/*
OpenPostingsFile opens an existing postings file.
Parameters:
- filePath: string path to the postings file to open
Returns:
- *os.File: pointer to the opened postings file
- *PostingsFileHeader: pointer to the postings file header
- error: if any
*/
func OpenPostingsFile(filePath string) (*os.File, *PostingsFileHeader, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, ErrPostingsFileOpen
	}
	headerBuf := make([]byte, 16)
	_, err = file.ReadAt(headerBuf, 0)
	if err != nil {
		return nil, nil, err
	}
	header := &PostingsFileHeader{}
	header.BlockSize = binary.LittleEndian.Uint32(headerBuf[0:4])
	header.FirstFreeBlock = binary.LittleEndian.Uint32(headerBuf[4:8])
	header.NumberOfPostings = binary.LittleEndian.Uint32(headerBuf[8:12])
	header.Format = PostingsFormat(binary.LittleEndian.Uint32(headerBuf[12:16]))
	return file, header, nil
}

/*
ReadPostingsBlock reads a postings block from the postings file.
Parameters:
- file: *os.File pointer to the postings file
- blockSize: uint32 size of each postings block == PostingsFileHeader.BlockSize
- blockNumber: uint32 block number to read
Returns:
- *PostingBlock: pointer to the read postings block
- error: if any
*/
func ReadPostingsBlock(file *os.File, blockSize uint32, blockNumber uint32) (*PostingBlock, error) {
	offset := int64(blockNumber)*int64(blockSize) + 16 // +16 for header
	buf := make([]byte, blockSize)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, ErrPostingsBlockRead
	}
	block, err := DeserializePostingsBlock(buf, blockSize)
	if err != nil {
		return nil, err
	}
	return block, nil
}

/*
WritePostingsBlock writes a postings block to the postings file.
Parameters:
- file: *os.File pointer to the postings file
- blockSize: uint32 size of each postings block == PostingsFileHeader.BlockSize
- block: *PostingBlock pointer to the postings block to write
Returns:
- error: if any
*/
func WritePostingsBlock(file *os.File, blockSize uint32, block *PostingBlock) error {
	offset := int64(block.Header.BlockNumber)*int64(blockSize) + 16 // +16 for header
	buf, err := SerializePostingsBlock(block, blockSize)
	if err != nil {
		return err
	}
	_, err = file.WriteAt(buf, offset)
	if err != nil {
		return ErrPostingsBlockWrite
	}
	return nil
}

/*
GetNewPostingBlock gets a new postings block, either from the list of free blocks, or by expanding the file.
Parameters:
- file: *os.File pointer to the postings file
- header: *PostingsFileHeader pointer to the postings file header
Returns:
- BlockNumber: number of the new/free postings block
- error: if any
*/
func GetNewPostingBlock(file *os.File, header *PostingsFileHeader) (uint32, error) {
	if header.FirstFreeBlock != NoBlock {
		// there is a free block available - remove it from the free list
		freeBlockNumber := header.FirstFreeBlock
		// read the free block to get the next free block
		freeBlock, err := ReadPostingsBlock(file, header.BlockSize, freeBlockNumber)
		if err != nil {
			return NoBlock, err
		}
		// update header to point to the next free block, or NoBlock if none left
		header.FirstFreeBlock = freeBlock.Header.NextBlock
		err = WritePostingsFileHeader(file, header)
		if err != nil {
			return NoBlock, err
		}

		return freeBlockNumber, nil
	}
	// no free blocks - add new block at the end of the file
	currentSize, err := file.Stat()
	if err != nil {
		return NoBlock, err
	}

	// calculate the new block number (number of blocks currently in file)
	numBlocks := (currentSize.Size() - 12) / int64(header.BlockSize)
	newBlockNumber := uint32(numBlocks)

	// expand file to accommodate new block
	newSize := currentSize.Size() + int64(header.BlockSize)
	err = file.Truncate(newSize)
	if err != nil {
		return NoBlock, ErrPostingsFileTruncate
	}

	return newBlockNumber, nil
}

/*
AddFreePostingBlock adds a postings block to the list of free blocks for reuse.
Parameters:
- file: *os.File pointer to the postings file
- header: *PostingsFileHeader pointer to the postings file header
- blockNumber: uint32 block number to add to the free list
Returns:
- error: if any
*/
func AddFreePostingBlock(file *os.File, header *PostingsFileHeader, blockNumber uint32) error {
	// sind the end of the free list
	if header.FirstFreeBlock == NoBlock {
		// free list is empty - add this block as the first free block
		block := &PostingBlock{
			Header: PostingBlockHeader{
				BlockNumber: blockNumber,
				NextBlock:   NoBlock,
			},
			RecordIDs: nil,
		}
		err := WritePostingsBlock(file, header.BlockSize, block)
		if err != nil {
			return err
		}
		header.FirstFreeBlock = blockNumber
		return WritePostingsFileHeader(file, header)
	}
	// free list is not empty - add this block at the start of the free list
	block := &PostingBlock{
		Header: PostingBlockHeader{
			BlockNumber: blockNumber,
			NextBlock:   header.FirstFreeBlock,
		},
		RecordIDs: nil,
	}
	err := WritePostingsBlock(file, header.BlockSize, block)
	if err != nil {
		return err
	}
	header.FirstFreeBlock = blockNumber
	return WritePostingsFileHeader(file, header)
}

/*
DeletePostingsList deletes an entire postings list starting at the given block number by
adding all blocks in the chain to the free list.
Parameters:
- file: *os.File pointer to the postings file
- header: *PostingsFileHeader pointer to the postings file header
- startBlockNumber: uint32 block number where the postings list starts
Returns:
- error: if any

Note: If startBlockNumber is NoBlock, the function returns without error (empty list case).
*/
func DeletePostingsList(file *os.File, header *PostingsFileHeader, startBlockNumber uint32) error {
	// Handle empty list case
	if startBlockNumber == NoBlock {
		return nil
	}

	// Traverse the linked list and collect all block numbers
	currentBlockNumber := startBlockNumber
	for currentBlockNumber != NoBlock {
		// Read the current block to get the next block pointer
		block, err := ReadPostingsBlock(file, header.BlockSize, currentBlockNumber)
		if err != nil {
			return err
		}

		// Save the next block number before we free the current one
		nextBlockNumber := block.Header.NextBlock

		// Add current block to free list
		err = AddFreePostingBlock(file, header, currentBlockNumber)
		if err != nil {
			return err
		}

		// Move to next block
		currentBlockNumber = nextBlockNumber
	}

	return nil
}
