package postings

import (
	"os"
)

/*
PostingsList is an interface for different postings list implementations.
It supports both slice-based (list of uint32 record IDs) and bitmap-based representations.
*/
type PostingsList interface {
	// DeserializeBlock deserializes a byte slice into a PostingBlock struct
	DeserializeBlock(data []byte) (*PostingBlock, error)

	// SerializeBlock serializes a PostingBlock struct into a byte slice
	SerializeBlock(block *PostingBlock) ([]byte, error)

	// GetRecordsList retrieves all record IDs for a given dictionary ID by traversing the postings blocks
	GetRecordsList(file *os.File, blockNumber uint32) ([]uint32, []uint32, error)

	// WriteBackRecordsList writes a full list of record IDs for a given dictionary ID into postings blocks
	WriteBackRecordsList(file *os.File, fileHeader *PostingsFileHeader, blockNumbers []uint32, recordIDs []uint32, dictID uint32) error

	// AddNewList adds a new postings list for a given dictionary ID and returns the starting block number
	AddNewList(file *os.File, fileHeader *PostingsFileHeader, recordIDs []uint32, dictID uint32) (uint32, error)

	// AddRecordID adds a single recordID to an existing postings list (idempotent - no error if already exists)
	AddRecordID(file *os.File, fileHeader *PostingsFileHeader, blockNumber uint32, recordID uint32, dictID uint32) error

	// RemoveRecordID removes a single recordID from an existing postings list (idempotent - no error if not found)
	RemoveRecordID(file *os.File, fileHeader *PostingsFileHeader, blockNumber uint32, recordID uint32, dictID uint32) error
}

/*
NewPostingsList creates a PostingsList implementation based on the format.
Parameters:
- format: PostingsFormat indicating which implementation to use (FormatSlice or FormatBitmap)
- blockSize: uint32 size of each postings block
Returns:
- PostingsList: implementation based on the format
*/
func NewPostingsList(format PostingsFormat, blockSize uint32) PostingsList {
	switch format {
	case FormatSlice:
		return NewSlicePostingsList(blockSize)
	case FormatBitmap:
		return NewBitmapPostingsList(blockSize)
	default:
		// Default to slice-based
		return NewSlicePostingsList(blockSize)
	}
}

/*
DeserializePostingsBlock deserializes a byte slice into a PostingBlock struct.
This is a low-level utility function used by ReadPostingsBlock and tests.
For high-level operations, use PostingsList.GetRecordsList instead.
Parameters:
- data: byte slice containing the serialized postings block data
- blockSize: uint32 size of the postings block in bytes
Returns:
- *PostingBlock: pointer to the deserialized PostingBlock struct
- error: if any
*/
func DeserializePostingsBlock(data []byte, blockSize uint32) (*PostingBlock, error) {
	list := NewSlicePostingsList(blockSize)
	return list.DeserializeBlock(data)
}

/*
SerializePostingsBlock serializes a PostingBlock struct into a byte slice.
This is a low-level utility function used by WritePostingsBlock and tests.
For high-level operations, use PostingsList.WriteBackRecordsList instead.
Parameters:
- block: pointer to PostingBlock struct to serialize
- blockSize: uint32 size of the postings block in bytes
Returns:
- []byte: serialized byte slice of the postings block
- error: if any
*/
func SerializePostingsBlock(block *PostingBlock, blockSize uint32) ([]byte, error) {
	list := NewSlicePostingsList(blockSize)
	return list.SerializeBlock(block)
}
