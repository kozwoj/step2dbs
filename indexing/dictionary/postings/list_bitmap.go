package postings

import (
	"encoding/binary"
	"os"

	"github.com/RoaringBitmap/roaring"
)

// BitmapPostingsList implements PostingsList using RoaringBitmap for compressed bitmap storage.
type BitmapPostingsList struct {
	blockSize uint32
}

// NewBitmapPostingsList creates a new bitmap-based postings list handler.
func NewBitmapPostingsList(blockSize uint32) *BitmapPostingsList {
	return &BitmapPostingsList{
		blockSize: blockSize,
	}
}

// DeserializeBlock deserializes a byte slice into a PostingBlock struct by reading the bitmap data.
func (b *BitmapPostingsList) DeserializeBlock(data []byte) (*PostingBlock, error) {
	if len(data) < int(b.blockSize) {
		return nil, ErrPostingsBlockRead
	}
	header := PostingBlockHeader{}
	header.BlockNumber = binary.LittleEndian.Uint32(data[0:4])
	header.DictID = binary.LittleEndian.Uint32(data[4:8])
	header.NextBlock = binary.LittleEndian.Uint32(data[8:12])
	header.Count = binary.LittleEndian.Uint32(data[12:16])

	postingBlock := PostingBlock{
		Header:    header,
		RecordIDs: make([]uint32, 0), // For bitmap, RecordIDs is not used in block storage
	}

	return &postingBlock, nil
}

// SerializeBlock serializes a PostingBlock struct into a byte slice.
// For bitmap format, the bitmap data is stored in the payload, not as individual record IDs.
func (b *BitmapPostingsList) SerializeBlock(block *PostingBlock) ([]byte, error) {
	buf := make([]byte, b.blockSize)
	binary.LittleEndian.PutUint32(buf[0:4], block.Header.BlockNumber)
	binary.LittleEndian.PutUint32(buf[4:8], block.Header.DictID)
	binary.LittleEndian.PutUint32(buf[8:12], block.Header.NextBlock)
	binary.LittleEndian.PutUint32(buf[12:16], block.Header.Count)

	// The bitmap payload is already in the buffer from WriteBackRecordsList
	// This function is mainly for the header
	return buf, nil
}

// GetRecordsList retrieves all record IDs for a given dictionary ID by reading and deserializing the bitmap.
func (b *BitmapPostingsList) GetRecordsList(file *os.File, blockNumber uint32) ([]uint32, []uint32, error) {
	var bitmapBytes []byte
	var blockNumbers []uint32
	currentBlockNumber := blockNumber

	// Read all blocks to reconstruct the complete bitmap
	for {
		blockData := make([]byte, b.blockSize)
		offset := int64(16 + currentBlockNumber*b.blockSize)
		_, err := file.ReadAt(blockData, offset)
		if err != nil {
			return nil, nil, err
		}

		// Parse header
		header := PostingBlockHeader{}
		header.BlockNumber = binary.LittleEndian.Uint32(blockData[0:4])
		header.DictID = binary.LittleEndian.Uint32(blockData[4:8])
		header.NextBlock = binary.LittleEndian.Uint32(blockData[8:12])
		header.Count = binary.LittleEndian.Uint32(blockData[12:16])

		blockNumbers = append(blockNumbers, currentBlockNumber)

		// Append payload (bitmap bytes)
		payloadSize := b.blockSize - 16
		bitmapBytes = append(bitmapBytes, blockData[16:16+payloadSize]...)

		if header.NextBlock == NoBlock {
			break
		}
		currentBlockNumber = header.NextBlock
	}

	// Deserialize the roaring bitmap
	bitmap := roaring.New()
	err := bitmap.UnmarshalBinary(bitmapBytes)
	if err != nil {
		return nil, nil, err
	}

	// Convert bitmap to sorted array of record IDs
	recordIDs := bitmap.ToArray()
	return recordIDs, blockNumbers, nil
}

// WriteBackRecordsList writes a full list of record IDs as a compressed roaring bitmap into postings blocks.
func (b *BitmapPostingsList) WriteBackRecordsList(file *os.File, fileHeader *PostingsFileHeader, blockNumbers []uint32, recordIDs []uint32, dictID uint32) error {
	// Create roaring bitmap from record IDs
	bitmap := roaring.New()
	for _, recordID := range recordIDs {
		bitmap.Add(recordID)
	}

	// Serialize bitmap
	bitmapBytes, err := bitmap.MarshalBinary()
	if err != nil {
		return err
	}

	// Calculate blocks needed
	payloadPerBlock := b.blockSize - 16
	totalBytes := uint32(len(bitmapBytes))
	neededBlocks := int((totalBytes + payloadPerBlock - 1) / payloadPerBlock)
	if neededBlocks == 0 {
		neededBlocks = 1
	}

	currentBlockCount := len(blockNumbers)

	// Handle block allocation/deallocation
	if neededBlocks > currentBlockCount {
		// Need more blocks
		for i := 0; i < neededBlocks-currentBlockCount; i++ {
			newBlockNumber, err := GetNewPostingBlock(file, fileHeader)
			if err != nil {
				return err
			}
			blockNumbers = append(blockNumbers, newBlockNumber)
		}
	} else if neededBlocks < currentBlockCount {
		// Free excess blocks
		for i := neededBlocks; i < currentBlockCount; i++ {
			err := AddFreePostingBlock(file, fileHeader, blockNumbers[i])
			if err != nil {
				return err
			}
		}
		blockNumbers = blockNumbers[:neededBlocks]
	}

	// Write bitmap bytes across blocks
	bytesWritten := uint32(0)
	for i, blockNumber := range blockNumbers {
		buf := make([]byte, b.blockSize)

		// Write header
		binary.LittleEndian.PutUint32(buf[0:4], blockNumber)
		binary.LittleEndian.PutUint32(buf[4:8], dictID)

		// Set NextBlock pointer
		if i < len(blockNumbers)-1 {
			binary.LittleEndian.PutUint32(buf[8:12], blockNumbers[i+1])
		} else {
			binary.LittleEndian.PutUint32(buf[8:12], NoBlock)
		}

		// Store total byte count in Count field for last block
		binary.LittleEndian.PutUint32(buf[12:16], totalBytes)

		// Copy bitmap payload
		startIdx := bytesWritten
		endIdx := bytesWritten + payloadPerBlock
		if endIdx > totalBytes {
			endIdx = totalBytes
		}
		copy(buf[16:], bitmapBytes[startIdx:endIdx])
		bytesWritten = endIdx

		// Write block to file
		offset := int64(16 + blockNumber*b.blockSize)
		_, err := file.WriteAt(buf, offset)
		if err != nil {
			return err
		}
	}

	// Update file header
	return WritePostingsFileHeader(file, fileHeader)
}

// AddNewList adds a new postings list for a given dictionary ID and returns the starting block number.
func (b *BitmapPostingsList) AddNewList(file *os.File, fileHeader *PostingsFileHeader, recordIDs []uint32, dictID uint32) (uint32, error) {
	// Create roaring bitmap from record IDs
	bitmap := roaring.New()
	for _, recordID := range recordIDs {
		bitmap.Add(recordID)
	}

	// Serialize bitmap
	bitmapBytes, err := bitmap.MarshalBinary()
	if err != nil {
		return NoBlock, err
	}

	// Calculate blocks needed
	payloadPerBlock := b.blockSize - 16
	totalBytes := uint32(len(bitmapBytes))
	neededBlocks := int((totalBytes + payloadPerBlock - 1) / payloadPerBlock)
	if neededBlocks == 0 {
		neededBlocks = 1
	}

	// Allocate new blocks
	var blockNumbers []uint32
	for i := 0; i < neededBlocks; i++ {
		newBlockNumber, err := GetNewPostingBlock(file, fileHeader)
		if err != nil {
			return NoBlock, err
		}
		blockNumbers = append(blockNumbers, newBlockNumber)
	}

	// Write the bitmap bytes across the blocks
	err = b.WriteBackRecordsList(file, fileHeader, blockNumbers, recordIDs, dictID)
	if err != nil {
		return NoBlock, err
	}

	return blockNumbers[0], nil
}

// AddRecordID adds a single recordID to an existing postings list.
// This operation is idempotent - if the recordID already exists, it returns nil without error.
func (b *BitmapPostingsList) AddRecordID(file *os.File, fileHeader *PostingsFileHeader, blockNumber uint32, recordID uint32, dictID uint32) error {
	// Integrity check: verify the first block has the correct dictID
	blockData := make([]byte, b.blockSize)
	offset := int64(16 + blockNumber*b.blockSize)
	_, err := file.ReadAt(blockData, offset)
	if err != nil {
		return err
	}
	firstBlockDictID := binary.LittleEndian.Uint32(blockData[4:8])
	if firstBlockDictID != dictID {
		return ErrIntegrityCheckFailed
	}

	// Get current record IDs and block numbers
	recordIDs, blockNumbers, err := b.GetRecordsList(file, blockNumber)
	if err != nil {
		return err
	}

	// Check if recordID already exists (idempotent check)
	for _, existingID := range recordIDs {
		if existingID == recordID {
			// Record ID already exists - idempotent behavior, return success
			return nil
		}
	}

	// Add the new record ID
	recordIDs = append(recordIDs, recordID)

	// Write back the updated list
	return b.WriteBackRecordsList(file, fileHeader, blockNumbers, recordIDs, dictID)
}

// RemoveRecordID removes a single recordID from an existing postings list.
// This operation is idempotent - if the recordID doesn't exist, it returns nil without error.
func (b *BitmapPostingsList) RemoveRecordID(file *os.File, fileHeader *PostingsFileHeader, blockNumber uint32, recordID uint32, dictID uint32) error {
	// Integrity check: verify the first block has the correct dictID
	blockData := make([]byte, b.blockSize)
	offset := int64(16 + blockNumber*b.blockSize)
	_, err := file.ReadAt(blockData, offset)
	if err != nil {
		return err
	}
	firstBlockDictID := binary.LittleEndian.Uint32(blockData[4:8])
	if firstBlockDictID != dictID {
		return ErrIntegrityCheckFailed
	}

	// Get current record IDs and block numbers
	recordIDs, blockNumbers, err := b.GetRecordsList(file, blockNumber)
	if err != nil {
		return err
	}

	// Find and remove the record ID
	found := false
	removePos := -1
	for i, existingID := range recordIDs {
		if existingID == recordID {
			found = true
			removePos = i
			break
		}
	}

	// If not found, return success (idempotent behavior)
	if !found {
		return nil
	}

	// Remove the record ID
	recordIDs = append(recordIDs[:removePos], recordIDs[removePos+1:]...)

	// Write back the updated list
	return b.WriteBackRecordsList(file, fileHeader, blockNumbers, recordIDs, dictID)
}
