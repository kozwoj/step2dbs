package postings

import (
	"encoding/binary"
	"os"
)

// SlicePostingsList implements PostingsList using slices of uint32 record IDs.
type SlicePostingsList struct {
	blockSize uint32
}

// NewSlicePostingsList creates a new slice-based postings list handler.
func NewSlicePostingsList(blockSize uint32) *SlicePostingsList {
	return &SlicePostingsList{
		blockSize: blockSize,
	}
}

// DeserializeBlock deserializes a byte slice into a PostingBlock struct.
func (s *SlicePostingsList) DeserializeBlock(data []byte) (*PostingBlock, error) {
	if len(data) < int(s.blockSize) {
		return nil, ErrPostingsBlockRead
	}
	header := PostingBlockHeader{}
	header.BlockNumber = binary.LittleEndian.Uint32(data[0:4])
	header.DictID = binary.LittleEndian.Uint32(data[4:8])
	header.NextBlock = binary.LittleEndian.Uint32(data[8:12])
	header.Count = binary.LittleEndian.Uint32(data[12:16])

	postingBlock := PostingBlock{
		Header:    header,
		RecordIDs: make([]uint32, header.Count),
	}

	offset := 16
	for i := uint32(0); i < header.Count; i++ {
		if offset+4 > len(data) {
			return nil, ErrPostingsBlockRead
		}
		postingBlock.RecordIDs[i] = binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4
	}
	return &postingBlock, nil
}

// SerializeBlock serializes a PostingBlock struct into a byte slice.
func (s *SlicePostingsList) SerializeBlock(block *PostingBlock) ([]byte, error) {
	buf := make([]byte, s.blockSize)
	binary.LittleEndian.PutUint32(buf[0:4], block.Header.BlockNumber)
	binary.LittleEndian.PutUint32(buf[4:8], block.Header.DictID)
	binary.LittleEndian.PutUint32(buf[8:12], block.Header.NextBlock)
	binary.LittleEndian.PutUint32(buf[12:16], block.Header.Count)
	offset := 16
	for i, recordID := range block.RecordIDs {
		if offset+4 > len(buf) {
			return nil, ErrPostingsBlockWrite
		}
		binary.LittleEndian.PutUint32(buf[offset:offset+4], recordID)
		offset += 4
		if i+1 >= int(block.Header.Count) {
			break
		}
	}
	return buf, nil
}

// GetRecordsList retrieves all record IDs for a given dictionary ID by traversing the postings blocks.
func (s *SlicePostingsList) GetRecordsList(file *os.File, blockNumber uint32) ([]uint32, []uint32, error) {
	var allRecordIDs []uint32
	var blockNumbers []uint32
	currentBlockNumber := blockNumber
	// loop until there is no more blocks in the list
	for {
		block, err := ReadPostingsBlock(file, s.blockSize, currentBlockNumber)
		if err != nil {
			return nil, nil, err
		}
		blockNumbers = append(blockNumbers, currentBlockNumber)
		allRecordIDs = append(allRecordIDs, block.RecordIDs...)
		if block.Header.NextBlock == NoBlock {
			break
		}
		currentBlockNumber = block.Header.NextBlock
	}
	return allRecordIDs, blockNumbers, nil
}

// WriteBackRecordsList writes a full list of record IDs for a given dictionary ID into postings blocks.
func (s *SlicePostingsList) WriteBackRecordsList(file *os.File, fileHeader *PostingsFileHeader, blockNumbers []uint32, recordIDs []uint32, dictID uint32) error {
	recordsPerBlock := (s.blockSize - 16) / 4 // 16 bytes header, 4 bytes per record ID
	totalRecords := uint32(len(recordIDs))
	var neededBlocks int
	if totalRecords == 0 {
		neededBlocks = 1
	} else {
		neededBlocks = int(totalRecords / recordsPerBlock)
		if totalRecords%recordsPerBlock != 0 {
			neededBlocks++
		}
	}
	currentBlockCount := len(blockNumbers)
	// Handle different cases of block count
	if neededBlocks > currentBlockCount {
		// Need more blocks - allocate new ones
		for i := 0; i < neededBlocks-currentBlockCount; i++ {
			newBlockNumber, err := GetNewPostingBlock(file, fileHeader)
			if err != nil {
				return err
			}
			blockNumbers = append(blockNumbers, newBlockNumber)
		}
	} else if neededBlocks < currentBlockCount {
		// Need fewer blocks - free the extra blocks
		for i := neededBlocks; i < currentBlockCount; i++ {
			err := AddFreePostingBlock(file, fileHeader, blockNumbers[i])
			if err != nil {
				return err
			}
		}
		blockNumbers = blockNumbers[:neededBlocks]
	}
	// Write record IDs across the blocks and link them
	postingBlock := &PostingBlock{}
	for i, blockNumber := range blockNumbers {
		startIdx := i * int(recordsPerBlock)
		endIdx := startIdx + int(recordsPerBlock)
		if endIdx > len(recordIDs) {
			endIdx = len(recordIDs)
		}
		recordsToWrite := recordIDs[startIdx:endIdx]
		// create posting block structure
		postingBlock.Header.Count = uint32(len(recordsToWrite))
		postingBlock.Header.BlockNumber = blockNumber
		postingBlock.Header.DictID = dictID
		postingBlock.RecordIDs = recordsToWrite
		// set the NextBlock pointer to the next block or NoBlock if i == len(blockNumbers)-1
		if i < len(blockNumbers)-1 {
			postingBlock.Header.NextBlock = blockNumbers[i+1]
		} else {
			postingBlock.Header.NextBlock = NoBlock
		}
		err := WritePostingsBlock(file, s.blockSize, postingBlock)
		if err != nil {
			return err
		}
	}
	return nil
}

// AddNewList adds a new postings list for a given dictionary ID and returns the starting block number.
func (s *SlicePostingsList) AddNewList(file *os.File, fileHeader *PostingsFileHeader, recordIDs []uint32, dictID uint32) (uint32, error) {
	recordsPerBlock := (s.blockSize - 16) / 4 // 16 bytes header, 4 bytes per record ID
	totalRecords := uint32(len(recordIDs))
	var neededBlocks int
	if totalRecords == 0 {
		neededBlocks = 1
	} else {
		neededBlocks = int(totalRecords / recordsPerBlock)
		if totalRecords%recordsPerBlock != 0 {
			neededBlocks++
		}
	}
	// get required number of new blocks
	var blockNumbers []uint32
	for i := 0; i < neededBlocks; i++ {
		newBlockNumber, err := GetNewPostingBlock(file, fileHeader)
		if err != nil {
			return NoBlock, err
		}
		blockNumbers = append(blockNumbers, newBlockNumber)
	}
	// write the record IDs across the blocks
	err := s.WriteBackRecordsList(file, fileHeader, blockNumbers, recordIDs, dictID)
	if err != nil {
		return NoBlock, err
	}
	return blockNumbers[0], nil
}

// AddRecordID adds a single recordID to an existing postings list.
// This operation is idempotent - if the recordID already exists, it returns nil without error.
// The recordIDs are maintained in sorted order.
func (s *SlicePostingsList) AddRecordID(file *os.File, fileHeader *PostingsFileHeader, blockNumber uint32, recordID uint32, dictID uint32) error {
	// Integrity check: verify the first block has the correct dictID
	firstBlock, err := ReadPostingsBlock(file, s.blockSize, blockNumber)
	if err != nil {
		return err
	}
	if firstBlock.Header.DictID != dictID {
		return ErrIntegrityCheckFailed
	}

	// Get current record IDs and block numbers
	recordIDs, blockNumbers, err := s.GetRecordsList(file, blockNumber)
	if err != nil {
		return err
	}

	// Check if recordID already exists and find insertion position
	insertPos := len(recordIDs)
	for i, existingID := range recordIDs {
		if existingID == recordID {
			// Record ID already exists - idempotent behavior, return success
			return nil
		}
		if existingID > recordID {
			// Found insertion position to maintain sorted order
			insertPos = i
			break
		}
	}

	// Insert new record ID in sorted position
	recordIDs = append(recordIDs, 0)
	copy(recordIDs[insertPos+1:], recordIDs[insertPos:])
	recordIDs[insertPos] = recordID

	// Write back the updated list
	return s.WriteBackRecordsList(file, fileHeader, blockNumbers, recordIDs, dictID)
}

// RemoveRecordID removes a single recordID from an existing postings list.
// This operation is idempotent - if the recordID doesn't exist, it returns nil without error.
func (s *SlicePostingsList) RemoveRecordID(file *os.File, fileHeader *PostingsFileHeader, blockNumber uint32, recordID uint32, dictID uint32) error {
	// Integrity check: verify the first block has the correct dictID
	firstBlock, err := ReadPostingsBlock(file, s.blockSize, blockNumber)
	if err != nil {
		return err
	}
	if firstBlock.Header.DictID != dictID {
		return ErrIntegrityCheckFailed
	}

	// Get current record IDs and block numbers
	recordIDs, blockNumbers, err := s.GetRecordsList(file, blockNumber)
	if err != nil {
		return err
	}

	// Find the record ID to remove
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
	return s.WriteBackRecordsList(file, fileHeader, blockNumbers, recordIDs, dictID)
}
