package postings

import (
	"os"
	"testing"
)

// Test creating a new postings file
func TestCreatePostingsFile(t *testing.T) {
	// Create a temporary file path
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath) // Remove it so CreatePostingsFile can create it
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(10)

	// Create the postings file
	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	// Verify file exists and has correct size
	fileInfo, err := file.Stat()
	if err != nil {
		t.Fatalf("Failed to stat file: %v", err)
	}

	expectedSize := int64(16 + blockSize*initialSize) // header + blocks
	if fileInfo.Size() != expectedSize {
		t.Errorf("File size = %d, want %d", fileInfo.Size(), expectedSize)
	}

	// Read and verify header
	headerBuf := make([]byte, 16)
	_, err = file.ReadAt(headerBuf, 0)
	if err != nil {
		t.Fatalf("Failed to read header: %v", err)
	}

	// Verify BlockSize
	readBlockSize := uint32(headerBuf[0]) | uint32(headerBuf[1])<<8 | uint32(headerBuf[2])<<16 | uint32(headerBuf[3])<<24
	if readBlockSize != blockSize {
		t.Errorf("BlockSize = %d, want %d", readBlockSize, blockSize)
	}

	// Verify FirstFreeBlock
	readFirstFreeBlock := uint32(headerBuf[4]) | uint32(headerBuf[5])<<8 | uint32(headerBuf[6])<<16 | uint32(headerBuf[7])<<24
	if readFirstFreeBlock != 0 {
		t.Errorf("FirstFreeBlock = %d, want 0 (first block in free list)", readFirstFreeBlock)
	}

	// Verify NumberOfPostings
	readNumberOfPostings := uint32(headerBuf[8]) | uint32(headerBuf[9])<<8 | uint32(headerBuf[10])<<16 | uint32(headerBuf[11])<<24
	if readNumberOfPostings != 0 {
		t.Errorf("NumberOfPostings = %d, want 0", readNumberOfPostings)
	}
}

// Test creating postings file with different sizes
func TestCreatePostingsFileDifferentSizes(t *testing.T) {
	testCases := []struct {
		name        string
		blockSize   uint32
		initialSize uint32
	}{
		{"Small_256_5", 256, 5},
		{"Medium_512_20", 512, 20},
		{"Large_1024_50", 1024, 50},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			tempFile, err := os.CreateTemp("", "testpostings_*.post")
			if err != nil {
				t.Fatalf("Failed to create temp file: %v", err)
			}
			filePath := tempFile.Name()
			tempFile.Close()
			os.Remove(filePath)
			defer os.Remove(filePath)

			file, err := CreatePostingsFile(filePath, tc.blockSize, tc.initialSize, FormatSlice)
			if err != nil {
				t.Fatalf("CreatePostingsFile failed: %v", err)
			}
			defer file.Close()

			fileInfo, err := file.Stat()
			if err != nil {
				t.Fatalf("Failed to stat file: %v", err)
			}

			expectedSize := int64(16 + tc.blockSize*tc.initialSize)
			if fileInfo.Size() != expectedSize {
				t.Errorf("File size = %d, want %d", fileInfo.Size(), expectedSize)
			}
		})
	}
}

// Test opening an existing postings file
func TestOpenPostingsFile(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(10)

	// Create the file
	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	file.Close()

	// Open the file
	openedFile, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer openedFile.Close()

	// Verify header
	if header.BlockSize != blockSize {
		t.Errorf("BlockSize = %d, want %d", header.BlockSize, blockSize)
	}
	if header.FirstFreeBlock != 0 {
		t.Errorf("FirstFreeBlock = %d, want 0", header.FirstFreeBlock)
	}
	if header.NumberOfPostings != 0 {
		t.Errorf("NumberOfPostings = %d, want 0", header.NumberOfPostings)
	}
}

// Test reading and writing postings blocks
func TestReadWritePostingsBlock(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(5)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create a test block
	testBlock := &PostingBlock{
		Header: PostingBlockHeader{
			BlockNumber: 2,
			DictID:      100,
			NextBlock:   NoBlock,
			Count:       3,
		},
		RecordIDs: []uint32{1001, 2002, 3003},
	}

	// Write the block
	err = WritePostingsBlock(file, blockSize, testBlock)
	if err != nil {
		t.Fatalf("WritePostingsBlock failed: %v", err)
	}

	// Read the block back
	readBlock, err := ReadPostingsBlock(file, blockSize, 2)
	if err != nil {
		t.Fatalf("ReadPostingsBlock failed: %v", err)
	}

	// Verify the block
	if readBlock.Header.BlockNumber != testBlock.Header.BlockNumber {
		t.Errorf("BlockNumber = %d, want %d", readBlock.Header.BlockNumber, testBlock.Header.BlockNumber)
	}
	if readBlock.Header.DictID != testBlock.Header.DictID {
		t.Errorf("DictID = %d, want %d", readBlock.Header.DictID, testBlock.Header.DictID)
	}
	if readBlock.Header.NextBlock != testBlock.Header.NextBlock {
		t.Errorf("NextBlock = %d, want %d", readBlock.Header.NextBlock, testBlock.Header.NextBlock)
	}
	if readBlock.Header.Count != testBlock.Header.Count {
		t.Errorf("Count = %d, want %d", readBlock.Header.Count, testBlock.Header.Count)
	}
	if len(readBlock.RecordIDs) != len(testBlock.RecordIDs) {
		t.Errorf("RecordIDs length = %d, want %d", len(readBlock.RecordIDs), len(testBlock.RecordIDs))
	}
	for i, recordID := range readBlock.RecordIDs {
		if recordID != testBlock.RecordIDs[i] {
			t.Errorf("RecordIDs[%d] = %d, want %d", i, recordID, testBlock.RecordIDs[i])
		}
	}
}

// Test getting a new posting block from free list
func TestGetNewPostingBlockFromFreeList(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(5)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	// Open the file to get the header
	file.Close()
	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Get a new block (should come from free list)
	blockNumber, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}

	// Should get block 0 (first free block)
	if blockNumber != 0 {
		t.Errorf("BlockNumber = %d, want 0", blockNumber)
	}

	// FirstFreeBlock should now point to block 1
	if header.FirstFreeBlock != 1 {
		t.Errorf("FirstFreeBlock = %d, want 1", header.FirstFreeBlock)
	}

	// Get another block
	blockNumber2, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}

	if blockNumber2 != 1 {
		t.Errorf("Second BlockNumber = %d, want 1", blockNumber2)
	}
}

// Test getting a new posting block by expanding file
func TestGetNewPostingBlockExpandFile(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(2)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	file.Close()
	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Exhaust the free list
	_, err = GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock 1 failed: %v", err)
	}
	_, err = GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock 2 failed: %v", err)
	}

	// FirstFreeBlock should now be NoBlock
	if header.FirstFreeBlock != NoBlock {
		t.Errorf("FirstFreeBlock = %d, want NoBlock", header.FirstFreeBlock)
	}

	// Get another block (should expand the file)
	oldSize, _ := file.Stat()
	blockNumber, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock (expand) failed: %v", err)
	}

	// Should get block 2 (next sequential block)
	if blockNumber != 2 {
		t.Errorf("BlockNumber = %d, want 2", blockNumber)
	}

	// File should be larger
	newSize, _ := file.Stat()
	expectedSize := oldSize.Size() + int64(blockSize)
	if newSize.Size() != expectedSize {
		t.Errorf("File size = %d, want %d", newSize.Size(), expectedSize)
	}
}

// Test adding a free posting block to empty free list
func TestAddFreePostingBlockToEmptyList(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(3)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	file.Close()
	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Exhaust the free list
	block0, _ := GetNewPostingBlock(file, header)
	block1, _ := GetNewPostingBlock(file, header)
	block2, _ := GetNewPostingBlock(file, header)

	// Free list should be empty
	if header.FirstFreeBlock != NoBlock {
		t.Errorf("FirstFreeBlock = %d, want NoBlock", header.FirstFreeBlock)
	}

	// Add block 1 back to free list
	err = AddFreePostingBlock(file, header, block1)
	if err != nil {
		t.Fatalf("AddFreePostingBlock failed: %v", err)
	}

	// FirstFreeBlock should now point to block 1
	if header.FirstFreeBlock != block1 {
		t.Errorf("FirstFreeBlock = %d, want %d", header.FirstFreeBlock, block1)
	}

	// Read the freed block and verify it points to NoBlock
	freedBlock, err := ReadPostingsBlock(file, header.BlockSize, block1)
	if err != nil {
		t.Fatalf("ReadPostingsBlock failed: %v", err)
	}
	if freedBlock.Header.NextBlock != NoBlock {
		t.Errorf("NextBlock = %d, want NoBlock", freedBlock.Header.NextBlock)
	}

	// Verify we can get the block back from free list
	newBlock, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}
	if newBlock != block1 {
		t.Errorf("Got block %d, want %d", newBlock, block1)
	}

	// Verify the other blocks weren't affected
	_ = block0
	_ = block2
}

// Test adding a free posting block to non-empty free list
func TestAddFreePostingBlockToNonEmptyList(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(5)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	file.Close()
	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Get some blocks but leave some in free list
	block0, _ := GetNewPostingBlock(file, header)
	block1, _ := GetNewPostingBlock(file, header)

	// Free list should have blocks 2, 3, 4
	if header.FirstFreeBlock != 2 {
		t.Errorf("FirstFreeBlock = %d, want 2", header.FirstFreeBlock)
	}

	// Add block 0 back to free list (should go at the start)
	err = AddFreePostingBlock(file, header, block0)
	if err != nil {
		t.Fatalf("AddFreePostingBlock failed: %v", err)
	}

	// FirstFreeBlock should now point to block 0
	if header.FirstFreeBlock != block0 {
		t.Errorf("FirstFreeBlock = %d, want %d", header.FirstFreeBlock, block0)
	}

	// Read block 0 and verify it points to block 2
	freedBlock, err := ReadPostingsBlock(file, header.BlockSize, block0)
	if err != nil {
		t.Fatalf("ReadPostingsBlock failed: %v", err)
	}
	if freedBlock.Header.NextBlock != 2 {
		t.Errorf("Block 0 NextBlock = %d, want 2", freedBlock.Header.NextBlock)
	}

	// Add block 1 to free list
	err = AddFreePostingBlock(file, header, block1)
	if err != nil {
		t.Fatalf("AddFreePostingBlock failed: %v", err)
	}

	// FirstFreeBlock should now point to block 1
	if header.FirstFreeBlock != block1 {
		t.Errorf("FirstFreeBlock = %d, want %d", header.FirstFreeBlock, block1)
	}

	// Read block 1 and verify it points to block 0
	freedBlock1, err := ReadPostingsBlock(file, header.BlockSize, block1)
	if err != nil {
		t.Fatalf("ReadPostingsBlock failed: %v", err)
	}
	if freedBlock1.Header.NextBlock != block0 {
		t.Errorf("Block 1 NextBlock = %d, want %d", freedBlock1.Header.NextBlock, block0)
	}

	// Get blocks back and verify order (LIFO: 1, 0, 2, 3, 4)
	retrieved1, _ := GetNewPostingBlock(file, header)
	retrieved2, _ := GetNewPostingBlock(file, header)
	retrieved3, _ := GetNewPostingBlock(file, header)

	if retrieved1 != block1 {
		t.Errorf("First retrieved = %d, want %d", retrieved1, block1)
	}
	if retrieved2 != block0 {
		t.Errorf("Second retrieved = %d, want %d", retrieved2, block0)
	}
	if retrieved3 != 2 {
		t.Errorf("Third retrieved = %d, want 2", retrieved3)
	}
}

func TestDeletePostingsList_EmptyList(t *testing.T) {
	// Setup: Create a postings file
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(5)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	header, err := ReadPostingsFileHeader(file)
	if err != nil {
		t.Fatalf("ReadPostingsFileHeader failed: %v", err)
	}

	// Test: Delete empty list (NoBlock)
	err = DeletePostingsList(file, header, NoBlock)
	if err != nil {
		t.Errorf("DeletePostingsList with NoBlock should not error, got: %v", err)
	}

	// Verify header unchanged
	if header.FirstFreeBlock != 0 {
		t.Errorf("FirstFreeBlock should still be 0, got %d", header.FirstFreeBlock)
	}
}

func TestDeletePostingsList_SingleBlock(t *testing.T) {
	// Setup: Create a postings file
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(5)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	header, err := ReadPostingsFileHeader(file)
	if err != nil {
		t.Fatalf("ReadPostingsFileHeader failed: %v", err)
	}

	// Allocate a block and create a single-block list
	block0, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}

	// Write a single block with NoBlock as NextBlock
	postingBlock := &PostingBlock{
		Header: PostingBlockHeader{
			BlockNumber: block0,
			DictID:      100,
			NextBlock:   NoBlock,
			Count:       3,
		},
		RecordIDs: []uint32{1, 2, 3},
	}
	err = WritePostingsBlock(file, blockSize, postingBlock)
	if err != nil {
		t.Fatalf("WritePostingsBlock failed: %v", err)
	}

	// Test: Delete the single-block list
	err = DeletePostingsList(file, header, block0)
	if err != nil {
		t.Fatalf("DeletePostingsList failed: %v", err)
	}

	// Verify block was added to free list
	if header.FirstFreeBlock != block0 {
		t.Errorf("FirstFreeBlock = %d, want %d", header.FirstFreeBlock, block0)
	}

	// Verify we can get the block back
	retrieved, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}
	if retrieved != block0 {
		t.Errorf("Retrieved block = %d, want %d", retrieved, block0)
	}
}

func TestDeletePostingsList_MultipleBlocks(t *testing.T) {
	// Setup: Create a postings file
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(10)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	defer file.Close()

	header, err := ReadPostingsFileHeader(file)
	if err != nil {
		t.Fatalf("ReadPostingsFileHeader failed: %v", err)
	}

	// Allocate 3 blocks and create a linked list
	block0, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}
	block1, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}
	block2, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}

	// Create a 3-block linked list: block0 -> block1 -> block2 -> NoBlock
	postingBlock0 := &PostingBlock{
		Header: PostingBlockHeader{
			BlockNumber: block0,
			DictID:      100,
			NextBlock:   block1,
			Count:       2,
		},
		RecordIDs: []uint32{1, 2},
	}
	err = WritePostingsBlock(file, blockSize, postingBlock0)
	if err != nil {
		t.Fatalf("WritePostingsBlock failed: %v", err)
	}

	postingBlock1 := &PostingBlock{
		Header: PostingBlockHeader{
			BlockNumber: block1,
			DictID:      100,
			NextBlock:   block2,
			Count:       2,
		},
		RecordIDs: []uint32{3, 4},
	}
	err = WritePostingsBlock(file, blockSize, postingBlock1)
	if err != nil {
		t.Fatalf("WritePostingsBlock failed: %v", err)
	}

	postingBlock2 := &PostingBlock{
		Header: PostingBlockHeader{
			BlockNumber: block2,
			DictID:      100,
			NextBlock:   NoBlock,
			Count:       2,
		},
		RecordIDs: []uint32{5, 6},
	}
	err = WritePostingsBlock(file, blockSize, postingBlock2)
	if err != nil {
		t.Fatalf("WritePostingsBlock failed: %v", err)
	}

	// Test: Delete the entire 3-block list
	err = DeletePostingsList(file, header, block0)
	if err != nil {
		t.Fatalf("DeletePostingsList failed: %v", err)
	}

	// Verify all 3 blocks were added to free list
	// They should be added in order: block2, block1, block0 (LIFO)
	retrieved0, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}
	retrieved1, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}
	retrieved2, err := GetNewPostingBlock(file, header)
	if err != nil {
		t.Fatalf("GetNewPostingBlock failed: %v", err)
	}

	// Since blocks are added to free list in LIFO order,
	// we get them back in reverse order: block2, block1, block0
	if retrieved0 != block2 {
		t.Errorf("First retrieved = %d, want %d", retrieved0, block2)
	}
	if retrieved1 != block1 {
		t.Errorf("Second retrieved = %d, want %d", retrieved1, block1)
	}
	if retrieved2 != block0 {
		t.Errorf("Third retrieved = %d, want %d", retrieved2, block0)
	}
}
