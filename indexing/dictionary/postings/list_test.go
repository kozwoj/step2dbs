package postings

import (
	"os"
	"testing"
)

// Test adding a new postings list with a single record
func TestAddNewListSingleRecord(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add a new list with one record
	dictID := uint32(100)
	recordIDs := []uint32{1001}

	startBlock, err := postingsList.AddNewList(file, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Should get block 0 (first free block)
	if startBlock != 0 {
		t.Errorf("StartBlock = %d, want 0", startBlock)
	}

	// Read back the list
	readRecords, readBlocks, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	// Verify records
	if len(readRecords) != len(recordIDs) {
		t.Errorf("Got %d records, want %d", len(readRecords), len(recordIDs))
	}
	if readRecords[0] != recordIDs[0] {
		t.Errorf("Record[0] = %d, want %d", readRecords[0], recordIDs[0])
	}

	// Verify only one block used
	if len(readBlocks) != 1 {
		t.Errorf("Used %d blocks, want 1", len(readBlocks))
	}
}

// Test adding a new postings list with multiple records in one block
func TestAddNewListMultipleRecordsOneBlock(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add records that fit in one block (512-16)/4 = 124 records max
	dictID := uint32(200)
	recordIDs := make([]uint32, 50)
	for i := range recordIDs {
		recordIDs[i] = uint32(2000 + i)
	}

	startBlock, err := postingsList.AddNewList(file, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Read back the list
	readRecords, readBlocks, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	// Verify records
	if len(readRecords) != len(recordIDs) {
		t.Errorf("Got %d records, want %d", len(readRecords), len(recordIDs))
	}
	for i, record := range readRecords {
		if record != recordIDs[i] {
			t.Errorf("Record[%d] = %d, want %d", i, record, recordIDs[i])
		}
	}

	// Verify only one block used
	if len(readBlocks) != 1 {
		t.Errorf("Used %d blocks, want 1", len(readBlocks))
	}
}

// Test adding a new postings list that spans multiple blocks
func TestAddNewListMultipleBlocks(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(256) // Smaller blocks for easier testing
	initialSize := uint32(10)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add records that span multiple blocks
	// (256-16)/4 = 60 records per block
	// 150 records = 3 blocks needed
	dictID := uint32(300)
	recordIDs := make([]uint32, 150)
	for i := range recordIDs {
		recordIDs[i] = uint32(3000 + i)
	}

	startBlock, err := postingsList.AddNewList(file, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Read back the list
	readRecords, readBlocks, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	// Verify records
	if len(readRecords) != len(recordIDs) {
		t.Errorf("Got %d records, want %d", len(readRecords), len(recordIDs))
	}
	for i, record := range readRecords {
		if record != recordIDs[i] {
			t.Errorf("Record[%d] = %d, want %d", i, record, recordIDs[i])
		}
	}

	// Verify 3 blocks used
	if len(readBlocks) != 3 {
		t.Errorf("Used %d blocks, want 3", len(readBlocks))
	}
}

// Test WriteBackRecordsList with same number of blocks
func TestWriteBackRecordsListSameSize(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add initial list
	dictID := uint32(400)
	initialRecords := []uint32{4001, 4002, 4003}
	startBlock, err := postingsList.AddNewList(file, header, initialRecords, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Read to get block numbers
	_, blockNumbers, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	// Write back different records but same count (still fits in same blocks)
	newRecords := []uint32{4011, 4012, 4013}
	err = postingsList.WriteBackRecordsList(file, header, blockNumbers, newRecords, dictID)
	if err != nil {
		t.Fatalf("WriteBackRecordsList failed: %v", err)
	}

	// Read back and verify
	readRecords, readBlocks, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	if len(readRecords) != len(newRecords) {
		t.Errorf("Got %d records, want %d", len(readRecords), len(newRecords))
	}
	for i, record := range readRecords {
		if record != newRecords[i] {
			t.Errorf("Record[%d] = %d, want %d", i, record, newRecords[i])
		}
	}

	// Should still use same number of blocks
	if len(readBlocks) != len(blockNumbers) {
		t.Errorf("Used %d blocks, want %d", len(readBlocks), len(blockNumbers))
	}
}

// Test WriteBackRecordsList growing the list
func TestWriteBackRecordsListGrow(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(256) // Smaller blocks
	initialSize := uint32(10)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add initial small list (1 block)
	dictID := uint32(500)
	initialRecords := []uint32{5001, 5002, 5003}
	startBlock, err := postingsList.AddNewList(file, header, initialRecords, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Read to get block numbers
	_, blockNumbers, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	initialBlockCount := len(blockNumbers)

	// Write back larger list that needs more blocks
	// (256-16)/4 = 60 records per block, so 100 records = 2 blocks
	newRecords := make([]uint32, 100)
	for i := range newRecords {
		newRecords[i] = uint32(5100 + i)
	}

	err = postingsList.WriteBackRecordsList(file, header, blockNumbers, newRecords, dictID)
	if err != nil {
		t.Fatalf("WriteBackRecordsList failed: %v", err)
	}

	// Read back and verify
	readRecords, readBlocks, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	if len(readRecords) != len(newRecords) {
		t.Errorf("Got %d records, want %d", len(readRecords), len(newRecords))
	}
	for i, record := range readRecords {
		if record != newRecords[i] {
			t.Errorf("Record[%d] = %d, want %d", i, record, newRecords[i])
		}
	}

	// Should use more blocks now
	if len(readBlocks) <= initialBlockCount {
		t.Errorf("Used %d blocks, want more than %d", len(readBlocks), initialBlockCount)
	}
}

// Test WriteBackRecordsList shrinking the list
func TestWriteBackRecordsListShrink(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(256)
	initialSize := uint32(10)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatSlice)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add initial large list (multiple blocks)
	dictID := uint32(600)
	initialRecords := make([]uint32, 100) // 2 blocks
	for i := range initialRecords {
		initialRecords[i] = uint32(6000 + i)
	}

	startBlock, err := postingsList.AddNewList(file, header, initialRecords, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Read to get block numbers
	_, blockNumbers, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	initialBlockCount := len(blockNumbers)

	// Write back smaller list
	newRecords := []uint32{6001, 6002, 6003}

	err = postingsList.WriteBackRecordsList(file, header, blockNumbers, newRecords, dictID)
	if err != nil {
		t.Fatalf("WriteBackRecordsList failed: %v", err)
	}

	// Read back and verify
	readRecords, readBlocks, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	if len(readRecords) != len(newRecords) {
		t.Errorf("Got %d records, want %d", len(readRecords), len(newRecords))
	}
	for i, record := range readRecords {
		if record != newRecords[i] {
			t.Errorf("Record[%d] = %d, want %d", i, record, newRecords[i])
		}
	}

	// Should use fewer blocks now
	if len(readBlocks) >= initialBlockCount {
		t.Errorf("Used %d blocks, want fewer than %d", len(readBlocks), initialBlockCount)
	}

	// Verify extra blocks were added to free list
	// (The freed blocks should be available for reuse)
}

// Test multiple independent lists
func TestMultipleIndependentLists(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	// Create PostingsList instance
	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Create three independent lists
	list1Records := []uint32{1001, 1002, 1003}
	list2Records := []uint32{2001, 2002, 2003, 2004}
	list3Records := []uint32{3001, 3002}

	startBlock1, err := postingsList.AddNewList(file, header, list1Records, 100)
	if err != nil {
		t.Fatalf("AddNewList 1 failed: %v", err)
	}

	startBlock2, err := postingsList.AddNewList(file, header, list2Records, 200)
	if err != nil {
		t.Fatalf("AddNewList 2 failed: %v", err)
	}

	startBlock3, err := postingsList.AddNewList(file, header, list3Records, 300)
	if err != nil {
		t.Fatalf("AddNewList 3 failed: %v", err)
	}

	// Verify all three lists independently
	readRecords1, _, err := postingsList.GetRecordsList(file, startBlock1)
	if err != nil {
		t.Fatalf("GetRecordsList 1 failed: %v", err)
	}

	readRecords2, _, err := postingsList.GetRecordsList(file, startBlock2)
	if err != nil {
		t.Fatalf("GetRecordsList 2 failed: %v", err)
	}

	readRecords3, _, err := postingsList.GetRecordsList(file, startBlock3)
	if err != nil {
		t.Fatalf("GetRecordsList 3 failed: %v", err)
	}

	// Verify each list
	if len(readRecords1) != len(list1Records) {
		t.Errorf("List 1: got %d records, want %d", len(readRecords1), len(list1Records))
	}
	if len(readRecords2) != len(list2Records) {
		t.Errorf("List 2: got %d records, want %d", len(readRecords2), len(list2Records))
	}
	if len(readRecords3) != len(list3Records) {
		t.Errorf("List 3: got %d records, want %d", len(readRecords3), len(list3Records))
	}

	for i := range list1Records {
		if readRecords1[i] != list1Records[i] {
			t.Errorf("List 1 Record[%d] = %d, want %d", i, readRecords1[i], list1Records[i])
		}
	}
	for i := range list2Records {
		if readRecords2[i] != list2Records[i] {
			t.Errorf("List 2 Record[%d] = %d, want %d", i, readRecords2[i], list2Records[i])
		}
	}
	for i := range list3Records {
		if readRecords3[i] != list3Records[i] {
			t.Errorf("List 3 Record[%d] = %d, want %d", i, readRecords3[i], list3Records[i])
		}
	}
}

// Test idempotent AddRecordID operation
func TestAddRecordIDIdempotent(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add a new list with initial records
	dictID := uint32(100)
	recordIDs := []uint32{10, 20, 30}

	startBlock, err := postingsList.AddNewList(file, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Add a new record ID
	err = postingsList.AddRecordID(file, header, startBlock, 25, dictID)
	if err != nil {
		t.Fatalf("First AddRecordID failed: %v", err)
	}

	// Verify it was added
	readRecords, _, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}
	expected := []uint32{10, 20, 25, 30}
	if len(readRecords) != len(expected) {
		t.Fatalf("Expected %d records, got %d", len(expected), len(readRecords))
	}
	for i := range expected {
		if readRecords[i] != expected[i] {
			t.Errorf("Record[%d] = %d, want %d", i, readRecords[i], expected[i])
		}
	}

	// Add the same record ID again (idempotent)
	err = postingsList.AddRecordID(file, header, startBlock, 25, dictID)
	if err != nil {
		t.Errorf("Second AddRecordID (idempotent) failed: %v", err)
	}

	// Verify list hasn't changed
	readRecords, _, err = postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}
	if len(readRecords) != len(expected) {
		t.Fatalf("Expected %d records after idempotent add, got %d", len(expected), len(readRecords))
	}
	for i := range expected {
		if readRecords[i] != expected[i] {
			t.Errorf("Record[%d] = %d, want %d", i, readRecords[i], expected[i])
		}
	}

	// Try adding with wrong dictID (should fail)
	err = postingsList.AddRecordID(file, header, startBlock, 40, 999)
	if err == nil {
		t.Error("Expected error when adding with wrong dictID, got nil")
	}
}

// Test idempotent RemoveRecordID operation
func TestRemoveRecordIDIdempotent(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add a new list with initial records
	dictID := uint32(100)
	recordIDs := []uint32{10, 20, 30, 40}

	startBlock, err := postingsList.AddNewList(file, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Remove a record ID
	err = postingsList.RemoveRecordID(file, header, startBlock, 20, dictID)
	if err != nil {
		t.Fatalf("First RemoveRecordID failed: %v", err)
	}

	// Verify it was removed
	readRecords, _, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}
	expected := []uint32{10, 30, 40}
	if len(readRecords) != len(expected) {
		t.Fatalf("Expected %d records, got %d", len(expected), len(readRecords))
	}
	for i := range expected {
		if readRecords[i] != expected[i] {
			t.Errorf("Record[%d] = %d, want %d", i, readRecords[i], expected[i])
		}
	}

	// Remove the same record ID again (idempotent - should not error)
	err = postingsList.RemoveRecordID(file, header, startBlock, 20, dictID)
	if err != nil {
		t.Errorf("Second RemoveRecordID (idempotent) failed: %v", err)
	}

	// Verify list hasn't changed
	readRecords, _, err = postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}
	if len(readRecords) != len(expected) {
		t.Fatalf("Expected %d records after idempotent remove, got %d", len(expected), len(readRecords))
	}
	for i := range expected {
		if readRecords[i] != expected[i] {
			t.Errorf("Record[%d] = %d, want %d", i, readRecords[i], expected[i])
		}
	}

	// Try removing with wrong dictID (should fail)
	err = postingsList.RemoveRecordID(file, header, startBlock, 30, 999)
	if err == nil {
		t.Error("Expected error when removing with wrong dictID, got nil")
	}

	// Verify list hasn't changed after failed remove
	readRecords, _, err = postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}
	if len(readRecords) != len(expected) {
		t.Fatalf("Expected %d records after failed remove, got %d", len(expected), len(readRecords))
	}
}

// Test that removing the last recordID from a postings list keeps an empty list in the file
func TestRemoveLastRecordKeepsEmptyList(t *testing.T) {
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
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	postingsList := NewPostingsList(FormatSlice, blockSize)

	// Add a new list with a single record
	dictID := uint32(100)
	recordIDs := []uint32{42}

	startBlock, err := postingsList.AddNewList(file, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Verify the list has one record
	readRecords, blockNumbers, err := postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}
	if len(readRecords) != 1 {
		t.Fatalf("Expected 1 record, got %d", len(readRecords))
	}
	if readRecords[0] != 42 {
		t.Errorf("Expected record ID 42, got %d", readRecords[0])
	}
	if len(blockNumbers) != 1 {
		t.Fatalf("Expected 1 block, got %d", len(blockNumbers))
	}

	// Remove the last (and only) record
	err = postingsList.RemoveRecordID(file, header, startBlock, 42, dictID)
	if err != nil {
		t.Fatalf("RemoveRecordID failed: %v", err)
	}

	// Verify the list is now empty but still exists
	readRecords, blockNumbers, err = postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed after removing last record: %v", err)
	}
	if len(readRecords) != 0 {
		t.Errorf("Expected empty list (0 records), got %d records", len(readRecords))
	}
	// The list should still have at least one block allocated
	if len(blockNumbers) == 0 {
		t.Error("Expected at least one block to be allocated for empty list, got 0 blocks")
	}

	// Verify we can read the block directly and it has correct structure
	block, err := ReadPostingsBlock(file, blockSize, startBlock)
	if err != nil {
		t.Fatalf("ReadPostingsBlock failed: %v", err)
	}
	if block.Header.DictID != dictID {
		t.Errorf("Block dictID = %d, want %d", block.Header.DictID, dictID)
	}
	if block.Header.Count != 0 {
		t.Errorf("Block count = %d, want 0", block.Header.Count)
	}

	// Verify we can add a new record to the empty list (reuse it)
	err = postingsList.AddRecordID(file, header, startBlock, 100, dictID)
	if err != nil {
		t.Fatalf("AddRecordID to empty list failed: %v", err)
	}

	readRecords, _, err = postingsList.GetRecordsList(file, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed after re-adding: %v", err)
	}
	if len(readRecords) != 1 {
		t.Fatalf("Expected 1 record after re-adding, got %d", len(readRecords))
	}
	if readRecords[0] != 100 {
		t.Errorf("Expected record ID 100, got %d", readRecords[0])
	}
}
