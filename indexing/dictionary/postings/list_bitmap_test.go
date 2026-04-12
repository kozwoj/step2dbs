package postings

import (
	"os"
	"testing"
)

// TestBitmapPostingsListInterface verifies that BitmapPostingsList implements the PostingsList interface correctly.
func TestBitmapPostingsListInterface(t *testing.T) {
	// Create a test file
	file, err := os.CreateTemp("", "postings_bitmap_test_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	blockSize := uint32(512)
	initialBlocks := uint32(10)

	// Create postings file with bitmap format
	postingsFile, err := CreatePostingsFile(file.Name(), blockSize, initialBlocks, FormatBitmap)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	postingsFile.Close()

	// Re-open the file
	var header *PostingsFileHeader
	postingsFile, header, err = OpenPostingsFile(file.Name())
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer postingsFile.Close()

	// Verify format is bitmap
	if header.Format != FormatBitmap {
		t.Fatalf("Expected FormatBitmap, got %v", header.Format)
	}

	// Create PostingsList using factory function
	list := NewPostingsList(FormatBitmap, blockSize)

	// Test AddNewList with sparse record IDs (good for bitmap compression)
	recordIDs := []uint32{100, 500, 1000, 5000, 10000}
	dictID := uint32(1)
	startBlock, err := list.AddNewList(postingsFile, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	if startBlock == NoBlock {
		t.Fatalf("Expected valid start block, got NoBlock")
	}

	// Test GetRecordsList
	retrievedIDs, blockNumbers, err := list.GetRecordsList(postingsFile, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	if len(retrievedIDs) != len(recordIDs) {
		t.Fatalf("Expected %d record IDs, got %d", len(recordIDs), len(retrievedIDs))
	}

	for i, id := range recordIDs {
		if retrievedIDs[i] != id {
			t.Errorf("Record ID mismatch at index %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}

	// Test WriteBackRecordsList with more records (including dense range)
	newRecordIDs := []uint32{100, 101, 102, 103, 104, 105, 500, 1000, 5000, 10000, 10001, 10002}
	err = list.WriteBackRecordsList(postingsFile, header, blockNumbers, newRecordIDs, dictID)
	if err != nil {
		t.Fatalf("WriteBackRecordsList failed: %v", err)
	}

	// Verify the updated list
	updatedIDs, _, err := list.GetRecordsList(postingsFile, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList (after update) failed: %v", err)
	}

	if len(updatedIDs) != len(newRecordIDs) {
		t.Fatalf("Expected %d updated record IDs, got %d", len(newRecordIDs), len(updatedIDs))
	}

	for i, id := range newRecordIDs {
		if updatedIDs[i] != id {
			t.Errorf("Updated record ID mismatch at index %d: expected %d, got %d", i, id, updatedIDs[i])
		}
	}

	t.Logf("Successfully tested BitmapPostingsList with %d initial records and %d updated records",
		len(recordIDs), len(newRecordIDs))
}

// TestBitmapPostingsListFactoryFormatBitmap verifies the factory creates a BitmapPostingsList for FormatBitmap.
func TestBitmapPostingsListFactoryFormatBitmap(t *testing.T) {
	blockSize := uint32(512)
	list := NewPostingsList(FormatBitmap, blockSize)

	// Verify it's a BitmapPostingsList
	_, ok := list.(*BitmapPostingsList)
	if !ok {
		t.Errorf("Expected *BitmapPostingsList, got %T", list)
	}

	t.Log("Successfully verified factory creates BitmapPostingsList for FormatBitmap")
}

// TestBitmapPostingsListCompression tests that bitmap provides good compression for sparse data.
func TestBitmapPostingsListCompression(t *testing.T) {
	// Create a test file
	file, err := os.CreateTemp("", "postings_bitmap_compression_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	blockSize := uint32(512)
	initialBlocks := uint32(50)

	postingsFile, err := CreatePostingsFile(file.Name(), blockSize, initialBlocks, FormatBitmap)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	postingsFile.Close()

	postingsFile, header, err := OpenPostingsFile(file.Name())
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer postingsFile.Close()

	list := NewPostingsList(FormatBitmap, blockSize)

	// Create very sparse data - 100 records spread across 0 to 1,000,000
	recordIDs := make([]uint32, 100)
	for i := 0; i < 100; i++ {
		recordIDs[i] = uint32(i * 10000)
	}

	dictID := uint32(1)
	startBlock, err := list.AddNewList(postingsFile, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Get back the records and block numbers used
	retrievedIDs, blockNumbers, err := list.GetRecordsList(postingsFile, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	// Verify correctness
	if len(retrievedIDs) != len(recordIDs) {
		t.Fatalf("Expected %d records, got %d", len(recordIDs), len(retrievedIDs))
	}

	for i, id := range recordIDs {
		if retrievedIDs[i] != id {
			t.Errorf("Record ID mismatch at index %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}

	// With uncompressed format, 100 records would need: 100 * 4 = 400 bytes
	// With blockSize=512 and 16-byte headers, that's 1 block minimum
	// Roaring bitmap should compress this very well too
	t.Logf("Stored 100 sparse records (range 0-1,000,000) using %d block(s)", len(blockNumbers))
	t.Logf("Block usage: %d bytes (including headers)", len(blockNumbers)*int(blockSize))
}

// TestBitmapPostingsListDenseData tests bitmap with dense consecutive IDs.
func TestBitmapPostingsListDenseData(t *testing.T) {
	file, err := os.CreateTemp("", "postings_bitmap_dense_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	defer os.Remove(file.Name())
	defer file.Close()

	blockSize := uint32(512)
	initialBlocks := uint32(10)

	postingsFile, err := CreatePostingsFile(file.Name(), blockSize, initialBlocks, FormatBitmap)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	postingsFile.Close()

	postingsFile, header, err := OpenPostingsFile(file.Name())
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer postingsFile.Close()

	list := NewPostingsList(FormatBitmap, blockSize)

	// Create dense consecutive data - 1000 consecutive records
	recordIDs := make([]uint32, 1000)
	for i := 0; i < 1000; i++ {
		recordIDs[i] = uint32(i + 1)
	}

	dictID := uint32(1)
	startBlock, err := list.AddNewList(postingsFile, header, recordIDs, dictID)
	if err != nil {
		t.Fatalf("AddNewList failed: %v", err)
	}

	// Get back the records
	retrievedIDs, blockNumbers, err := list.GetRecordsList(postingsFile, startBlock)
	if err != nil {
		t.Fatalf("GetRecordsList failed: %v", err)
	}

	// Verify correctness
	if len(retrievedIDs) != len(recordIDs) {
		t.Fatalf("Expected %d records, got %d", len(recordIDs), len(retrievedIDs))
	}

	for i, id := range recordIDs {
		if retrievedIDs[i] != id {
			t.Errorf("Record ID mismatch at index %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}

	// With uncompressed format: 1000 * 4 = 4000 bytes = 9 blocks (with 496 bytes/block)
	// Roaring with run-length encoding should be extremely efficient for consecutive IDs
	t.Logf("Stored 1000 consecutive records using %d block(s)", len(blockNumbers))
	t.Logf("Block usage: %d bytes total", len(blockNumbers)*int(blockSize))
	t.Logf("Compression ratio: %.2f%% of uncompressed slice format",
		float64(len(blockNumbers)*int(blockSize))/float64(1000*4)*100)
}

// Test that removing the last recordID from a bitmap postings list keeps an empty list in the file
func TestBitmapRemoveLastRecordKeepsEmptyList(t *testing.T) {
	tempFile, err := os.CreateTemp("", "testpostings_bitmap_*.post")
	if err != nil {
		t.Fatalf("Failed to create temp file: %v", err)
	}
	filePath := tempFile.Name()
	tempFile.Close()
	os.Remove(filePath)
	defer os.Remove(filePath)

	blockSize := uint32(512)
	initialSize := uint32(5)

	file, err := CreatePostingsFile(filePath, blockSize, initialSize, FormatBitmap)
	if err != nil {
		t.Fatalf("CreatePostingsFile failed: %v", err)
	}
	file.Close()

	file, header, err := OpenPostingsFile(filePath)
	if err != nil {
		t.Fatalf("OpenPostingsFile failed: %v", err)
	}
	defer file.Close()

	postingsList := NewPostingsList(FormatBitmap, blockSize)

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
	if len(blockNumbers) == 0 {
		t.Fatal("Expected at least 1 block to be allocated")
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
