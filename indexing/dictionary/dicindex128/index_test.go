package dicindex128

import (
	"os"
	"testing"
)

// Test opening and closing an index
func TestOpenAndClose(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open the index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}

	// Verify header
	if idx.Header.BlockSize != blockSize {
		t.Errorf("BlockSize = %d, want %d", idx.Header.BlockSize, blockSize)
	}
	if idx.Header.FileLength != fileLength {
		t.Errorf("FileLength = %d, want %d", idx.Header.FileLength, fileLength)
	}
	if idx.Header.RootNode != 1 {
		t.Errorf("RootNode = %d, want 1", idx.Header.RootNode)
	}
	if idx.Header.FirstLeaf != 2 {
		t.Errorf("FirstLeaf = %d, want 2", idx.Header.FirstLeaf)
	}

	// Close the index
	err = idx.Close()
	if err != nil {
		t.Fatalf("Close failed: %v", err)
	}
}

// Test inserting and finding a single entry
func TestInsertAndFindSingle(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert an entry
	entry := &IndexEntry128{
		Hash:        HashString128("hello"),
		DictID:      100,
		PostingsRef: 200,
	}

	err = idx.Insert(entry)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Find the entry
	dictID, postingsRef, err := idx.Find(entry.Hash)
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}

	if dictID != entry.DictID {
		t.Errorf("DictID = %d, want %d", dictID, entry.DictID)
	}
	if postingsRef != entry.PostingsRef {
		t.Errorf("PostingsRef = %d, want %d", postingsRef, entry.PostingsRef)
	}
}

// Test inserting multiple entries
func TestInsertMultiple(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert multiple entries
	entries := []*IndexEntry128{
		{Hash: HashString128("apple"), DictID: 1, PostingsRef: 10},
		{Hash: HashString128("banana"), DictID: 2, PostingsRef: 20},
		{Hash: HashString128("cherry"), DictID: 3, PostingsRef: 30},
		{Hash: HashString128("date"), DictID: 4, PostingsRef: 40},
		{Hash: HashString128("elderberry"), DictID: 5, PostingsRef: 50},
	}

	for _, entry := range entries {
		err = idx.Insert(entry)
		if err != nil {
			t.Fatalf("Insert failed for entry %v: %v", entry, err)
		}
	}

	// Find all entries
	for _, entry := range entries {
		dictID, postingsRef, err := idx.Find(entry.Hash)
		if err != nil {
			t.Fatalf("Find failed for hash %v: %v", entry.Hash, err)
		}

		if dictID != entry.DictID {
			t.Errorf("DictID = %d, want %d", dictID, entry.DictID)
		}
		if postingsRef != entry.PostingsRef {
			t.Errorf("PostingsRef = %d, want %d", postingsRef, entry.PostingsRef)
		}
	}
}

// Test duplicate insertion
func TestInsertDuplicate(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert an entry
	entry := &IndexEntry128{
		Hash:        HashString128("hello"),
		DictID:      100,
		PostingsRef: 200,
	}

	err = idx.Insert(entry)
	if err != nil {
		t.Fatalf("Insert failed: %v", err)
	}

	// Try to insert the same entry again
	err = idx.Insert(entry)
	if err != ErrIndexEntryAlreadyExists {
		t.Errorf("Expected ErrIndexEntryAlreadyExists, got %v", err)
	}
}

// Test finding non-existent entry
func TestFindNonExistent(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Try to find a non-existent entry
	hash := HashString128("nonexistent")
	_, _, err = idx.Find(hash)
	if err != ErrIndexEntryNotFound {
		t.Errorf("Expected ErrIndexEntryNotFound, got %v", err)
	}
}

// Test delete operation
func TestDelete(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert entries
	entries := []*IndexEntry128{
		{Hash: HashString128("apple"), DictID: 1, PostingsRef: 10},
		{Hash: HashString128("banana"), DictID: 2, PostingsRef: 20},
		{Hash: HashString128("cherry"), DictID: 3, PostingsRef: 30},
	}

	for _, entry := range entries {
		err = idx.Insert(entry)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Delete one entry
	err = idx.Delete(entries[1].Hash)
	if err != nil {
		t.Fatalf("Delete failed: %v", err)
	}

	// Verify it's deleted
	_, _, err = idx.Find(entries[1].Hash)
	if err != ErrIndexEntryNotFound {
		t.Errorf("Expected ErrIndexEntryNotFound after delete, got %v", err)
	}

	// Verify other entries still exist
	for i, entry := range entries {
		if i == 1 {
			continue // skip deleted entry
		}
		dictID, postingsRef, err := idx.Find(entry.Hash)
		if err != nil {
			t.Fatalf("Find failed for entry %d: %v", i, err)
		}
		if dictID != entry.DictID || postingsRef != entry.PostingsRef {
			t.Errorf("Entry %d mismatch", i)
		}
	}
}

// Test delete non-existent entry
func TestDeleteNonExistent(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(10)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Try to delete a non-existent entry
	hash := HashString128("nonexistent")
	err = idx.Delete(hash)
	if err != ErrIndexEntryNotFound {
		t.Errorf("Expected ErrIndexEntryNotFound, got %v", err)
	}
}

// Test inserting enough entries to trigger a split
func TestInsertWithSplit(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(256)
	fileLength := uint16(50)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Calculate max entries per block
	maxEntries := MaxEntriesPerBlock(blockSize)
	t.Logf("Max entries per block: %d", maxEntries)

	// Insert more entries than can fit in one leaf block
	numEntries := maxEntries + 5
	entries := make([]*IndexEntry128, numEntries)

	for i := 0; i < numEntries; i++ {
		entries[i] = &IndexEntry128{
			Hash:        Hash128{High: uint64(i * 100), Low: uint64(i)},
			DictID:      uint32(i + 1),
			PostingsRef: uint32((i + 1) * 10),
		}
		err = idx.Insert(entries[i])
		if err != nil {
			t.Fatalf("Insert failed for entry %d: %v", i, err)
		}
	}

	t.Logf("Successfully inserted %d entries (triggered split)", numEntries)

	// Verify all entries can be found
	for i, entry := range entries {
		dictID, postingsRef, err := idx.Find(entry.Hash)
		if err != nil {
			t.Fatalf("Find failed for entry %d: %v", i, err)
		}
		if dictID != entry.DictID {
			t.Errorf("Entry %d: DictID = %d, want %d", i, dictID, entry.DictID)
		}
		if postingsRef != entry.PostingsRef {
			t.Errorf("Entry %d: PostingsRef = %d, want %d", i, postingsRef, entry.PostingsRef)
		}
	}
}

// Test inserting many entries to trigger multiple splits and tree growth
func TestInsertManyEntries(t *testing.T) {
	// Create a temporary directory
	tempDir, err := os.MkdirTemp("", "testindex")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	fileName := "testindex"
	blockSize := uint16(512)
	fileLength := uint16(100)

	// Create index
	_, err = CreateDictionaryIndexFile(tempDir, fileName, blockSize, fileLength)
	if err != nil {
		t.Fatalf("CreateDictionaryIndexFile failed: %v", err)
	}

	// Open index
	idx, err := OpenDictionaryIndex(tempDir, fileName)
	if err != nil {
		t.Fatalf("OpenDictionaryIndex failed: %v", err)
	}
	defer idx.Close()

	// Insert 100 entries
	numEntries := 100
	entries := make([]*IndexEntry128, numEntries)

	for i := 0; i < numEntries; i++ {
		entries[i] = &IndexEntry128{
			Hash:        Hash128{High: uint64(i * 1000), Low: uint64(i)},
			DictID:      uint32(i + 1),
			PostingsRef: uint32((i + 1) * 10),
		}
		err = idx.Insert(entries[i])
		if err != nil {
			t.Fatalf("Insert failed for entry %d: %v", i, err)
		}
	}

	t.Logf("Successfully inserted %d entries", numEntries)

	// Verify all entries can be found
	for i, entry := range entries {
		dictID, postingsRef, err := idx.Find(entry.Hash)
		if err != nil {
			t.Fatalf("Find failed for entry %d: %v", i, err)
		}
		if dictID != entry.DictID {
			t.Errorf("Entry %d: DictID = %d, want %d", i, dictID, entry.DictID)
		}
		if postingsRef != entry.PostingsRef {
			t.Errorf("Entry %d: PostingsRef = %d, want %d", i, postingsRef, entry.PostingsRef)
		}
	}

	// Test deleting some entries
	deleteIndices := []int{10, 25, 50, 75, 90}
	for _, i := range deleteIndices {
		err = idx.Delete(entries[i].Hash)
		if err != nil {
			t.Fatalf("Delete failed for entry %d: %v", i, err)
		}
	}

	// Verify deleted entries are gone
	for _, i := range deleteIndices {
		_, _, err = idx.Find(entries[i].Hash)
		if err != ErrIndexEntryNotFound {
			t.Errorf("Entry %d should be deleted, but Find returned: %v", i, err)
		}
	}

	// Verify remaining entries still exist
	for i, entry := range entries {
		isDeleted := false
		for _, di := range deleteIndices {
			if i == di {
				isDeleted = true
				break
			}
		}
		if isDeleted {
			continue
		}

		dictID, postingsRef, err := idx.Find(entry.Hash)
		if err != nil {
			t.Fatalf("Find failed for entry %d after deletes: %v", i, err)
		}
		if dictID != entry.DictID || postingsRef != entry.PostingsRef {
			t.Errorf("Entry %d mismatch after deletes", i)
		}
	}
}
