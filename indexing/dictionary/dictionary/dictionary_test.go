package dictionary

import (
	"testing"
)

// Test constants for block sizes
var testBlockSizes = DictionaryBlockSizes{
	PostingsBlockSize: 512,
	IndexBlockSize:    512,
	PrefixBlockSize:   512,
}

const testInitialBlocks = 10

func TestCreateAndOpenDictionary(t *testing.T) {
	// Create temp directory for test files
	tempDir := t.TempDir()
	dictName := "testdict"

	// Create dictionary
	dict, err := CreateDictionary(tempDir, dictName, testBlockSizes, testInitialBlocks)
	if err != nil {
		t.Fatalf("Failed to create dictionary: %v", err)
	}

	// Verify dictionary was created
	if dict.DirPath != tempDir {
		t.Errorf("Expected DirPath %s, got %s", tempDir, dict.DirPath)
	}
	if dict.Name != dictName {
		t.Errorf("Expected Name %s, got %s", dictName, dict.Name)
	}
	if dict.Index == nil {
		t.Error("Expected Index to be initialized")
	}
	if dict.StringsFile == nil {
		t.Error("Expected StringsFile to be initialized")
	}
	if dict.OffsetsFile == nil {
		t.Error("Expected OffsetsFile to be initialized")
	}
	if dict.PostingsFile == nil {
		t.Error("Expected PostingsFile to be initialized")
	}

	// Close the dictionary
	err = dict.Close()
	if err != nil {
		t.Fatalf("Failed to close dictionary: %v", err)
	}

	// Open the dictionary again
	dict2, err := OpenDictionary(tempDir, dictName)
	if err != nil {
		t.Fatalf("Failed to open dictionary: %v", err)
	}
	defer dict2.Close()

	// Verify opened dictionary
	if dict2.DirPath != tempDir {
		t.Errorf("Expected DirPath %s, got %s", tempDir, dict2.DirPath)
	}
	if dict2.Name != dictName {
		t.Errorf("Expected Name %s, got %s", dictName, dict2.Name)
	}
	if dict2.Index == nil {
		t.Error("Expected Index to be initialized")
	}
}

func TestAddStringAndDuplicate(t *testing.T) {
	// Create temp directory and dictionary
	tempDir := t.TempDir()
	dictName := "testdict"

	dict, err := CreateDictionary(tempDir, dictName, testBlockSizes, testInitialBlocks)
	if err != nil {
		t.Fatalf("Failed to create dictionary: %v", err)
	}
	defer dict.Close()

	// Add a string
	testStr := "hello world"
	dictID1, postingsRef1, err := dict.AddString(testStr)
	if err != nil {
		t.Fatalf("Failed to add string: %v", err)
	}

	// Verify dictID and postingsRef are valid
	if dictID1 != 0 {
		t.Errorf("Expected first dictID to be 0, got %d", dictID1)
	}
	if postingsRef1 == 0xFFFFFFFF {
		t.Error("Expected valid postingsRef, got NoBlock")
	}

	// Add the same string again
	dictID2, postingsRef2, err := dict.AddString(testStr)
	if err != nil {
		t.Fatalf("Expected no error when adding duplicate string, got: %v", err)
	}

	// Verify that the same IDs are returned
	if dictID2 != dictID1 {
		t.Errorf("Expected duplicate string to return same dictID %d, got %d", dictID1, dictID2)
	}
	if postingsRef2 != postingsRef1 {
		t.Errorf("Expected duplicate string to return same postingsRef %d, got %d", postingsRef1, postingsRef2)
	}

	// Verify we can retrieve the string
	retrieved, err := dict.GetStringByID(dictID1)
	if err != nil {
		t.Fatalf("Failed to retrieve string: %v", err)
	}
	if retrieved != testStr {
		t.Errorf("Expected string '%s', got '%s'", testStr, retrieved)
	}
}

func TestAddRecordID(t *testing.T) {
	// Create temp directory and dictionary
	tempDir := t.TempDir()
	dictName := "testdict"

	dict, err := CreateDictionary(tempDir, dictName, testBlockSizes, testInitialBlocks)
	if err != nil {
		t.Fatalf("Failed to create dictionary: %v", err)
	}
	defer dict.Close()

	// Add a string to get a postingsRef and dictID
	testStr := "test string"
	dictID, postingsRef, err := dict.AddString(testStr)
	if err != nil {
		t.Fatalf("Failed to add string: %v", err)
	}

	// Add first record ID
	recordID1 := uint32(100)
	err = dict.AddRecordID(postingsRef, recordID1, dictID)
	if err != nil {
		t.Fatalf("Failed to add first record ID: %v", err)
	}

	// Verify the record was added
	recordIDs, err := dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to get records list: %v", err)
	}
	if len(recordIDs) != 1 {
		t.Errorf("Expected 1 record ID, got %d", len(recordIDs))
	}
	if recordIDs[0] != recordID1 {
		t.Errorf("Expected record ID %d, got %d", recordID1, recordIDs[0])
	}

	// Add second record ID
	recordID2 := uint32(200)
	err = dict.AddRecordID(postingsRef, recordID2, dictID)
	if err != nil {
		t.Fatalf("Failed to add second record ID: %v", err)
	}

	// Verify both records are present
	recordIDs, err = dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to get records list after second add: %v", err)
	}
	if len(recordIDs) != 2 {
		t.Errorf("Expected 2 record IDs, got %d", len(recordIDs))
	}
	if recordIDs[0] != recordID1 {
		t.Errorf("Expected first record ID %d, got %d", recordID1, recordIDs[0])
	}
	if recordIDs[1] != recordID2 {
		t.Errorf("Expected second record ID %d, got %d", recordID2, recordIDs[1])
	}

	// Test adding duplicate record ID (should be idempotent)
	err = dict.AddRecordID(postingsRef, recordID1, dictID)
	if err != nil {
		t.Fatalf("Failed to add duplicate record ID: %v", err)
	}

	// Verify still have 2 records (duplicate wasn't added)
	recordIDs, err = dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to get records list after duplicate add: %v", err)
	}
	if len(recordIDs) != 2 {
		t.Errorf("Expected 2 record IDs after duplicate add, got %d", len(recordIDs))
	}
}

func TestRemoveRecordID(t *testing.T) {
	// Create temp directory and dictionary
	tempDir := t.TempDir()
	dictName := "testdict"

	dict, err := CreateDictionary(tempDir, dictName, testBlockSizes, testInitialBlocks)
	if err != nil {
		t.Fatalf("Failed to create dictionary: %v", err)
	}
	defer dict.Close()

	// Add a string to get a postingsRef and dictID
	testStr := "test string"
	dictID, postingsRef, err := dict.AddString(testStr)
	if err != nil {
		t.Fatalf("Failed to add string: %v", err)
	}

	// Add multiple record IDs
	recordIDs := []uint32{100, 200, 300, 400, 500}
	for _, recordID := range recordIDs {
		err = dict.AddRecordID(postingsRef, recordID, dictID)
		if err != nil {
			t.Fatalf("Failed to add record ID %d: %v", recordID, err)
		}
	}

	// Verify all records were added
	retrievedIDs, err := dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to retrieve postings: %v", err)
	}
	if len(retrievedIDs) != len(recordIDs) {
		t.Fatalf("Expected %d record IDs, got %d", len(recordIDs), len(retrievedIDs))
	}

	// Remove middle record ID (300)
	err = dict.RemoveRecordID(postingsRef, 300, dictID)
	if err != nil {
		t.Fatalf("Failed to remove record ID 300: %v", err)
	}

	// Verify 300 was removed
	retrievedIDs, err = dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to retrieve postings after removal: %v", err)
	}
	if len(retrievedIDs) != 4 {
		t.Fatalf("Expected 4 record IDs after removal, got %d", len(retrievedIDs))
	}
	expected := []uint32{100, 200, 400, 500}
	for i, id := range expected {
		if retrievedIDs[i] != id {
			t.Errorf("At index %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}

	// Remove first record ID (100)
	err = dict.RemoveRecordID(postingsRef, 100, dictID)
	if err != nil {
		t.Fatalf("Failed to remove record ID 100: %v", err)
	}

	retrievedIDs, err = dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to retrieve postings after second removal: %v", err)
	}
	if len(retrievedIDs) != 3 {
		t.Fatalf("Expected 3 record IDs after second removal, got %d", len(retrievedIDs))
	}
	expected = []uint32{200, 400, 500}
	for i, id := range expected {
		if retrievedIDs[i] != id {
			t.Errorf("At index %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}

	// Remove last record ID (500)
	err = dict.RemoveRecordID(postingsRef, 500, dictID)
	if err != nil {
		t.Fatalf("Failed to remove record ID 500: %v", err)
	}

	retrievedIDs, err = dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to retrieve postings after third removal: %v", err)
	}
	if len(retrievedIDs) != 2 {
		t.Fatalf("Expected 2 record IDs after third removal, got %d", len(retrievedIDs))
	}
	expected = []uint32{200, 400}
	for i, id := range expected {
		if retrievedIDs[i] != id {
			t.Errorf("At index %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}

	// Try to remove non-existent record ID (should succeed - idempotent behavior)
	err = dict.RemoveRecordID(postingsRef, 999, dictID)
	if err != nil {
		t.Errorf("Expected no error when removing non-existent record ID (idempotent), got: %v", err)
	}

	// Try to remove with wrong dictID (should fail - integrity check)
	wrongDictID := uint32(9999)
	err = dict.RemoveRecordID(postingsRef, 200, wrongDictID)
	if err == nil {
		t.Error("Expected error when removing record with wrong dictID, got nil")
	}

	// Verify list is still intact after operations
	retrievedIDs, err = dict.RetrievePostings(postingsRef)
	if err != nil {
		t.Fatalf("Failed to retrieve postings after operations: %v", err)
	}
	if len(retrievedIDs) != 2 {
		t.Fatalf("Expected 2 record IDs after operations, got %d", len(retrievedIDs))
	}
}

// TestAddStringIdempotent tests that AddString is idempotent and can be safely retried
func TestAddStringIdempotent(t *testing.T) {
	// Create temp directory and dictionary
	tempDir := t.TempDir()
	dictName := "testdict"

	dict, err := CreateDictionary(tempDir, dictName, testBlockSizes, testInitialBlocks)
	if err != nil {
		t.Fatalf("Failed to create dictionary: %v", err)
	}
	defer dict.Close()

	// Add multiple strings
	testStrings := []string{
		"apple",
		"banana",
		"cherry",
		"date",
		"elderberry",
	}

	// Track original IDs
	originalDictIDs := make(map[string]uint32)
	originalPostingsRefs := make(map[string]uint32)

	// Add all strings first time
	for _, str := range testStrings {
		dictID, postingsRef, err := dict.AddString(str)
		if err != nil {
			t.Fatalf("Failed to add string '%s': %v", str, err)
		}
		originalDictIDs[str] = dictID
		originalPostingsRefs[str] = postingsRef
	}

	// Add all strings again multiple times (simulating retries)
	for retry := 0; retry < 3; retry++ {
		for _, str := range testStrings {
			dictID, postingsRef, err := dict.AddString(str)
			if err != nil {
				t.Errorf("Retry %d: Failed to add duplicate string '%s': %v", retry, str, err)
			}

			// Verify same IDs are returned
			if dictID != originalDictIDs[str] {
				t.Errorf("Retry %d: String '%s' returned different dictID: got %d, want %d",
					retry, str, dictID, originalDictIDs[str])
			}
			if postingsRef != originalPostingsRefs[str] {
				t.Errorf("Retry %d: String '%s' returned different postingsRef: got %d, want %d",
					retry, str, postingsRef, originalPostingsRefs[str])
			}
		}
	}

	// Verify all strings are still retrievable
	for str, dictID := range originalDictIDs {
		retrieved, err := dict.GetStringByID(dictID)
		if err != nil {
			t.Errorf("Failed to retrieve string for dictID %d: %v", dictID, err)
		}
		if retrieved != str {
			t.Errorf("Retrieved string mismatch: got '%s', want '%s'", retrieved, str)
		}
	}
}
