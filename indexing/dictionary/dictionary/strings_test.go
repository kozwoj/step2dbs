package dictionary

import (
	"path/filepath"
	"github.com/kozwoj/indexing/dictionary/postings"
	"testing"
)

func TestAddAndRetrieveString(t *testing.T) {
	// Create temp directory for test files
	tempDir := t.TempDir()
	stringsPath := filepath.Join(tempDir, "strings.dat")
	offsetsPath := filepath.Join(tempDir, "offsets.dat")
	postingsPath := filepath.Join(tempDir, "postings.dat")

	// Create strings and offsets files
	stringsFile, err := CreateStringsFile(stringsPath)
	if err != nil {
		t.Fatalf("Failed to create strings file: %v", err)
	}
	defer stringsFile.Close()

	offsetsFile, err := CreateOffsetsFile(offsetsPath)
	if err != nil {
		t.Fatalf("Failed to create offsets file: %v", err)
	}
	defer offsetsFile.Close()

	// Create postings file
	postingsFile, err := postings.CreatePostingsFile(postingsPath, 512, 10, postings.FormatSlice)
	if err != nil {
		t.Fatalf("Failed to create postings file: %v", err)
	}
	defer postingsFile.Close()

	// Read postings header
	postingsFile2, postingsHeader, err := postings.OpenPostingsFile(postingsPath)
	if err != nil {
		t.Fatalf("Failed to open postings file: %v", err)
	}
	postingsFile2.Close() // close the extra handle

	// Test adding a single string
	testStr := "hello world"
	dictID, postingsRef, err := AddStringEntry(stringsFile, offsetsFile, postingsFile, postingsHeader, testStr)
	if err != nil {
		t.Fatalf("Failed to add string: %v", err)
	}

	if dictID != 0 {
		t.Errorf("Expected dictID 0, got %d", dictID)
	}

	if postingsRef == postings.NoBlock {
		t.Errorf("Expected valid postingsRef, got NoBlock")
	}

	// Retrieve the string
	retrieved, err := RetrieveStringEntry(stringsFile, offsetsFile, dictID)
	if err != nil {
		t.Fatalf("Failed to retrieve string: %v", err)
	}

	if retrieved != testStr {
		t.Errorf("Expected '%s', got '%s'", testStr, retrieved)
	}
}

func TestAddAndRetrieveMultipleStrings(t *testing.T) {
	// Create temp directory for test files
	tempDir := t.TempDir()
	stringsPath := filepath.Join(tempDir, "strings.dat")
	offsetsPath := filepath.Join(tempDir, "offsets.dat")
	postingsPath := filepath.Join(tempDir, "postings.dat")

	// Create strings and offsets files
	stringsFile, err := CreateStringsFile(stringsPath)
	if err != nil {
		t.Fatalf("Failed to create strings file: %v", err)
	}
	defer stringsFile.Close()

	offsetsFile, err := CreateOffsetsFile(offsetsPath)
	if err != nil {
		t.Fatalf("Failed to create offsets file: %v", err)
	}
	defer offsetsFile.Close()

	// Create postings file
	postingsFile, err := postings.CreatePostingsFile(postingsPath, 512, 10, postings.FormatSlice)
	if err != nil {
		t.Fatalf("Failed to create postings file: %v", err)
	}
	defer postingsFile.Close()

	// Read postings header
	postingsFile2, postingsHeader, err := postings.OpenPostingsFile(postingsPath)
	if err != nil {
		t.Fatalf("Failed to open postings file: %v", err)
	}
	postingsFile2.Close() // close the extra handle

	// Test data
	testStrings := []string{
		"first",
		"second string",
		"third one here",
		"fourth",
		"",
		"sixth with special chars !@#$%",
	}

	// Add all strings
	var dictIDs []uint32
	for i, str := range testStrings {
		dictID, postingsRef, err := AddStringEntry(stringsFile, offsetsFile, postingsFile, postingsHeader, str)
		if err != nil {
			t.Fatalf("Failed to add string %d: %v", i, err)
		}
		if dictID != uint32(i) {
			t.Errorf("Expected dictID %d, got %d", i, dictID)
		}
		if postingsRef == postings.NoBlock {
			t.Errorf("Expected valid postingsRef for string %d, got NoBlock", i)
		}
		dictIDs = append(dictIDs, dictID)
	}

	// Retrieve all strings and verify
	for i, expectedStr := range testStrings {
		retrieved, err := RetrieveStringEntry(stringsFile, offsetsFile, dictIDs[i])
		if err != nil {
			t.Fatalf("Failed to retrieve string %d: %v", i, err)
		}
		if retrieved != expectedStr {
			t.Errorf("String %d: expected '%s', got '%s'", i, expectedStr, retrieved)
		}
	}
}

func TestOpenExistingStringsFile(t *testing.T) {
	// Create temp directory for test files
	tempDir := t.TempDir()
	stringsPath := filepath.Join(tempDir, "strings.dat")
	offsetsPath := filepath.Join(tempDir, "offsets.dat")
	postingsPath := filepath.Join(tempDir, "postings.dat")

	// Create and populate files
	{
		stringsFile, err := CreateStringsFile(stringsPath)
		if err != nil {
			t.Fatalf("Failed to create strings file: %v", err)
		}
		offsetsFile, err := CreateOffsetsFile(offsetsPath)
		if err != nil {
			stringsFile.Close()
			t.Fatalf("Failed to create offsets file: %v", err)
		}
		postingsFile, err := postings.CreatePostingsFile(postingsPath, 512, 10, postings.FormatSlice)
		if err != nil {
			stringsFile.Close()
			offsetsFile.Close()
			t.Fatalf("Failed to create postings file: %v", err)
		}
		// Read postings header
		postingsFile2, postingsHeader, err := postings.OpenPostingsFile(postingsPath)
		if err != nil {
			stringsFile.Close()
			offsetsFile.Close()
			postingsFile.Close()
			t.Fatalf("Failed to open postings file: %v", err)
		}
		postingsFile2.Close() // close the extra handle

		// Add some strings
		testStrings := []string{"alpha", "beta", "gamma"}
		for _, str := range testStrings {
			_, _, err := AddStringEntry(stringsFile, offsetsFile, postingsFile, postingsHeader, str)
			if err != nil {
				t.Fatalf("Failed to add string: %v", err)
			}
		}
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
	}

	// Open existing files
	stringsFile, header, err := OpenStringsFile(stringsPath)
	if err != nil {
		t.Fatalf("Failed to open strings file: %v", err)
	}
	defer stringsFile.Close()

	if header.NumOfStrings != 3 {
		t.Errorf("Expected 3 strings, got %d", header.NumOfStrings)
	}

	offsetsFile, err := OpenOffsetsFile(offsetsPath)
	if err != nil {
		t.Fatalf("Failed to open offsets file: %v", err)
	}
	defer offsetsFile.Close()

	// Verify we can still retrieve strings
	retrieved, err := RetrieveStringEntry(stringsFile, offsetsFile, 1)
	if err != nil {
		t.Fatalf("Failed to retrieve string: %v", err)
	}
	if retrieved != "beta" {
		t.Errorf("Expected 'beta', got '%s'", retrieved)
	}
}

func TestAddStringAndPostingsRecords(t *testing.T) {
	// Create temp directory for test files
	tempDir := t.TempDir()
	stringsPath := filepath.Join(tempDir, "strings.dat")
	offsetsPath := filepath.Join(tempDir, "offsets.dat")
	postingsPath := filepath.Join(tempDir, "postings.dat")

	// Create all files
	stringsFile, err := CreateStringsFile(stringsPath)
	if err != nil {
		t.Fatalf("Failed to create strings file: %v", err)
	}
	defer stringsFile.Close()

	offsetsFile, err := CreateOffsetsFile(offsetsPath)
	if err != nil {
		t.Fatalf("Failed to create offsets file: %v", err)
	}
	defer offsetsFile.Close()

	postingsFile, err := postings.CreatePostingsFile(postingsPath, 512, 10, postings.FormatSlice)
	if err != nil {
		t.Fatalf("Failed to create postings file: %v", err)
	}
	defer postingsFile.Close()

	// Read postings header
	postingsFile2, postingsHeader, err := postings.OpenPostingsFile(postingsPath)
	if err != nil {
		t.Fatalf("Failed to open postings file: %v", err)
	}
	postingsFile2.Close()

	// Add a string (this creates an empty postings list)
	testStr := "example"
	dictID, postingsRef, err := AddStringEntry(stringsFile, offsetsFile, postingsFile, postingsHeader, testStr)
	if err != nil {
		t.Fatalf("Failed to add string: %v", err)
	}

	if postingsRef == postings.NoBlock {
		t.Fatalf("Expected valid postingsRef, got NoBlock")
	}

	// Create PostingsList instance
	postingsList := postings.NewPostingsList(postingsHeader.Format, postingsHeader.BlockSize)

	// Verify the postings list is initially empty
	recordIDs, blockNumbers, err := postingsList.GetRecordsList(postingsFile, postingsRef)
	if err != nil {
		t.Fatalf("Failed to get initial records list: %v", err)
	}
	if len(recordIDs) != 0 {
		t.Errorf("Expected empty postings list, got %d records", len(recordIDs))
	}
	if len(blockNumbers) != 1 {
		t.Errorf("Expected 1 block for empty list, got %d blocks", len(blockNumbers))
	}

	// Add record IDs to the postings list
	newRecordIDs := []uint32{100, 200, 300, 400, 500}
	err = postingsList.WriteBackRecordsList(postingsFile, postingsHeader, blockNumbers, newRecordIDs, dictID)
	if err != nil {
		t.Fatalf("Failed to write back records list: %v", err)
	}

	// Retrieve and verify the record IDs
	retrievedRecordIDs, _, err := postingsList.GetRecordsList(postingsFile, postingsRef)
	if err != nil {
		t.Fatalf("Failed to get records list after update: %v", err)
	}

	if len(retrievedRecordIDs) != len(newRecordIDs) {
		t.Errorf("Expected %d records, got %d", len(newRecordIDs), len(retrievedRecordIDs))
	}

	for i, recordID := range newRecordIDs {
		if retrievedRecordIDs[i] != recordID {
			t.Errorf("Record %d: expected %d, got %d", i, recordID, retrievedRecordIDs[i])
		}
	}

	// Verify the string is still retrievable
	retrievedStr, err := RetrieveStringEntry(stringsFile, offsetsFile, dictID)
	if err != nil {
		t.Fatalf("Failed to retrieve string: %v", err)
	}
	if retrievedStr != testStr {
		t.Errorf("Expected string '%s', got '%s'", testStr, retrievedStr)
	}
}
