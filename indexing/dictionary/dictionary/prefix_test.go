package dictionary

import (
	"os"
	"github.com/kozwoj/indexing/primindex"
	"testing"
)

// Test constants for prefix index
const (
	testPrefixBlockSize   = 1024
	testPrefixInitialBlocks = 10
)

func TestCreatePrefixIndex(t *testing.T) {
	dirPath := "test_data/test_prefix_create"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	err = CreatePrefixIndex(dirPath, testPrefixBlockSize, testPrefixInitialBlocks)
	if err != nil {
		t.Fatalf("CreatePrefixIndex failed: %v", err)
	}

	// Verify the index file was created
	prefixPath := dirPath + "\\prefix.dat"
	if _, err := os.Stat(prefixPath); os.IsNotExist(err) {
		t.Fatalf("prefix.dat file was not created")
	}
}

func TestOpenPrefixIndex(t *testing.T) {
	dirPath := "test_data\\test_prefix_open"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create the prefix index file first
	err = CreatePrefixIndex(dirPath, testPrefixBlockSize, testPrefixInitialBlocks)
	if err != nil {
		t.Fatalf("CreatePrefixIndex failed: %v", err)
	}

	// Now open the created index
	idx, err := OpenPrefixIndex(dirPath)
	if err != nil {
		t.Fatalf("OpenPrefixIndex failed: %v", err)
	}
	defer idx.Close()

	// Verify the index object is not nil
	if idx == nil {
		t.Fatal("OpenPrefixIndex returned nil index")
	}
}

func TestOpenPrefixIndexNotExists(t *testing.T) {
	dirPath := "test_data\\test_prefix_not_exists"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Try to open non-existent prefix index
	_, err = OpenPrefixIndex(dirPath)
	if err == nil {
		t.Fatal("Expected error when opening non-existent prefix index, got nil")
	}

	if err != ErrFileOpen {
		t.Errorf("Expected ErrPrefixFileOpen, got: %v", err)
	}
}

func TestAddPrefixEntry(t *testing.T) {
	dirPath := "test_data\\test_add_prefix_entry"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create a prefix index
	err = CreatePrefixIndex(dirPath, testPrefixBlockSize, testPrefixInitialBlocks)
	if err != nil {
		t.Fatalf("CreatePrefixIndex failed: %v", err)
	}

	// Open the prefix index
	idx, err := OpenPrefixIndex(dirPath)
	if err != nil {
		t.Fatalf("OpenPrefixIndex failed: %v", err)
	}
	defer idx.Close()

	// Simulate dictionary with map: dictID -> lastName
	lastNames := map[uint32]string{
		1:  "Smith",
		2:  "Johnson",
		3:  "Williams",
		4:  "Brown",
		5:  "Jones",
		6:  "Garcia",
		7:  "Miller",
		8:  "Davis",
		9:  "Rodriguez",
		10: "Martinez",
	}

	// Add prefix entries for all last names
	for dictID, lastName := range lastNames {
		err := AddPrefixEntry(idx, lastName, dictID)
		if err != nil {
			t.Fatalf("AddPrefixEntry failed for '%s' (dictID=%d): %v", lastName, dictID, err)
		}
	}

	// Verify entries were added by searching for them
	for dictID, lastName := range lastNames {
		key := primindex.BuildPrefixKey(lastName, dictID, prefixLength)
		if err != nil {
			t.Fatalf("Failed to serialize key '%s': %v", lastName, err)
		}
		_, err = idx.Find(key)
		if err != nil {
			t.Fatalf("Find failed for '%s' (dictID=%d): %v", lastName, dictID, err)
		}

	}
}

func TestAddPrefixEntryEdgeCases(t *testing.T) {
	dirPath := "test_data\\test_prefix_edge_cases"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create and open prefix index
	err = CreatePrefixIndex(dirPath, testPrefixBlockSize, testPrefixInitialBlocks)
	if err != nil {
		t.Fatalf("CreatePrefixIndex failed: %v", err)
	}

	idx, err := OpenPrefixIndex(dirPath)
	if err != nil {
		t.Fatalf("OpenPrefixIndex failed: %v", err)
	}
	defer idx.Close()

	// Test edge cases
	edgeCases := map[uint32]string{
		1: "a",                // 1 byte - shorter than prefix
		2: "ab",               // 2 bytes
		3: "Smith",            // 5 bytes
		4: "VeryLongLastName", // longer than 8 bytes
		5: "O'Brien",          // special character
		6: "",                 // empty string
	}

	// Add all edge cases
	for dictID, str := range edgeCases {
		err := AddPrefixEntry(idx, str, dictID)
		if err != nil {
			t.Fatalf("AddPrefixEntry failed for '%s' (dictID=%d): %v", str, dictID, err)
		}
	}

	// Verify all were inserted
	for dictID, str := range edgeCases {
		key := primindex.BuildPrefixKey(str, dictID, prefixLength)
		_, err := idx.Find(key)
		if err != nil {
			t.Fatalf("Find failed for '%s' (dictID=%d): %v", str, dictID, err)
		}
	}
}

func TestPrefixSearch(t *testing.T) {
	dirPath := "test_data\\test_prefix_search"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create and open prefix index
	err = CreatePrefixIndex(dirPath, testPrefixBlockSize, testPrefixInitialBlocks)
	if err != nil {
		t.Fatalf("CreatePrefixIndex failed: %v", err)
	}

	idx, err := OpenPrefixIndex(dirPath)
	if err != nil {
		t.Fatalf("OpenPrefixIndex failed: %v", err)
	}
	defer idx.Close()

	// Test data: map of dictID -> string
	// Designed to test various prefix scenarios
	testStrings := map[uint32]string{
		// "cust" prefix group (4 matches)
		1: "customer",
		2: "customers",
		3: "customization",
		4: "custom",

		// "comp" prefix group (3 matches)
		5: "company",
		6: "computer",
		7: "complete",

		// "con" prefix group (2 matches)
		8: "contact",
		9: "contract",

		// Single character prefixes
		10: "a",
		11: "apple",
		12: "application",

		// No shared prefix
		13: "zebra",
		14: "yellow",

		// Empty string edge case
		15: "",

		// Very long strings (>8 bytes)
		16: "international",
		17: "internationalization",
		18: "internet",

		// Special characters
		19: "O'Brien",
		20: "O'Connor",

		// Strings with spaces
		21: "New York",
		22: "New Jersey",
		23: "New Mexico",
	}

	// Add all test strings to prefix index
	for dictID, str := range testStrings {
		err := AddPrefixEntry(idx, str, dictID)
		if err != nil {
			t.Fatalf("AddPrefixEntry failed for '%s' (dictID=%d): %v", str, dictID, err)
		}
	}

	// Test cases: prefix -> expected dictIDs
	testCases := []struct {
		prefix      string
		expectedIDs []uint32
		description string
	}{
		{
			prefix:      "cust",
			expectedIDs: []uint32{1, 2, 3, 4},
			description: "4 strings starting with 'cust'",
		},
		{
			prefix:      "customer",
			expectedIDs: []uint32{1, 2}, // customer, customers
			description: "2 strings starting with 'customer'",
		},
		{
			prefix:      "comp",
			expectedIDs: []uint32{5, 6, 7},
			description: "3 strings starting with 'comp'",
		},
		{
			prefix:      "con",
			expectedIDs: []uint32{8, 9},
			description: "2 strings starting with 'con'",
		},
		{
			prefix:      "a",
			expectedIDs: []uint32{10, 11, 12},
			description: "3 strings starting with 'a'",
		},
		{
			prefix:      "app",
			expectedIDs: []uint32{11, 12},
			description: "2 strings starting with 'app'",
		},
		{
			prefix:      "inter",
			expectedIDs: []uint32{16, 17, 18},
			description: "3 strings starting with 'inter'",
		},
		{
			prefix:      "international",
			expectedIDs: []uint32{16, 17}, // international, internationalization
			description: "2 strings starting with 'international'",
		},
		{
			prefix:      "O'",
			expectedIDs: []uint32{19, 20},
			description: "2 strings starting with O'",
		},
		{
			prefix:      "New ",
			expectedIDs: []uint32{21, 22, 23},
			description: "3 strings starting with 'New '",
		},
		{
			prefix:      "xyz",
			expectedIDs: []uint32{},
			description: "No strings starting with 'xyz'",
		},
		{
			prefix:      "",
			expectedIDs: []uint32{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23},
			description: "Empty prefix matches all strings",
		},
	}

	// Run test cases
	for _, tc := range testCases {
		t.Run(tc.description, func(t *testing.T) {
			results, err := PrefixSearch(idx, tc.prefix)
			if err != nil {
				t.Fatalf("PrefixSearch failed for prefix '%s': %v", tc.prefix, err)
			}

			// Convert results to map for easy comparison
			resultMap := make(map[uint32]bool)
			for _, id := range results {
				resultMap[id] = true
			}

			// Check all expected IDs are present
			for _, expectedID := range tc.expectedIDs {
				if !resultMap[expectedID] {
					t.Errorf("Expected dictID %d (string='%s') not found in results for prefix '%s'",
						expectedID, testStrings[expectedID], tc.prefix)
				}
			}

			// Check no unexpected IDs are present
			if len(results) != len(tc.expectedIDs) {
				t.Errorf("Expected %d results for prefix '%s', got %d: %v",
					len(tc.expectedIDs), tc.prefix, len(results), results)
			}

			// Verify each result actually starts with the prefix
			for _, dictID := range results {
				str := testStrings[dictID]
				if len(str) < len(tc.prefix) || str[:len(tc.prefix)] != tc.prefix {
					t.Errorf("Result dictID %d (string='%s') does not start with prefix '%s'",
						dictID, str, tc.prefix)
				}
			}
		})
	}
}

func TestPrefixSearchDebug(t *testing.T) {
	dirPath := "test_data\\test_prefix_debug"
	os.RemoveAll(dirPath)
	defer os.RemoveAll(dirPath)

	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	err = CreatePrefixIndex(dirPath, testPrefixBlockSize, testPrefixInitialBlocks)
	if err != nil {
		t.Fatalf("CreatePrefixIndex failed: %v", err)
	}

	idx, err := OpenPrefixIndex(dirPath)
	if err != nil {
		t.Fatalf("OpenPrefixIndex failed: %v", err)
	}
	defer idx.Close()

	// Add just a few entries
	testStrings := map[uint32]string{
		1: "customer",
		2: "custom",
	}

	for dictID, str := range testStrings {
		err := AddPrefixEntry(idx, str, dictID)
		if err != nil {
			t.Fatalf("AddPrefixEntry failed: %v", err)
		}
		t.Logf("Added: dictID=%d, str='%s'", dictID, str)
	}

	// Verify entries exist using Find
	for dictID, str := range testStrings {
		key := primindex.BuildPrefixKey(str, dictID, prefixLength)
		_, err := idx.Find(key)
		if err != nil {
			t.Errorf("Find failed for dictID=%d, str='%s': %v", dictID, str, err)
		} else {
			t.Logf("Find succeeded for dictID=%d, str='%s'", dictID, str)
		}
	}

	// Now try PrefixSearch
	prefix := "cust"
	t.Logf("Searching for prefix: '%s'", prefix)

	results, err := PrefixSearch(idx, prefix)
	if err != nil {
		t.Fatalf("PrefixSearch failed: %v", err)
	}

	t.Logf("PrefixSearch results: %v", results)

	if len(results) != 2 {
		t.Errorf("Expected 2 results, got %d", len(results))
	}
}
