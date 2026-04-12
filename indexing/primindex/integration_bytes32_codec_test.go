package primindex

import (
	"encoding/binary"
	"fmt"
	"testing"
)

// makeStringKey creates a 32-byte key from a string, left-padded with zeros
func makeStringKey(s string) []byte {
	key := make([]byte, 32)
	copy(key, []byte(s))
	return key
}

// TestBytes32KeyIssue tests 32-byte string keys (usernames, product codes, etc.)
// Verifies that Find works after Insert, especially after tree splits
func TestBytes32KeyIssue(t *testing.T) {
	tempDir := t.TempDir()

	// Create index with 32-byte keys
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeBytes32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	// Calculate how many entries fit per leaf
	// Block size: 512 bytes
	// Leaf header: 9 bytes
	// Entry size: 32 (key) + 8 (value) = 40 bytes
	// Available: 512 - 9 = 503 bytes
	// Max entries: 503 / 40 = 12 entries per leaf
	t.Log("With 32-byte keys + 8-byte values:")
	t.Log("  Entry size: 40 bytes")
	t.Log("  Max entries per leaf: 12")
	t.Log("  First split at entry 13")
	t.Log("  Second split at entry 25 (13 in second leaf)")

	// Realistic string keys - product codes, user IDs, etc.
	keys := []string{
		"PROD-2024-001-ALPHA",
		"PROD-2024-002-BETA",
		"PROD-2024-003-GAMMA",
		"PROD-2024-004-DELTA",
		"PROD-2024-005-EPSILON",
		"PROD-2024-006-ZETA",
		"PROD-2024-007-ETA",
		"PROD-2024-008-THETA",
		"PROD-2024-009-IOTA",
		"PROD-2024-010-KAPPA",
		"PROD-2024-011-LAMBDA",
		"PROD-2024-012-MU",
		"PROD-2024-013-NU", // Triggers first split
		"PROD-2024-014-XI",
		"PROD-2024-015-OMICRON",
		"PROD-2024-016-PI",
		"PROD-2024-017-RHO",
		"PROD-2024-018-SIGMA", // Second split point
		"PROD-2024-019-TAU",
		"PROD-2024-020-UPSILON",
	}

	value := make([]byte, 8)

	// Insert entries one by one and check after each insert
	for i := 0; i < len(keys); i++ {
		key := makeStringKey(keys[i])
		binary.LittleEndian.PutUint64(value, uint64(i*100))

		t.Logf("\n=== Inserting key %d: %s ===", i, keys[i])
		err = idx.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d (%s): %v", i, keys[i], err)
		}

		// Immediately try to find it
		found, err := idx.Find(key)
		if err != nil {
			t.Errorf("CRITICAL: Find failed immediately after insert for key %d (%s): %v", i, keys[i], err)

			// Print detailed tree structure
			PrintTreeStructure(t, idx, "After failed insert")

			// Try to understand what happened
			t.Logf("\nAttempting to debug key %d: %s", i, keys[i])
			t.Logf("Key bytes (first 20): %s", string(key[:20]))

			// Try to manually traverse and find where it should be
			leafBlock, path, err := idx.FindLeafBlock(key)
			if err != nil {
				t.Logf("FindLeafBlock failed: %v", err)
			} else {
				t.Logf("FindLeafBlock returned: leafBlock=%d, path=%v", leafBlock, path)

				// Read the leaf
				leafData, err := ReadIndexBlock(idx, int(leafBlock))
				if err != nil {
					t.Logf("Failed to read leaf block %d: %v", leafBlock, err)
				} else {
					leaf, err := DeserializeLeafNode(leafData, idx.Codec, 8)
					if err != nil {
						t.Logf("Failed to deserialize leaf: %v", err)
					} else {
						t.Logf("Leaf has %d entries", leaf.EntryCount)
						for j := 0; j < int(leaf.EntryCount); j++ {
							entryKey := leaf.Entries[j].Key.([]byte)
							keyStr := string(entryKey[:20])
							t.Logf("  Entry[%d] key: %s", j, keyStr)
						}
					}
				}
			}

			t.FailNow()
		}

		// Verify the value is correct
		expectedValue := uint64(i * 100)
		actualValue := binary.LittleEndian.Uint64(found)
		if actualValue != expectedValue {
			t.Errorf("Value mismatch for key %d (%s): expected %d, got %d", i, keys[i], expectedValue, actualValue)
		}

		// Log progress at critical points
		if i == 12 {
			t.Log("\n=== After first split (key 12) ===")
			PrintTreeStructure(t, idx, "After first split")
		} else if i == 17 {
			t.Log("\n=== Before problematic insert (key 17) ===")
			PrintTreeStructure(t, idx, "Before key 18")
		}
	}

	t.Log("\n=== Final tree structure ===")
	PrintTreeStructure(t, idx, "After all inserts")

	// Verify all keys are still findable
	t.Log("\nVerifying all keys are findable:")
	for i := 0; i < len(keys); i++ {
		key := makeStringKey(keys[i])

		_, err := idx.Find(key)
		if err != nil {
			t.Errorf("Key %d (%s) not found: %v", i, keys[i], err)
		}
	}
}

// TestBytes32KeySimplified is a minimal reproduction case with string keys
func TestBytes32KeySimplified(t *testing.T) {
	tempDir := t.TempDir()

	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeBytes32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Generate 18 user IDs (to test second split point)
	keys := make([]string, 18)
	for i := 0; i < 18; i++ {
		keys[i] = fmt.Sprintf("user-%04d@example.com", i+1)
	}

	// Insert exactly 18 entries (to trigger the second split)
	t.Log("Inserting 18 user ID entries...")
	for i := 0; i < 18; i++ {
		key := makeStringKey(keys[i])
		binary.LittleEndian.PutUint64(value, uint64(i*100))

		err = idx.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d (%s): %v", i, keys[i], err)
		}
	}

	PrintTreeStructure(t, idx, "After 18 inserts")

	// Try to find each key
	t.Log("\nVerifying keys...")
	for i := 0; i < 18; i++ {
		key := makeStringKey(keys[i])

		_, err := idx.Find(key)
		if err != nil {
			t.Errorf("Key %d (%s) not found: %v", i, keys[i], err)
		} else {
			t.Logf("Key %d (%s) found OK", i, keys[i])
		}
	}
}
