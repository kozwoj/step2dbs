package primindex

import (
	"encoding/binary"
	"testing"
)

// TestEmptyTreeOperations tests operations on empty tree
func TestEmptyTreeOperations(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	// Delete from empty tree (should return error)
	err = idx.Delete(uint32(100))
	if err == nil {
		t.Error("Delete from empty tree should return error")
	}

	// Search empty tree (should return error)
	_, err = idx.Find(uint32(100))
	if err == nil {
		t.Error("Find in empty tree should return error")
	}

	// Multiple deletes on empty tree
	for i := 0; i < 10; i++ {
		err = idx.Delete(uint32(i))
		if err == nil {
			t.Errorf("Delete %d from empty tree should return error", i)
		}
	}

	// Verify tree still valid
	ValidateTreeStructure(t, idx)

	// Insert one entry to verify tree still works
	value := make([]byte, 8)
	binary.LittleEndian.PutUint64(value, 42)
	err = idx.Insert(uint32(1), value)
	if err != nil {
		t.Errorf("Insert after empty operations failed: %v", err)
	}

	found, err := idx.Find(uint32(1))
	if err != nil {
		t.Errorf("Find after insert failed: %v", err)
	}
	if binary.LittleEndian.Uint64(found) != 42 {
		t.Errorf("Wrong value: got %d, want 42", binary.LittleEndian.Uint64(found))
	}
}

// TestSingleEntryLifecycle tests minimal tree operations
func TestSingleEntryLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Test with different keys
	testKeys := []uint32{0, 1, 100, 0xFFFFFFFF}

	for _, key := range testKeys {
		// Insert
		binary.LittleEndian.PutUint64(value, uint64(key)*10)
		err = idx.Insert(key, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", key, err)
		}

		// Verify found
		found, err := idx.Find(key)
		if err != nil {
			t.Errorf("Find failed for key %d: %v", key, err)
		}
		expectedValue := uint64(key) * 10
		if binary.LittleEndian.Uint64(found) != expectedValue {
			t.Errorf("Wrong value for key %d: got %d, want %d",
				key, binary.LittleEndian.Uint64(found), expectedValue)
		}

		// Delete
		err = idx.Delete(key)
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", key, err)
		}

		// Verify not found
		_, err = idx.Find(key)
		if err == nil {
			t.Errorf("Key %d should not be found after deletion", key)
		}

		// Validate empty state
		ValidateTreeStructure(t, idx)
	}
}

// TestDuplicateKeys tests behavior when inserting duplicate keys
func TestDuplicateKeys(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Insert a key
	binary.LittleEndian.PutUint64(value, 100)
	err = idx.Insert(uint32(42), value)
	if err != nil {
		t.Fatalf("Initial insert failed: %v", err)
	}

	// Try to insert same key again (should fail)
	binary.LittleEndian.PutUint64(value, 200)
	err = idx.Insert(uint32(42), value)
	if err == nil {
		t.Error("Duplicate key insert should return error")
	}

	// Verify original value unchanged
	found, err := idx.Find(uint32(42))
	if err != nil {
		t.Fatalf("Find failed: %v", err)
	}
	if binary.LittleEndian.Uint64(found) != 100 {
		t.Errorf("Value changed after duplicate insert: got %d, want 100",
			binary.LittleEndian.Uint64(found))
	}

	// Insert multiple entries, then try duplicate in middle
	for i := uint32(100); i < 200; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(i, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Try duplicate in middle of range
	err = idx.Insert(uint32(150), value)
	if err == nil {
		t.Error("Duplicate key insert in middle should return error")
	}

	ValidateTreeStructure(t, idx)
}

// TestMaximumCapacity tests tree behavior near capacity limits
func TestMaximumCapacity(t *testing.T) {
	tempDir := t.TempDir()

	// Use smaller block size and specific key/value sizes to control capacity
	blockSize := uint16(512)
	keySize := 4 // uint32
	valueSize := uint32(8)

	err := CreateIndexFile(tempDir, "testindex.indx", blockSize, 10, KeyTypeUint32, valueSize)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	// Calculate max entries per leaf
	// Leaf header: 9 bytes
	// Each entry: keySize + valueSize = 4 + 8 = 12 bytes
	availableSpace := int(blockSize) - 9
	maxEntries := availableSpace / (keySize + int(valueSize))

	t.Logf("Max entries per leaf: %d", maxEntries)

	// Insert enough to fill multiple leaves
	value := make([]byte, 8)
	insertCount := maxEntries * 5 // Fill 5 leaves worth

	for i := 0; i < insertCount; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed at %d: %v", i, err)
		}
	}

	// Verify all inserted
	for i := 0; i < insertCount; i++ {
		_, err := idx.Find(uint32(i))
		if err != nil {
			t.Errorf("Find failed for key %d: %v", i, err)
		}
	}

	// Delete half
	for i := 0; i < insertCount; i += 2 {
		err = idx.Delete(uint32(i))
		if err != nil {
			t.Fatalf("Delete failed for key %d: %v", i, err)
		}
	}

	// Reinsert in different range
	for i := insertCount; i < insertCount+(insertCount/2); i++ {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Reinsert failed for key %d: %v", i, err)
		}
	}

	ValidateTreeStructure(t, idx)
}

// TestNonSequentialKeys tests insertion of keys in various orders
func TestNonSequentialKeys(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Test 1: Descending order
	t.Run("Descending", func(t *testing.T) {
		for i := 1000; i > 0; i-- {
			binary.LittleEndian.PutUint64(value, uint64(i))
			err = idx.Insert(uint32(i), value)
			if err != nil {
				t.Fatalf("Insert failed for key %d: %v", i, err)
			}
		}

		// Verify all present
		for i := 1; i <= 1000; i++ {
			_, err := idx.Find(uint32(i))
			if err != nil {
				t.Errorf("Find failed for key %d: %v", i, err)
			}
		}

		ValidateTreeStructure(t, idx)

		// Clean up
		for i := 1; i <= 1000; i++ {
			err = idx.Delete(uint32(i))
			if err != nil {
				t.Fatalf("Delete failed for key %d: %v", i, err)
			}
		}
	})

	// Test 2: Random-ish order (prime number gaps)
	t.Run("PrimeGaps", func(t *testing.T) {
		keys := []uint32{}
		for i := uint32(0); i < 500; i++ {
			key := (i * 37) % 1000        // Prime number for pseudo-random distribution
			keys = append(keys, key+2000) // Offset to avoid collision with previous test
		}

		for _, key := range keys {
			binary.LittleEndian.PutUint64(value, uint64(key))
			err = idx.Insert(key, value)
			if err != nil {
				t.Fatalf("Insert failed for key %d: %v", key, err)
			}
		}

		// Verify all present
		for _, key := range keys {
			_, err := idx.Find(key)
			if err != nil {
				t.Errorf("Find failed for key %d: %v", key, err)
			}
		}

		ValidateTreeStructure(t, idx)

		// Clean up
		for _, key := range keys {
			err = idx.Delete(key)
			if err != nil {
				t.Fatalf("Delete failed for key %d: %v", key, err)
			}
		}
	})

	// Test 3: Middle-out insertion
	t.Run("MiddleOut", func(t *testing.T) {
		middle := uint32(5000)
		radius := uint32(250)

		for offset := uint32(0); offset < radius; offset++ {
			// Insert pairs: middle+offset, middle-offset
			binary.LittleEndian.PutUint64(value, uint64(middle+offset))
			err = idx.Insert(middle+offset, value)
			if err != nil {
				t.Fatalf("Insert failed for key %d: %v", middle+offset, err)
			}

			if offset > 0 {
				binary.LittleEndian.PutUint64(value, uint64(middle-offset))
				err = idx.Insert(middle-offset, value)
				if err != nil {
					t.Fatalf("Insert failed for key %d: %v", middle-offset, err)
				}
			}
		}

		// Verify all present
		for offset := uint32(0); offset < radius; offset++ {
			_, err := idx.Find(middle + offset)
			if err != nil {
				t.Errorf("Find failed for key %d: %v", middle+offset, err)
			}
			if offset > 0 {
				_, err = idx.Find(middle - offset)
				if err != nil {
					t.Errorf("Find failed for key %d: %v", middle-offset, err)
				}
			}
		}

		ValidateTreeStructure(t, idx)
	})
}

// TestLargeValueSizes tests index with various value sizes
func TestLargeValueSizes(t *testing.T) {
	valueSizes := []uint32{8, 16, 32, 64, 128, 256}

	for _, valueSize := range valueSizes {
		t.Run(string(rune('0'+valueSize/8))+"bytes", func(t *testing.T) {
			tempDir := t.TempDir()
			err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, valueSize)
			if err != nil {
				t.Fatalf("Failed to create index with valueSize %d: %v", valueSize, err)
			}

			idx, err := OpenIndex(tempDir, "testindex.indx")
			if err != nil {
				t.Fatalf("Failed to open index: %v", err)
			}
			defer idx.Close()

			// Calculate how many entries can fit
			availableSpace := 512 - 9       // Block size - leaf header
			entrySize := 4 + int(valueSize) // uint32 key + value
			maxEntriesPerLeaf := availableSpace / entrySize

			// Skip if fewer than 2 entries fit (B+-tree requires at least 2 entries per node for splits)
			if maxEntriesPerLeaf < 2 {
				t.Skipf("Skipping valueSize %d: only %d entries fit per leaf (minimum 2 required)", valueSize, maxEntriesPerLeaf)
			}

			// Insert enough for multiple leaves
			insertCount := maxEntriesPerLeaf * 3
			value := make([]byte, valueSize)

			for i := 0; i < insertCount; i++ {
				// Fill value with pattern
				for j := 0; j < int(valueSize); j++ {
					value[j] = byte(i + j)
				}

				err = idx.Insert(uint32(i), value)
				if err != nil {
					t.Fatalf("Insert failed for key %d: %v", i, err)
				}
			}

			// Verify all entries
			for i := 0; i < insertCount; i++ {
				found, err := idx.Find(uint32(i))
				if err != nil {
					t.Errorf("Find failed for key %d: %v", i, err)
					continue
				}

				// Verify value pattern
				for j := 0; j < int(valueSize); j++ {
					expected := byte(i + j)
					if found[j] != expected {
						t.Errorf("Key %d, byte %d: got %d, want %d", i, j, found[j], expected)
						break
					}
				}
			}

			ValidateTreeStructure(t, idx)
		})
	}
}

// TestDeleteNonExistentKey tests deleting keys that don't exist
func TestDeleteNonExistentKey(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 10, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Insert some keys: 0, 2, 4, 6, 8
	for i := 0; i < 10; i += 2 {
		binary.LittleEndian.PutUint64(value, uint64(i))
		err = idx.Insert(uint32(i), value)
		if err != nil {
			t.Fatalf("Insert failed: %v", err)
		}
	}

	// Try to delete odd numbers (not present)
	for i := 1; i < 10; i += 2 {
		err = idx.Delete(uint32(i))
		if err == nil {
			t.Errorf("Delete of non-existent key %d should return error", i)
		}
	}

	// Verify even keys still present
	for i := 0; i < 10; i += 2 {
		_, err := idx.Find(uint32(i))
		if err != nil {
			t.Errorf("Key %d should still be present: %v", i, err)
		}
	}

	// Try to delete key way outside range
	err = idx.Delete(uint32(999999))
	if err == nil {
		t.Error("Delete of way out-of-range key should return error")
	}

	ValidateTreeStructure(t, idx)
}
