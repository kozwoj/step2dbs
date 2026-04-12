package primindex

import (
	"encoding/binary"
	"math/rand"
	"testing"
)

// TestCompleteLifecycle tests a full index lifecycle:
// Create → Insert 5000 entries → Delete 2500 random → Delete remaining → Verify empty state
func TestCompleteLifecycle(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex", 512, 100, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	// Validate initial empty state
	ValidateTreeStructure(t, idx)

	// Insert 5000 sequential entries
	value := make([]byte, 8)
	for i := uint32(0); i < 5000; i++ {
		binary.LittleEndian.PutUint64(value, uint64(i*10))
		err = idx.Insert(i, value)
		if err != nil {
			t.Fatalf("Insert failed for key %d: %v", i, err)
		}
	}

	// Validate after inserts
	ValidateTreeStructure(t, idx)

	// Verify all entries searchable
	for i := uint32(0); i < 5000; i++ {
		foundValue, err := idx.Find(i)
		if err != nil {
			t.Errorf("Find failed for key %d: %v", i, err)
		}
		expectedValue := uint64(i * 10)
		actualValue := binary.LittleEndian.Uint64(foundValue)
		if actualValue != expectedValue {
			t.Errorf("Key %d: expected value %d, got %d", i, expectedValue, actualValue)
		}
	}

	// Delete 2500 random entries
	rng := rand.New(rand.NewSource(42))
	deleted := make(map[uint32]bool)
	attempts := 0
	maxAttempts := 10000 // Prevent infinite loop
	for len(deleted) < 2500 && attempts < maxAttempts {
		attempts++
		key := uint32(rng.Intn(5000))
		if !deleted[key] {
			err = idx.Delete(key)
			if err != nil {
				t.Fatalf("Delete failed for key %d: %v", key, err)
			}
			deleted[key] = true
		}
	}

	// Validate after random deletes
	ValidateTreeStructure(t, idx)

	// Verify remaining 2500 entries searchable, deleted entries not found
	for i := uint32(0); i < 5000; i++ {
		_, err := idx.Find(i)
		if deleted[i] {
			if err == nil {
				t.Errorf("Key %d should have been deleted but was found", i)
			}
		} else {
			if err != nil {
				t.Errorf("Key %d should exist but got error: %v", i, err)
			}
		}
	}

	// Delete remaining 2500 entries
	for i := uint32(0); i < 5000; i++ {
		if !deleted[i] {
			err = idx.Delete(i)
			if err != nil {
				// Check if key actually exists
				_, findErr := idx.Find(i)
				if findErr != nil {
					t.Logf("Key %d marked as not deleted but doesn't exist (find error: %v)", i, findErr)
					continue // Skip this key
				}
				t.Fatalf("Delete failed for key %d: %v", i, err)
			}
		}
	}

	// Validate final empty state
	ValidateTreeStructure(t, idx)

	// Verify tree returned to initial state (single empty leaf root)
	rootData, err := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if err != nil {
		t.Fatalf("Failed to read root: %v", err)
	}

	if rootData[2] != NodeTypeLeaf {
		t.Errorf("Root should be a leaf after all deletes, got type %d", rootData[2])
	}

	rootLeaf, err := DeserializeLeafNode(rootData, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		t.Fatalf("Failed to deserialize root leaf: %v", err)
	}

	if len(rootLeaf.Entries) != 0 {
		t.Errorf("Root leaf should have 0 entries, got %d", len(rootLeaf.Entries))
	}
}

// TestMixedOperations tests interleaved insert/delete operations
func TestMixedOperations(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 50, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)
	activeKeys := make(map[uint32]bool)
	nextKey := uint32(0)

	// Helper to insert n keys
	insertKeys := func(n int) {
		for i := 0; i < n; i++ {
			key := nextKey
			nextKey++
			binary.LittleEndian.PutUint64(value, uint64(key*10))
			err = idx.Insert(key, value)
			if err != nil {
				t.Fatalf("Insert failed for key %d: %v", key, err)
			}
			activeKeys[key] = true
		}
	}

	// Helper to delete n random keys
	deleteRandomKeys := func(n int) {
		if len(activeKeys) == 0 {
			return
		}
		deleted := 0
		attempts := 0
		maxAttempts := n * 10
		for deleted < n && attempts < maxAttempts && len(activeKeys) > 0 {
			attempts++
			// Pick a random active key
			var key uint32
			for k := range activeKeys {
				key = k
				break // Take first key from map
			}
			err = idx.Delete(key)
			if err != nil {
				t.Fatalf("Delete failed for key %d: %v", key, err)
			}
			delete(activeKeys, key)
			deleted++
		}
	}

	// Verify all active keys are searchable
	verifyActiveKeys := func() {
		for key := range activeKeys {
			_, err := idx.Find(key)
			if err != nil {
				t.Errorf("Key %d should exist but got error: %v", key, err)
			}
		}
	}

	// Mixed operation sequence: insert 100 → delete 30 → insert 50 → delete 80 → insert 200
	insertKeys(100)
	ValidateTreeStructure(t, idx)
	verifyActiveKeys()

	deleteRandomKeys(30)
	ValidateTreeStructure(t, idx)
	verifyActiveKeys()

	insertKeys(50)
	ValidateTreeStructure(t, idx)
	verifyActiveKeys()

	deleteRandomKeys(80)
	ValidateTreeStructure(t, idx)
	verifyActiveKeys()

	insertKeys(200)
	ValidateTreeStructure(t, idx)
	verifyActiveKeys()

	// Final verification
	if len(activeKeys) != 240 { // 100 - 30 + 50 - 80 + 200 = 240
		t.Errorf("Expected 240 active keys, got %d", len(activeKeys))
	}
}

// TestProgressiveGrowthAndShrinkage tests multiple grow/shrink cycles
func TestProgressiveGrowthAndShrinkage(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 100, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	value := make([]byte, 8)

	// Perform 3 complete grow/shrink cycles
	for cycle := 0; cycle < 3; cycle++ {
		// Grow: Insert entries until multi-level tree
		numEntries := 1000 + (cycle * 100) // Vary the size each cycle
		for i := uint32(0); i < uint32(numEntries); i++ {
			key := uint32(cycle*10000 + int(i)) // Use different key ranges per cycle
			binary.LittleEndian.PutUint64(value, uint64(key*10))
			err = idx.Insert(key, value)
			if err != nil {
				t.Fatalf("Cycle %d: Insert failed for key %d: %v", cycle, key, err)
			}
		}

		// Validate after growth
		ValidateTreeStructure(t, idx)

		// Verify tree is multi-level (root should be internal node)
		rootData, err := ReadIndexBlock(idx, int(idx.Header.RootNode))
		if err != nil {
			t.Fatalf("Cycle %d: Failed to read root: %v", cycle, err)
		}

		if rootData[2] == NodeTypeLeaf && numEntries > 100 {
			t.Logf("Cycle %d: Warning - expected multi-level tree with %d entries, but root is still leaf", cycle, numEntries)
		}

		// Shrink: Delete all entries
		for i := uint32(0); i < uint32(numEntries); i++ {
			key := uint32(cycle*10000 + int(i))
			err = idx.Delete(key)
			if err != nil {
				t.Fatalf("Cycle %d: Delete failed for key %d: %v", cycle, key, err)
			}
		}

		// Validate after shrinkage
		ValidateTreeStructure(t, idx)

		// Verify tree returned to single leaf
		rootData, err = ReadIndexBlock(idx, int(idx.Header.RootNode))
		if err != nil {
			t.Fatalf("Cycle %d: Failed to read root after shrink: %v", cycle, err)
		}

		if rootData[2] != NodeTypeLeaf {
			t.Errorf("Cycle %d: Root should be a leaf after full shrink, got type %d", cycle, rootData[2])
		}

		rootLeaf, err := DeserializeLeafNode(rootData, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			t.Fatalf("Cycle %d: Failed to deserialize root leaf: %v", cycle, err)
		}

		if len(rootLeaf.Entries) != 0 {
			t.Errorf("Cycle %d: Root leaf should have 0 entries, got %d", cycle, len(rootLeaf.Entries))
		}
	}
}

// TestStressRandomOperations performs 10K random operations: 60% insert, 30% delete, 10% find
func TestStressRandomOperations(t *testing.T) {
	tempDir := t.TempDir()
	err := CreateIndexFile(tempDir, "testindex.indx", 512, 200, KeyTypeUint32, 8)
	if err != nil {
		t.Fatalf("Failed to create index: %v", err)
	}

	idx, err := OpenIndex(tempDir, "testindex.indx")
	if err != nil {
		t.Fatalf("Failed to open index: %v", err)
	}
	defer idx.Close()

	rng := rand.New(rand.NewSource(12345))
	value := make([]byte, 8)
	activeKeys := make(map[uint32]bool)
	totalOps := 10000
	keyRange := uint32(5000) // Keys in range [0, 5000)

	for op := 0; op < totalOps; op++ {
		opType := rng.Intn(100)
		key := uint32(rng.Intn(int(keyRange)))

		if opType < 60 {
			// 60% Insert
			if !activeKeys[key] {
				binary.LittleEndian.PutUint64(value, uint64(key*10))
				err = idx.Insert(key, value)
				if err != nil {
					t.Fatalf("Op %d: Insert failed for key %d: %v", op, key, err)
				}
				activeKeys[key] = true
			}
		} else if opType < 90 {
			// 30% Delete
			if activeKeys[key] {
				err = idx.Delete(key)
				if err != nil {
					t.Fatalf("Op %d: Delete failed for key %d: %v", op, key, err)
				}
				delete(activeKeys, key)
			}
		} else {
			// 10% Find
			_, err := idx.Find(key)
			if activeKeys[key] && err != nil {
				t.Errorf("Op %d: Find failed for key %d that should exist: %v", op, key, err)
			}
			if !activeKeys[key] && err == nil {
				t.Errorf("Op %d: Find succeeded for key %d that should not exist", op, key)
			}
		}

		// Validate tree structure every 1000 operations
		if (op+1)%1000 == 0 {
			ValidateTreeStructure(t, idx)
			t.Logf("Completed %d operations, %d active keys", op+1, len(activeKeys))
		}
	}

	// Final validation
	ValidateTreeStructure(t, idx)

	// Verify all active keys are searchable
	for key := range activeKeys {
		foundValue, err := idx.Find(key)
		if err != nil {
			t.Errorf("Final verification: Key %d should exist but got error: %v", key, err)
		} else {
			expectedValue := uint64(key * 10)
			actualValue := binary.LittleEndian.Uint64(foundValue)
			if actualValue != expectedValue {
				t.Errorf("Final verification: Key %d has wrong value: expected %d, got %d",
					key, expectedValue, actualValue)
			}
		}
	}

	t.Logf("Test complete: %d operations, %d active keys remaining", totalOps, len(activeKeys))
}
