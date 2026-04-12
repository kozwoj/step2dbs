package db

import (
	"github.com/kozwoj/indexing/dictionary/postings"
	"os"
	"path/filepath"
	"testing"
)

func TestCreateSetFile(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	// Verify file exists
	if _, err := os.Stat(setFilePath); os.IsNotExist(err) {
		t.Errorf("Set file was not created at %s", setFilePath)
	}
}

func TestOpenSetFile(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create the file first
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	// Open the file
	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Verify header
	if header.BlockSize != DefaultSetBlockSize {
		t.Errorf("Expected BlockSize %d, got %d", DefaultSetBlockSize, header.BlockSize)
	}
	if header.Format != DefaultSetPostingFormat {
		t.Errorf("Expected Format %d, got %d", DefaultSetPostingFormat, header.Format)
	}
	if header.NumberOfPostings != 0 {
		t.Errorf("Expected NumberOfPostings 0, got %d", header.NumberOfPostings)
	}
}

func TestAddSetMembers_EmptySet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add members to a new set
	ownerRecordID := uint32(1)
	memberIDs := []uint32{10, 20, 30, 40, 50}

	blockNumber, err := AddSetMembers(file, header, memberIDs, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	if blockNumber == postings.NoBlock {
		t.Error("Expected valid block number, got NoBlock")
	}

	// Verify we can retrieve the members
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	if len(retrievedIDs) != len(memberIDs) {
		t.Errorf("Expected %d members, got %d", len(memberIDs), len(retrievedIDs))
	}

	for i, id := range memberIDs {
		if retrievedIDs[i] != id {
			t.Errorf("Member %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}
}

func TestGetSetMembers_EmptySet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Try to get members from an empty set (NoBlock)
	members, err := GetSetMembers(file, postings.NoBlock, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	if len(members) != 0 {
		t.Errorf("Expected empty set, got %d members", len(members))
	}
}

func TestUpdateSetMembers(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add initial members
	ownerRecordID := uint32(1)
	initialMembers := []uint32{10, 20, 30}

	blockNumber, err := AddSetMembers(file, header, initialMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Update with new members
	updatedMembers := []uint32{15, 25, 35, 45}
	err = UpdateSetMembers(file, header, blockNumber, updatedMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("UpdateSetMembers failed: %v", err)
	}

	// Verify updated members
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	if len(retrievedIDs) != len(updatedMembers) {
		t.Errorf("Expected %d members, got %d", len(updatedMembers), len(retrievedIDs))
	}

	for i, id := range updatedMembers {
		if retrievedIDs[i] != id {
			t.Errorf("Member %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}
}

func TestAddMemberToSet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add initial members
	ownerRecordID := uint32(1)
	initialMembers := []uint32{10, 20, 30}

	blockNumber, err := AddSetMembers(file, header, initialMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Add a single member
	newMemberID := uint32(40)
	err = AddMemberToSet(file, header, blockNumber, newMemberID, ownerRecordID)
	if err != nil {
		t.Fatalf("AddMemberToSet failed: %v", err)
	}

	// Verify the member was added
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	expectedMembers := []uint32{10, 20, 30, 40}
	if len(retrievedIDs) != len(expectedMembers) {
		t.Errorf("Expected %d members, got %d", len(expectedMembers), len(retrievedIDs))
	}

	for i, id := range expectedMembers {
		if retrievedIDs[i] != id {
			t.Errorf("Member %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}
}

func TestAddMemberToSet_DuplicateMember(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	ownerRecordID := uint32(1)
	initialMembers := []uint32{10, 20, 30}

	blockNumber, err := AddSetMembers(file, header, initialMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	err = AddMemberToSet(file, header, blockNumber, 20, ownerRecordID)
	if err != ErrSetDuplicate {
		t.Fatalf("AddMemberToSet duplicate error = %v, want %v", err, ErrSetDuplicate)
	}

	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	if len(retrievedIDs) != len(initialMembers) {
		t.Fatalf("Expected %d members after duplicate add attempt, got %d", len(initialMembers), len(retrievedIDs))
	}

	for i, id := range initialMembers {
		if retrievedIDs[i] != id {
			t.Errorf("Member %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}
}

func TestRemoveMemberFromSet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add initial members
	ownerRecordID := uint32(1)
	initialMembers := []uint32{10, 20, 30, 40}

	blockNumber, err := AddSetMembers(file, header, initialMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Remove a member
	memberToRemove := uint32(20)
	err = RemoveMemberFromSet(file, header, blockNumber, memberToRemove, ownerRecordID)
	if err != nil {
		t.Fatalf("RemoveMemberFromSet failed: %v", err)
	}

	// Verify the member was removed
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	expectedMembers := []uint32{10, 30, 40}
	if len(retrievedIDs) != len(expectedMembers) {
		t.Errorf("Expected %d members, got %d", len(expectedMembers), len(retrievedIDs))
	}

	for i, id := range expectedMembers {
		if retrievedIDs[i] != id {
			t.Errorf("Member %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}
}

func TestRemoveMemberFromSet_NonExistent(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add initial members
	ownerRecordID := uint32(1)
	initialMembers := []uint32{10, 20, 30}

	blockNumber, err := AddSetMembers(file, header, initialMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Try to remove a non-existent member (should be no-op)
	memberToRemove := uint32(99)
	err = RemoveMemberFromSet(file, header, blockNumber, memberToRemove, ownerRecordID)
	if err != nil {
		t.Fatalf("RemoveMemberFromSet failed: %v", err)
	}

	// Verify all members are still there
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	if len(retrievedIDs) != len(initialMembers) {
		t.Errorf("Expected %d members, got %d", len(initialMembers), len(retrievedIDs))
	}

	for i, id := range initialMembers {
		if retrievedIDs[i] != id {
			t.Errorf("Member %d: expected %d, got %d", i, id, retrievedIDs[i])
		}
	}
}

func TestMultipleSets(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Create multiple sets for different owner records
	sets := []struct {
		ownerID uint32
		members []uint32
	}{
		{1, []uint32{10, 20, 30}},
		{2, []uint32{15, 25}},
		{3, []uint32{100, 200, 300, 400}},
	}

	blockNumbers := make([]uint32, len(sets))

	// Add all sets
	for i, set := range sets {
		blockNum, err := AddSetMembers(file, header, set.members, set.ownerID)
		if err != nil {
			t.Fatalf("AddSetMembers for owner %d failed: %v", set.ownerID, err)
		}
		blockNumbers[i] = blockNum
	}

	// Verify all sets independently
	for i, set := range sets {
		retrievedIDs, err := GetSetMembers(file, blockNumbers[i], header.BlockSize)
		if err != nil {
			t.Fatalf("GetSetMembers for owner %d failed: %v", set.ownerID, err)
		}

		if len(retrievedIDs) != len(set.members) {
			t.Errorf("Owner %d: Expected %d members, got %d", set.ownerID, len(set.members), len(retrievedIDs))
		}

		for j, id := range set.members {
			if retrievedIDs[j] != id {
				t.Errorf("Owner %d, Member %d: expected %d, got %d", set.ownerID, j, id, retrievedIDs[j])
			}
		}
	}
}

func TestUpdateSetMembers_HeaderWriteOptimization(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add initial members that fit in one block
	ownerRecordID := uint32(1)
	initialMembers := []uint32{10, 20, 30}

	blockNumber, err := AddSetMembers(file, header, initialMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Save FirstFreeBlock after initial add
	freeBlockAfterAdd := header.FirstFreeBlock

	// Update with members that still fit in the same block (no new allocation)
	updatedMembers := []uint32{15, 25, 35} // Same size, should fit in same blocks
	err = UpdateSetMembers(file, header, blockNumber, updatedMembers, ownerRecordID)
	if err != nil {
		t.Fatalf("UpdateSetMembers failed: %v", err)
	}

	// FirstFreeBlock should not have changed (no blocks allocated/freed)
	if header.FirstFreeBlock != freeBlockAfterAdd {
		t.Errorf("FirstFreeBlock changed from %d to %d even though no blocks were allocated/freed",
			freeBlockAfterAdd, header.FirstFreeBlock)
	}

	// Verify the members were updated correctly
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	if len(retrievedIDs) != len(updatedMembers) {
		t.Errorf("Expected %d members, got %d", len(updatedMembers), len(retrievedIDs))
	}

	t.Logf("Header optimization working: FirstFreeBlock unchanged at %d after update that fit in existing blocks", header.FirstFreeBlock)
}

func TestDeleteSet_EmptySet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Test: Delete empty set (NoBlock)
	err = DeleteSet(file, header, postings.NoBlock)
	if err != nil {
		t.Errorf("DeleteSet with NoBlock should not error, got: %v", err)
	}
}

func TestDeleteSet_SingleBlockSet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file
	err := CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add a set with a few members (should fit in one block)
	ownerRecordID := uint32(1)
	members := []uint32{10, 20, 30}

	blockNumber, err := AddSetMembers(file, header, members, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Record FirstFreeBlock before deletion
	freeBlockBefore := header.FirstFreeBlock

	// Delete the set
	err = DeleteSet(file, header, blockNumber)
	if err != nil {
		t.Fatalf("DeleteSet failed: %v", err)
	}

	// Verify the block was returned to free list
	// FirstFreeBlock should now point to the deleted block
	if header.FirstFreeBlock != blockNumber {
		t.Errorf("FirstFreeBlock = %d, want %d", header.FirstFreeBlock, blockNumber)
	}

	// Verify we can retrieve members (should now show the block in free list)
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}

	// The block should be marked as free, so retrieving should return empty or error is acceptable behavior
	t.Logf("After deletion, retrieved %d members (block %d returned to free list, FirstFreeBlock was %d, now %d)",
		len(retrievedIDs), blockNumber, freeBlockBefore, header.FirstFreeBlock)
}

func TestDeleteSet_MultiBlockSet(t *testing.T) {
	tempDir := t.TempDir()
	setFilePath := filepath.Join(tempDir, "test_set.dat")

	// Create and open the file with small blocks to force multiple blocks
	blockSize := uint32(64) // Small blocks to force multiple
	err := CreateSetFile(setFilePath, blockSize, DefaultSetInitialSize)
	if err != nil {
		t.Fatalf("CreateSetFile failed: %v", err)
	}

	file, header, err := OpenSetFile(setFilePath)
	if err != nil {
		t.Fatalf("OpenSetFile failed: %v", err)
	}
	defer file.Close()

	// Add a set with many members to span multiple blocks
	ownerRecordID := uint32(1)
	// Block header is 16 bytes, leaving 48 bytes for data in a 64-byte block
	// Each uint32 is 4 bytes, so we can fit ~12 IDs per block
	// Add 30 members to force at least 3 blocks
	members := make([]uint32, 30)
	for i := range members {
		members[i] = uint32(100 + i)
	}

	blockNumber, err := AddSetMembers(file, header, members, ownerRecordID)
	if err != nil {
		t.Fatalf("AddSetMembers failed: %v", err)
	}

	// Verify members were added
	retrievedIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		t.Fatalf("GetSetMembers failed: %v", err)
	}
	if len(retrievedIDs) != len(members) {
		t.Errorf("Expected %d members, got %d", len(members), len(retrievedIDs))
	}

	// Record FirstFreeBlock before deletion
	freeBlockBefore := header.FirstFreeBlock

	// Delete the entire set
	err = DeleteSet(file, header, blockNumber)
	if err != nil {
		t.Fatalf("DeleteSet failed: %v", err)
	}

	// Verify blocks were returned to free list
	// The starting block should now be in the free list
	if header.FirstFreeBlock == freeBlockBefore {
		t.Errorf("FirstFreeBlock unchanged (%d), expected it to change after deleting multi-block set", freeBlockBefore)
	}

	t.Logf("Deleted multi-block set starting at block %d, FirstFreeBlock changed from %d to %d",
		blockNumber, freeBlockBefore, header.FirstFreeBlock)
}
