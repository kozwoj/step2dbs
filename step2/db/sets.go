package db

import (
	"errors"
	"github.com/kozwoj/indexing/dictionary/postings"
	"os"
)

/* -------------------------------------------------------------------------------------------------
Sets in STEP2 are used to associate a record in one table with a set of records in another table.
Sets are stored using the same mechanism as dictionary postings - each set has a block number
stored in the record header that points to the first block of a postings list.

The set operation functions are
- CreateSetFile: creates a new set file for a table's set instances
- OpenSetFile: opens an existing set file and returns the file handle and header
- GetSetMembers: retrieves all member record IDs for a given set identified by the starting block number
- AddSetMembers: creates a new set with the given member record IDs, and returns the starting block number
- UpdateSetMembers: updates the member record IDs for an existing set
- AddMemberToSet: adds a single member record ID to an existing set
- RemoveMemberFromSet: removes a single member record ID from an existing set
------------------------------------------------------------------------------------------------- */

// Custom error variables for set operations
var (
	ErrSetFileCreate = errors.New("failed to create set file")
	ErrSetFileOpen   = errors.New("failed to open set file")
	ErrSetRead       = errors.New("failed to read set members")
	ErrSetWrite      = errors.New("failed to write set members")
	ErrSetDuplicate  = errors.New("member already exists in set")
)

// Default parameters for set files
const (
	DefaultSetBlockSize     = 512                  // 512 bytes per block
	DefaultSetInitialSize   = 100                  // Initial number of blocks to allocate
	DefaultSetPostingFormat = postings.FormatSlice // Use slice format for sets
)

/*
CreateSetFile creates a new set file for a table's set.
This creates a postings file that will store lists of record IDs for set members.

Parameters:
- filePath: string path to the set file to create (e.g., "CollegeDB/Teachers/Teaches.dat")
- blockSize: uint32 size of each block in bytes (use DefaultSetBlockSize if unsure)
- initialSize: uint32 initial number of blocks to allocate (use DefaultSetInitialSize if unsure)

Returns:
- error: if any

Note: The file is created and closed. Use OpenSetFile to open it for operations.
*/
func CreateSetFile(filePath string, blockSize uint32, initialSize uint32) error {
	file, err := postings.CreatePostingsFile(filePath, blockSize, initialSize, DefaultSetPostingFormat)
	if err != nil {
		return ErrSetFileCreate
	}
	return file.Close()
}

/*
OpenSetFile opens an existing set file and returns the file handle and header.

Parameters:
- filePath: string path to the set file to open

Returns:
- *os.File: pointer to the opened set file
- *postings.PostingsFileHeader: pointer to the file header
- error: if any
*/
func OpenSetFile(filePath string) (*os.File, *postings.PostingsFileHeader, error) {
	file, header, err := postings.OpenPostingsFile(filePath)
	if err != nil {
		return nil, nil, ErrSetFileOpen
	}
	return file, header, nil
}

/*
GetSetFileHeader reads the header from an already-opened set file.

Parameters:
- file: *os.File pointer to the opened set file

Returns:
- *postings.PostingsFileHeader: pointer to the file header
- error: if any
*/
func GetSetFileHeader(file *os.File) (*postings.PostingsFileHeader, error) {
	header, err := postings.ReadPostingsFileHeader(file)
	if err != nil {
		return nil, ErrSetFileOpen
	}
	return header, nil
}

/*
GetSetMembers retrieves all member record IDs for a set identified by the starting block number,
which is stored in the owner's record header.

Parameters:
- file: *os.File pointer to the opened set file
- blockNumber: uint32 block number where the set starts (from record header)
- blockSize: uint32 size of each block in bytes

Returns:
- []uint32: slice of member record IDs
- error: if any

Note: Returns empty slice if blockNumber is postings.NoBlock (empty set).
*/
func GetSetMembers(file *os.File, blockNumber uint32, blockSize uint32) ([]uint32, error) {
	// Empty set
	if blockNumber == postings.NoBlock {
		return []uint32{}, nil
	}

	list := postings.NewPostingsList(DefaultSetPostingFormat, blockSize)
	recordIDs, _, err := list.GetRecordsList(file, blockNumber)
	if err != nil {
		return nil, ErrSetRead
	}
	return recordIDs, nil
}

/*
AddSetMembers adds member record IDs to a set. This creates a new postings list for the set
and returns the starting block number to be stored in the owner's record header.

Parameters:
- file: *os.File pointer to the opened set file
- header: *postings.PostingsFileHeader pointer to the file header
- memberIDs: []uint32 slice of member record IDs to add
- ownerRecordID: uint32 record ID of the owner record (used as dictID in postings)

Returns:
- uint32: block number of the first block in the set (to be stored in the owner's record header)
- error: if any

Note: This function writes the header back to file since AddNewList allocates new blocks.
*/
func AddSetMembers(file *os.File, header *postings.PostingsFileHeader, memberIDs []uint32, ownerRecordID uint32) (uint32, error) {
	list := postings.NewPostingsList(DefaultSetPostingFormat, header.BlockSize)
	blockNumber, err := list.AddNewList(file, header, memberIDs, ownerRecordID)
	if err != nil {
		return postings.NoBlock, ErrSetWrite
	}
	// Write the file header back since AddNewList modified it (allocated blocks)
	err = postings.WritePostingsFileHeader(file, header)
	if err != nil {
		return postings.NoBlock, ErrSetWrite
	}
	return blockNumber, nil
}

/*
UpdateSetMembers updates the member record IDs for an existing set identified by the starting block number,
which is stored in the owner's record header.

Parameters:
- file: *os.File pointer to the opened set file
- header: *postings.PostingsFileHeader pointer to the file header
- blockNumber: uint32 starting block number of the set (from owner's record header)
- memberIDs: []uint32 slice of member record IDs (replaces existing members)
- ownerRecordID: uint32 record ID of the owner record (used as dictID in postings)

Returns:
- error: if any

Note: This reads the existing block chain and writes back the updated list. It may allocate or release
blocks as needed.
The file header is only written back if blocks were allocated or released.
*/
func UpdateSetMembers(file *os.File, header *postings.PostingsFileHeader, blockNumber uint32, memberIDs []uint32, ownerRecordID uint32) error {
	list := postings.NewPostingsList(DefaultSetPostingFormat, header.BlockSize)

	// Get the existing block numbers in the chain
	_, blockNumbers, err := list.GetRecordsList(file, blockNumber)
	if err != nil {
		return ErrSetRead
	}

	// Save the FirstFreeBlock value to detect if blocks were allocated/released
	oldFirstFreeBlock := header.FirstFreeBlock

	// Write back the updated list using the same block chain
	err = list.WriteBackRecordsList(file, header, blockNumbers, memberIDs, ownerRecordID)
	if err != nil {
		return ErrSetWrite
	}

	// Only write the header back if blocks were allocated or released
	if header.FirstFreeBlock != oldFirstFreeBlock {
		err = postings.WritePostingsFileHeader(file, header)
		if err != nil {
			return ErrSetWrite
		}
	}

	return nil
}

/*
AddMemberToSet adds a single member record ID to an existing set.
This is a convenience function that reads the set, adds the new member, and writes the set back.

Parameters:
- file: *os.File pointer to the opened set file
- header: *postings.PostingsFileHeader pointer to the file header
- blockNumber: uint32 starting block number of the set (from owner's record header)
- memberID: uint32 record ID of the member to add
- ownerRecordID: uint32 record ID of the owner record (used as dictID in postings)

Returns:
- error: if any

Note: This adds the new record ID at the end of the list only if it is not already present.
If the member already exists in the set, the function returns ErrSetDuplicate and leaves the set unchanged.
The file header is written only if UpdateSetMembers allocates a new block in case the last block is full.
*/
func AddMemberToSet(file *os.File, header *postings.PostingsFileHeader, blockNumber uint32, memberID uint32, ownerRecordID uint32) error {
	// Get existing members
	memberIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		return err
	}

	for _, existingMemberID := range memberIDs {
		if existingMemberID == memberID {
			return ErrSetDuplicate
		}
	}

	// Add new member
	memberIDs = append(memberIDs, memberID)

	// Write back updated list (UpdateSetMembers handles header write if needed)
	return UpdateSetMembers(file, header, blockNumber, memberIDs, ownerRecordID)
}

/*
RemoveMemberFromSet removes a single member record ID from an existing set.
This is a convenience function that reads the set, removes the member, and writes it back.

Parameters:
- file: *os.File pointer to the opened set file
- header: *postings.PostingsFileHeader pointer to the file header
- blockNumber: uint32 starting block number of the set (from record header)
- memberID: uint32 record ID of the member to remove
- ownerRecordID: uint32 record ID of the owner record (used as dictID in postings)

Returns:
- error: if any

Note: If the member doesn't exist, this is a no-op (no error).
The file header is written only if UpdateSetMembers releases a block.
*/
func RemoveMemberFromSet(file *os.File, header *postings.PostingsFileHeader, blockNumber uint32, memberID uint32, ownerRecordID uint32) error {
	// Get existing members
	memberIDs, err := GetSetMembers(file, blockNumber, header.BlockSize)
	if err != nil {
		return err
	}

	// Remove the member
	updatedMembers := make([]uint32, 0, len(memberIDs))
	for _, id := range memberIDs {
		if id != memberID {
			updatedMembers = append(updatedMembers, id)
		}
	}

	// Write back updated list (UpdateSetMembers handles header write if needed)
	return UpdateSetMembers(file, header, blockNumber, updatedMembers, ownerRecordID)
}

/*
DeleteSet deletes an entire set by freeing all blocks in the postings list chain.
This is used when deleting a record that owns sets - all set instances for that record
must be deleted before the record itself can be removed.

Parameters:
- file: *os.File pointer to the opened set file
- header: *postings.PostingsFileHeader pointer to the set file header
- blockNumber: uint32 starting block number of the set (from record header)

Returns:
- error: if any

Note: If blockNumber is postings.NoBlock (empty set), this is a no-op (no error).
*/
func DeleteSet(file *os.File, header *postings.PostingsFileHeader, blockNumber uint32) error {
	return postings.DeletePostingsList(file, header, blockNumber)
}
