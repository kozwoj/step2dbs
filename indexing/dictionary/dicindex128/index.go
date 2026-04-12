package dicindex128

import (
	"errors"
	"os"
)

// IndexFileName is the file name for dictionary index files
const IndexFileName = "index.dat"

// custom error variables for index operations
var (
	ErrIndexFileNotFound              = errors.New("index file not found")
	ErrIndexDirectoryNotFound         = errors.New("index directory not found")
	ErrIndexFailedToReadIndexHeader   = errors.New("failed to read block zero of index file")
	ErrIndexFailedToDeserializeHeader = errors.New("failed to deserialize index file header")
	ErrIndexFailedToCloseIndex        = errors.New("failed to close index file")
	ErrIndexNotOpened                 = errors.New("index file not opened")
	ErrIndexFailedToReadBlock         = errors.New("failed to read index file block")
	ErrIndexFailedToWriteBlock        = errors.New("failed to write index file block")
	ErrIndexEntryAlreadyExists        = errors.New("entry already exists in index")
	ErrIndexEntryNotFound             = errors.New("entry does not exist in index")
)

/* ------------------------ INDEX OPERATIONS ------------------------

OpenDictionaryIndex : open an existing index - open the index file and return Index object
Insert : insert entry (hash128 + dictID + postingsRef)
Find : find an entry given the hash128 (returns dictID, postingsRef)
Delete : delete entry given the hash128
Close : close an index
---------------------------------------------------------------------*/

/*
OpenDictionaryIndex opens an existing index file given its directory and file name.
Parameters:
- dirPath: directory path where the index file is located
- fileName: base name of the dictionary (will be prefixed to index.dat)
Returns:
- *Index : pointer to opened Index struct
- error: if any (e.g. file not found, read error, deserialize error)
*/
func OpenDictionaryIndex(dirPath string, fileName string) (*Index, error) {
	var filePath string
	if dirPath[len(dirPath)-1] != '/' {
		dirPath += "/"
	}
	// check if the directory exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil, ErrIndexDirectoryNotFound
	}
	filePath = dirPath + IndexFileName
	// open with read-write so inserts and updates can write blocks
	file, err := os.OpenFile(filePath, os.O_RDWR, 0)
	if err != nil {
		return nil, ErrIndexFileNotFound
	}
	block := make([]byte, IndexHeaderSize)
	_, err = file.ReadAt(block, 0)
	if err != nil {
		file.Close()
		return nil, ErrIndexFailedToReadIndexHeader
	}
	deserialized, err := DeserializeIndexHeader(block)
	if err != nil {
		file.Close()
		return nil, ErrIndexFailedToDeserializeHeader
	}
	var idx Index
	idx.Header = deserialized
	idx.File = file
	return &idx, nil
}

/*
Close closes the index file.
Returns:
- error: if any (e.g. failed to close file)
*/
func (idx *Index) Close() error {
	if idx.File == nil {
		return ErrIndexNotOpened
	}
	err := idx.File.Close()
	if err != nil {
		return ErrIndexFailedToCloseIndex
	}
	idx.File = nil
	return nil
}

/*
Insert inserts a new entry into the index.
Parameters:
- indexEntry: pointer to IndexEntry128 struct to insert
Returns:
- error: if any (e.g. failed to insert entry, entry already exists)

Note: Unlike dicindex, this implementation doesn't check for hash collisions
since 128-bit hashes are considered unique. If an entry with the same hash
exists, it returns ErrIndexEntryAlreadyExists.
*/
func (idx *Index) Insert(indexEntry *IndexEntry128) error {
	// find leaf node to insert into
	leafBlockNumber, path, err := idx.FindLeafBlock(indexEntry.Hash)
	if err != nil {
		return err
	}
	// insert entry into leaf node
	leafBlock, err := ReadIndexBlock(idx, int(leafBlockNumber))
	if err != nil {
		return err
	}

	// Check if entry already exists
	existingEntry, err := FindEntryInBlock(leafBlock, indexEntry.Hash)
	if err == nil {
		// Found an entry with the same hash - with 128-bit hashes, this is a duplicate
		if existingEntry.Hash.Equal(indexEntry.Hash) {
			return ErrIndexEntryAlreadyExists
		}
	} else if err != ErrEntryNotFound {
		// Some other error occurred during search
		return err
	}
	// Entry not found, proceed with insertion

	// check if leaf node is full
	isFull := IsLeafBlockFull(leafBlock, indexEntry)
	if !isFull {
		// insert entry into leaf node
		err := InsertEntryToBlock(leafBlock, indexEntry)
		if err != nil {
			return err
		}
		// write the modified leaf block back to disk
		err = WriteIndexBlock(idx, int(leafBlockNumber), leafBlock)
		if err != nil {
			return err
		}
		return nil
	} else {
		// leaf node is full, need to split it
		// Allocate a new block for the right half
		newBlockNumber := idx.Header.NextEmptyBlock
		newBlock := make([]byte, idx.Header.BlockSize)

		// Split the leaf node first (before inserting new entry)
		_, rightMinHash, err := SplitLeafNodeBlock(leafBlock, newBlock, newBlockNumber)
		if err != nil {
			return err
		}

		// Now insert the new entry into the appropriate leaf
		// Determine which leaf to insert into based on the hash
		var targetBlock []byte
		if CompareHash128(indexEntry.Hash, rightMinHash) < 0 {
			// Insert into left (original) leaf
			targetBlock = leafBlock
		} else {
			// Insert into right (new) leaf
			targetBlock = newBlock
		}

		// Insert the entry into the chosen leaf
		err = InsertEntryToBlock(targetBlock, indexEntry)
		if err != nil {
			return err
		}

		// Write both blocks back to disk
		err = WriteIndexBlock(idx, int(leafBlockNumber), leafBlock)
		if err != nil {
			return err
		}
		err = WriteIndexBlock(idx, int(newBlockNumber), newBlock)
		if err != nil {
			return err
		}

		// Update NextEmptyBlock in header
		idx.Header.NextEmptyBlock++
		headerBlock, err := SerializeIndexHeader(idx.Header)
		if err != nil {
			return err
		}
		_, err = idx.File.WriteAt(headerBlock, 0)
		if err != nil {
			return err
		}

		// Propagate the split key up the tree using the traversal path
		err = idx.PropagateSplit(path, rightMinHash, newBlockNumber)
		if err != nil {
			return err
		}

		return nil
	}
}

/*
Find looks for an entry in the index given its hash.
Parameters:
- hash: Hash128 key to search for
Returns:
- dictID: uint32 dictionary ID if found
- postingsRef: uint32 postings reference if found
- error: if any (e.g. entry not found)

Note: Unlike dicindex, no string parameter is needed since 128-bit hashes
are considered unique and collision-free.
*/
func (idx *Index) Find(hash Hash128) (dictID uint32, postingsRef uint32, err error) {
	// Find the leaf block that should contain this entry
	leafBlockNumber, _, err := idx.FindLeafBlock(hash)
	if err != nil {
		return 0, 0, err
	}

	// Read the leaf block
	leafBlock, err := ReadIndexBlock(idx, int(leafBlockNumber))
	if err != nil {
		return 0, 0, err
	}

	// Search for the entry in the leaf block
	existingEntry, err := FindEntryInBlock(leafBlock, hash)
	if err == ErrEntryNotFound {
		// Entry with this hash doesn't exist
		return 0, 0, ErrIndexEntryNotFound
	} else if err != nil {
		// Some other error occurred during search
		return 0, 0, err
	}

	// Found the entry - return the dictID and postingsRef
	return existingEntry.DictID, existingEntry.PostingsRef, nil
}

/*
Delete removes an entry from the index.
Parameters:
- hash: Hash128 key to delete
Returns:
- error: if any (e.g. entry not found)

Note: This function uses lazy deletion - it only removes the entry from the leaf node without
modifying the tree structure. Empty leaf nodes remain in place and can be reused later.
Unlike dicindex, no string parameter is needed since 128-bit hashes are considered unique.
*/
func (idx *Index) Delete(hash Hash128) error {
	// Find the leaf block that should contain this entry
	leafBlockNumber, _, err := idx.FindLeafBlock(hash)
	if err != nil {
		return err
	}

	// Read the leaf block
	leafBlock, err := ReadIndexBlock(idx, int(leafBlockNumber))
	if err != nil {
		return err
	}

	// Search for the entry in the leaf block to verify it exists
	_, err = FindEntryInBlock(leafBlock, hash)
	if err == ErrEntryNotFound {
		// Entry with this hash doesn't exist
		return ErrIndexEntryNotFound
	} else if err != nil {
		// Some other error occurred during search
		return err
	}

	// Entry exists, proceed with deletion
	err = DeleteEntryFromBlock(leafBlock, hash)
	if err != nil {
		return err
	}

	// Write the modified leaf block back to disk
	err = WriteIndexBlock(idx, int(leafBlockNumber), leafBlock)
	if err != nil {
		return err
	}

	return nil
}

/*
CreateDictionaryIndexFile creates a new index file with initial structure.
This function creates the file, initializes it with root and first leaf nodes, then closes it.
Use OpenDictionaryIndex to open the file for subsequent operations.

Parameters:
- dirPath: directory path where the index file will be created
- fileName: base name of the dictionary (will be prefixed to index.dat)
- blockSize: size of each block in bytes (typically 1024)
- initialBlocks: initial number of blocks to allocate
Returns:
- *Index: always nil (file is closed after creation)
- error: if any (e.g. failed to create file)
*/
func CreateDictionaryIndexFile(dirPath string, fileName string, blockSize, initialBlocks uint16) (*Index, error) {
	header := IndexHeader{
		BlockSize:      blockSize,
		FileLength:     initialBlocks,
		RootNode:       1, // root node at block 1
		FirstLeaf:      2, // first leaf node at block 2
		NextEmptyBlock: 3, // next empty block starts at block 3
	}
	// check if the directory exists
	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		return nil, ErrDirNotExist
	}
	// check if the dirPath ends with a slash
	var filePath string
	if dirPath[len(dirPath)-1] != '/' {
		filePath = dirPath + "/" + IndexFileName
	} else {
		filePath = dirPath + IndexFileName
	}
	// create index file
	f, err := os.Create(filePath)
	if err != nil {
		return nil, ErrFileCreate
	}
	defer f.Close()
	// Serialize header
	headerBlock, err := SerializeIndexHeader(&header)
	if err != nil {
		return nil, ErrHeaderSerialize
	}
	// Write header to the file at offset 0
	if _, err := f.WriteAt(headerBlock, 0); err != nil {
		return nil, ErrHeaderWrite
	}

	// Create empty root internal node at block 1
	rootNode := &InternalNode{
		BlockNumber: 1,
		NodeType:    InternalNodeType,
		KeyCount:    0,
		Keys:        []Hash128{},
		Pointers:    []uint16{2}, // points to first leaf at block 2
	}
	rootBlock := make([]byte, blockSize)
	if err := SerializeInternalNode(rootNode, rootBlock); err != nil {
		return nil, err
	}
	// Write root node to block 1 (offset 10 + blockSize)
	offset := int64(IndexHeaderSize) + int64(blockSize)
	if _, err := f.WriteAt(rootBlock, offset); err != nil {
		return nil, err
	}

	// Create empty first leaf node at block 2
	firstLeaf := &LeafNode{
		BlockNumber: 2,
		NodeType:    LeafNodeType,
		EntryCount:  0,
		NextLeaf:    0, // no next leaf yet
		Entries:     []*IndexEntry128{},
	}
	leafBlock := make([]byte, blockSize)
	if err := SerializeLeafNode(firstLeaf, leafBlock); err != nil {
		return nil, err
	}
	// Write first leaf to block 2 (offset 10 + 2*blockSize)
	offset = int64(IndexHeaderSize) + 2*int64(blockSize)
	if _, err := f.WriteAt(leafBlock, offset); err != nil {
		return nil, err
	}

	// Expand file to required size
	totalSize := int64(IndexHeaderSize) + int64(header.BlockSize)*int64(header.FileLength)
	if err := f.Truncate(totalSize); err != nil {
		return nil, ErrFileTruncate
	}

	// File will be closed by defer - caller should use OpenDictionaryIndex to reopen it
	return nil, nil
}
