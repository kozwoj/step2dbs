package primindex

import (
	"errors"
	"os"
	"path/filepath"
)

var (
	ErrIndexFileNotFound = errors.New("index file not found")
	ErrIndexFileOpen     = errors.New("failed to open index file")
	ErrHeaderRead        = errors.New("failed to read index header")
	ErrIndexFileClosed   = errors.New("index file already closed")

	ErrBlockReadFailed  = errors.New("failed to read block")
	ErrBlockWriteFailed = errors.New("failed to write block")
)

// Sentinel value indicating end of empty block chain
const (
	NoNextEmptyBlock uint16 = 0xFFFF
)

/* ------------------------ INDEX OPERATIONS ------------------------

Insert : insert entry for the given key value
Find : find an entry given the key value
Delete : delete entry given the key value
Close : close an index

note: OpenIndex, which is standalone function, opens an existing
index file and returns Index object
---------------------------------------------------------------------*/

/*
OpenIndex opens an existing index file and returns an Index object.
Parameters:
- path: directory path where the index file is located
- filename: full name of the index file (e.g., "index.dat" or "myindex.indx")
Returns:
- *Index: pointer to Index object with header, codec, and file handle
- error: if any (e.g. file not found, header read failed)
*/
func OpenIndex(path string, filename string) (*Index, error) {
	// Construct the index file path
	indexFilePath := filepath.Join(path, filename)

	// Check if file exists
	if _, err := os.Stat(indexFilePath); os.IsNotExist(err) {
		return nil, ErrIndexFileNotFound
	}

	// Open the index file
	file, err := os.OpenFile(indexFilePath, os.O_RDWR, 0666)
	if err != nil {
		return nil, ErrIndexFileOpen
	}

	// Read the header (first 15 bytes)
	headerData := make([]byte, IndexHeaderSize)
	_, err = file.ReadAt(headerData, 0)
	if err != nil {
		file.Close()
		return nil, ErrHeaderRead
	}

	// Deserialize the header
	header, err := DeserializeIndexHeader(headerData)
	if err != nil {
		file.Close()
		return nil, err
	}

	// Get the appropriate codec based on KeyType
	codec := KeyCodecFactory(KeyType(header.KeyType))

	// Create and return the Index object
	index := &Index{
		Header: header,
		Codec:  codec,
		File:   file,
	}

	return index, nil
}

/*
Close closes the index file and releases resources.
Returns:
- error: if any (e.g. file close failed)
*/
func (idx *Index) Close() error {
	if idx.File == nil {
		return ErrIndexFileClosed
	}

	err := idx.File.Close()
	if err != nil {
		return err
	}

	idx.File = nil
	return nil
}

/*
Find looks for an entry in the index given its key.
Parameters:
- key: key value to search for (type depends on KeyType)
Returns:
- []byte: value data if found
- error: if any (e.g. entry not found)
*/
func (idx *Index) Find(key interface{}) ([]byte, error) {
	// Find the leaf block that should contain this entry
	leafBlockNumber, _, err := idx.FindLeafBlock(key)
	if err != nil {
		return nil, err
	}

	// Read the leaf block
	leafBlock, err := ReadIndexBlock(idx, int(leafBlockNumber))
	if err != nil {
		return nil, err
	}

	// Search for the entry in the leaf block
	entry, err := FindEntryInBlock(leafBlock, key, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return nil, err
	}

	// Found the entry - return the value
	return entry.Value, nil
}

/*
Delete removes an entry from the index.
Parameters:
- key: key value to delete (type depends on KeyType)
Returns:
- error: if any (e.g. entry not found)

Note: Enhanced delete with empty leaf handling and separator key updates:
- When a leaf becomes empty: removes from leaf chain, converts to EmptyNode, and removes from parent
- When minimum key deleted from non-empty leaf: updates parent separator key
- (Stage 7) Will handle cascading empty parents and tree height reduction
*/
func (idx *Index) Delete(key interface{}) error {
	// Find the leaf block that should contain this entry (with path for parent updates)
	leafBlockNumber, path, err := idx.FindLeafBlock(key)
	if err != nil {
		return err
	}

	// Read the leaf block
	leafBlock, err := ReadIndexBlock(idx, int(leafBlockNumber))
	if err != nil {
		return err
	}

	// Check if we're deleting the minimum key BEFORE deletion
	oldMinKey, err := GetMinimumKeyFromLeaf(leafBlock, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return err
	}

	serializedKey, err := idx.Codec.Serialize(key)
	if err != nil {
		return err
	}

	isDeletingMinimum := idx.Codec.Compare(serializedKey, oldMinKey) == 0

	// Search for the entry in the leaf block to verify it exists
	_, err = FindEntryInBlock(leafBlock, key, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return err
	}

	// Entry exists, proceed with deletion
	err = DeleteEntryFromBlock(leafBlock, key, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return err
	}

	// Check if leaf is now empty after deletion
	leafNode, err := DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return err
	}

	if leafNode.EntryCount == 0 {
		// Leaf is now empty

		// Special case: if this is the root leaf (len(path) == 1), keep it as an empty leaf
		// This is the initial state of the index - don't convert to EmptyNode
		if len(path) == 1 {
			// Root leaf is now empty - just write it back as an empty leaf
			// This returns the index to its initial state
			err = WriteIndexBlock(idx, int(leafBlockNumber), leafBlock)
			if err != nil {
				return err
			}
			return nil
		}

		// Non-root leaf is empty - perform empty leaf handling

		// Step 1: Remove leaf from the doubly-linked leaf chain
		err = RemoveLeafFromChain(idx, leafBlockNumber, leafNode.PrevLeaf, leafNode.NextLeaf)
		if err != nil {
			return err
		}

		// Step 2: Convert leaf to EmptyNode and add to reusable chain
		err = ConvertLeafToEmptyNode(idx, leafBlockNumber)
		if err != nil {
			return err
		}

		// Step 3: Remove key/pointer from parent internal node (triggers recursive handling in Stage 7)
		err = RemoveKeyFromParent(idx, path, leafBlockNumber)
		if err != nil {
			return err
		}

		return nil
	}

	// Leaf is not empty - check if we need to update parent separator key
	if isDeletingMinimum && len(path) > 1 {
		// Get the new minimum key from the leaf
		newMinKey, err := GetMinimumKeyFromLeaf(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		// Update parent's separator key
		err = UpdateParentSeparatorKey(idx, path, oldMinKey, newMinKey)
		if err != nil {
			return err
		}
	}

	// Write the modified leaf block back to disk
	err = WriteIndexBlock(idx, int(leafBlockNumber), leafBlock)
	if err != nil {
		return err
	}

	return nil
}

/*
Insert inserts a new entry into the index.
Parameters:
- key: key value to insert (type depends on KeyType)
- value: value data to associate with the key (must match ValueSize)
Returns:
- error: if any (e.g. failed to insert entry, entry already exists)
*/
func (idx *Index) Insert(key interface{}, value []byte) error {
	// Validate value size
	if len(value) != int(idx.Header.ValueSize) {
		return errors.New("value size does not match index ValueSize")
	}

	// Create the index entry
	entry := &IndexEntry{
		Key:   key,
		Value: value,
	}

	// Find leaf node to insert into
	leafBlockNumber, path, err := idx.FindLeafBlock(key)
	if err != nil {
		return err
	}

	// Read the leaf block
	leafBlock, err := ReadIndexBlock(idx, int(leafBlockNumber))
	if err != nil {
		return err
	}

	// Check if entry already exists
	existingEntry, err := FindEntryInBlock(leafBlock, key, idx.Codec, int(idx.Header.ValueSize))
	if err == nil {
		// Found an entry with the same key - this is a duplicate
		keyBytes, _ := idx.Codec.Serialize(key)
		existingKeyBytes, _ := idx.Codec.Serialize(existingEntry.Key)
		if idx.Codec.Compare(keyBytes, existingKeyBytes) == 0 {
			return errors.New("entry already exists in index")
		}
	}
	// Entry not found, proceed with insertion

	// Check if leaf node is full
	isFull := IsLeafBlockFull(leafBlock, idx.Codec.Size(), int(idx.Header.ValueSize))
	if !isFull {
		// Insert entry into leaf node
		err := InsertEntryToBlock(leafBlock, entry, idx.Codec, int(idx.Header.ValueSize))
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

	// Leaf node is full, need to split it
	// Allocate a new block for the right half
	newBlockNumber, err := GetEmptyBlock(idx)
	if err != nil {
		return err
	}
	newBlock := make([]byte, idx.Header.BlockSize)

	// Split the leaf node first (before inserting new entry)
	_, rightMinKey, err := SplitLeafNodeBlock(leafBlock, newBlock, newBlockNumber, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return err
	}

	// Serialize the new entry key for comparison
	entryKeyBytes, err := idx.Codec.Serialize(key)
	if err != nil {
		return err
	}

	// Now insert the new entry into the appropriate leaf
	// Determine which leaf to insert into based on the key
	var targetBlock []byte
	if idx.Codec.Compare(entryKeyBytes, rightMinKey) < 0 {
		// Insert into left (original) leaf
		targetBlock = leafBlock
	} else {
		// Insert into right (new) leaf
		targetBlock = newBlock
	}

	// Insert the entry into the chosen leaf
	err = InsertEntryToBlock(targetBlock, entry, idx.Codec, int(idx.Header.ValueSize))
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

	// Update the old next leaf's PrevLeaf pointer to point to the new right node
	// Read the NextLeaf pointer from the new right leaf
	rightLeaf, err := DeserializeLeafNode(newBlock, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		return err
	}

	if rightLeaf.NextLeaf != NoNextLeaf {
		// Read the old next leaf block
		oldNextBlock, err := ReadIndexBlock(idx, int(rightLeaf.NextLeaf))
		if err != nil {
			return err
		}

		// Deserialize it
		oldNextLeaf, err := DeserializeLeafNode(oldNextBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		// Update its PrevLeaf pointer to point to the new right node
		oldNextLeaf.PrevLeaf = newBlockNumber

		// Serialize and write back
		oldNextSerialized, err := SerializeLeafNode(oldNextLeaf, int(idx.Header.BlockSize), idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		err = WriteIndexBlock(idx, int(rightLeaf.NextLeaf), oldNextSerialized)
		if err != nil {
			return err
		}
	}

	// Check if we just split the root (when root is a leaf)
	if len(path) == 1 && leafBlockNumber == idx.Header.RootNode {
		// Root was a leaf and we split it - need to create a new root
		newRootBlockNumber, err := GetEmptyBlock(idx)
		if err != nil {
			return err
		}
		newRootBlock := make([]byte, idx.Header.BlockSize)

		// Create a new root with one key (rightMinKey) and two pointers
		newRoot := &InternalNode{
			BlockNumber: newRootBlockNumber,
			NodeType:    1, // InternalNodeType
			KeyCount:    1,
			Keys:        [][]byte{rightMinKey},
			Pointers:    []uint16{leafBlockNumber, newBlockNumber},
		}

		// Serialize the new root into the block
		keySize := idx.Codec.Size()
		serializedRoot, err := SerializeInternalNode(newRoot, int(idx.Header.BlockSize), keySize)
		if err != nil {
			return err
		}
		copy(newRootBlock, serializedRoot)

		// Write new root to disk
		err = WriteIndexBlock(idx, int(newRootBlockNumber), newRootBlock)
		if err != nil {
			return err
		}

		// Update header
		idx.Header.RootNode = newRootBlockNumber

		// Write updated header to disk
		headerBlock, err := SerializeIndexHeader(idx.Header)
		if err != nil {
			return err
		}
		_, err = idx.File.WriteAt(headerBlock, 0)
		if err != nil {
			return err
		}

		return nil
	}

	// Not splitting the root leaf - propagate the split key up the tree
	err = idx.PropagateSplit(path, rightMinKey, newBlockNumber)
	if err != nil {
		return err
	}

	return nil
}

/* ------------------------ Delete Helpers  ------------------------

The functions below are helpers for deleing with empty modes resulting
from deletions.

- ConvertLeafToEmptyNode: converts a leaf node to an empty node and adds
it to the empty block chain
- ConvertInternalToEmptyNode: converts an internal node to an empty node
- RemoveLeafFromChain: removes a leaf node from the doubly-linked leaf chain
- UpdateParentSeparatorKey: updates a separator key in a parent internal node
- FindParentKeyForChild: finds the separator key in parent for a given child block
- RemoveKeyFromParent: removes a key/pointer from parent internal node

---------------------------------------------------------------------*/

/*
ConvertLeafToEmptyNode converts a leaf node to an empty node and adds it to the empty block chain.
Parameters:
- idx: pointer to Index object
- leafBlockNumber: block number of the leaf to convert
Returns:
- error: if any (e.g. read/write failed)

This function:
1. Creates an EmptyNode with the given block number
2. Sets its NextEmptyBlock to the current header.NextEmptyBlock
3. Serializes the empty node and writes it to the block
4. Updates header.NextEmptyBlock to point to this newly converted block
5. Writes the updated header back to file
*/
func ConvertLeafToEmptyNode(idx *Index, leafBlockNumber uint16) error {
	// Create empty node
	emptyNode := &EmptyNode{
		BlockNumber:    leafBlockNumber,
		NodeType:       NodeTypeEmpty,
		NextEmptyBlock: idx.Header.NextEmptyBlock, // Point to current head of empty chain
	}

	// Serialize the empty node
	emptyBlock, err := SerializeEmptyNode(emptyNode, int(idx.Header.BlockSize))
	if err != nil {
		return err
	}

	// Write the empty node block to file
	err = WriteIndexBlock(idx, int(leafBlockNumber), emptyBlock)
	if err != nil {
		return ErrBlockWriteFailed
	}

	// Update header to point to this block as the new head of empty chain
	idx.Header.NextEmptyBlock = leafBlockNumber

	// Write updated header to file
	headerData, err := SerializeIndexHeader(idx.Header)
	if err != nil {
		return err
	}
	_, err = idx.File.WriteAt(headerData, 0)
	if err != nil {
		return ErrHeaderWrite
	}

	return nil
}

/*
ConvertInternalToEmptyNode converts an internal node to an empty node and adds it to the empty block chain.
Parameters:
- idx: pointer to Index object
- internalBlockNumber: block number of the internal node to convert
Returns:
- error: if any (e.g. read/write failed)

This function is similar to ConvertLeafToEmptyNode but works on internal nodes.
Used when an internal node becomes empty after recursive deletion.
*/
func ConvertInternalToEmptyNode(idx *Index, internalBlockNumber uint16) error {
	// Create empty node
	emptyNode := &EmptyNode{
		BlockNumber:    internalBlockNumber,
		NodeType:       NodeTypeEmpty,
		NextEmptyBlock: idx.Header.NextEmptyBlock, // Point to current head of empty chain
	}

	// Serialize the empty node
	emptyBlock, err := SerializeEmptyNode(emptyNode, int(idx.Header.BlockSize))
	if err != nil {
		return err
	}

	// Write the empty node block to file
	err = WriteIndexBlock(idx, int(internalBlockNumber), emptyBlock)
	if err != nil {
		return ErrBlockWriteFailed
	}

	// Update header to point to this block as the new head of empty chain
	idx.Header.NextEmptyBlock = internalBlockNumber

	// Write updated header to file
	headerData, err := SerializeIndexHeader(idx.Header)
	if err != nil {
		return err
	}
	_, err = idx.File.WriteAt(headerData, 0)
	if err != nil {
		return ErrHeaderWrite
	}

	return nil
}

/*
RemoveLeafFromChain removes a leaf node from the doubly-linked leaf chain.
Parameters:
- idx: pointer to Index object
- leafBlockNumber: block number of the leaf being removed
- prevLeaf: block number of previous leaf (or NoPrevLeaf if this is first)
- nextLeaf: block number of next leaf (or NoNextLeaf if this is last)
Returns:
- error: if any (e.g. read/write failed)

This function:
1. Updates the previous leaf's NextLeaf pointer (or header.FirstLeaf if removing first)
2. Updates the next leaf's PrevLeaf pointer
3. Maintains the integrity of the doubly-linked leaf chain
*/
func RemoveLeafFromChain(idx *Index, leafBlockNumber uint16, prevLeaf, nextLeaf uint16) error {
	// Handle previous leaf pointer
	if prevLeaf != NoPrevLeaf {
		// Read previous leaf block
		prevBlock, err := ReadIndexBlock(idx, int(prevLeaf))
		if err != nil {
			return ErrBlockReadFailed
		}

		// Deserialize previous leaf
		prevLeafNode, err := DeserializeLeafNode(prevBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		// Update its NextLeaf pointer to skip the removed leaf
		prevLeafNode.NextLeaf = nextLeaf

		// Serialize and write back
		prevBlock, err = SerializeLeafNode(prevLeafNode, int(idx.Header.BlockSize), idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		err = WriteIndexBlock(idx, int(prevLeaf), prevBlock)
		if err != nil {
			return ErrBlockWriteFailed
		}
	} else {
		// This was the first leaf - update header.FirstLeaf
		idx.Header.FirstLeaf = nextLeaf

		// Write updated header
		headerData, err := SerializeIndexHeader(idx.Header)
		if err != nil {
			return err
		}
		_, err = idx.File.WriteAt(headerData, 0)
		if err != nil {
			return ErrHeaderWrite
		}
	}

	// Handle next leaf pointer
	if nextLeaf != NoNextLeaf {
		// Read next leaf block
		nextBlock, err := ReadIndexBlock(idx, int(nextLeaf))
		if err != nil {
			return ErrBlockReadFailed
		}

		// Deserialize next leaf
		nextLeafNode, err := DeserializeLeafNode(nextBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		// Update its PrevLeaf pointer to skip the removed leaf
		nextLeafNode.PrevLeaf = prevLeaf

		// Serialize and write back
		nextBlock, err = SerializeLeafNode(nextLeafNode, int(idx.Header.BlockSize), idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return err
		}

		err = WriteIndexBlock(idx, int(nextLeaf), nextBlock)
		if err != nil {
			return ErrBlockWriteFailed
		}
	}

	return nil
}

/*
UpdateParentSeparatorKey updates a separator key in a parent internal node.
Parameters:
- idx: pointer to Index object
- path: path from root to leaf (from FindLeafBlock)
- oldKey: the old separator key to replace
- newKey: the new separator key
Returns:
- error: if any (e.g. read/write failed, key not found)

This function is called when the minimum key in a leaf is deleted (but leaf is not empty).
The parent's separator key needs to be updated to reflect the new minimum key.
In a B+ tree, separator keys represent the minimum key of the right subtree.
*/
func UpdateParentSeparatorKey(idx *Index, path []uint16, oldKey, newKey []byte) error {
	if len(path) < 2 {
		// No parent to update (single-node tree)
		return nil
	}

	// Get parent block number from path
	parentBlockNum := path[len(path)-2]

	// Read parent block
	parentBlock, err := ReadIndexBlock(idx, int(parentBlockNum))
	if err != nil {
		return ErrBlockReadFailed
	}

	// Deserialize parent internal node
	keySize := idx.Codec.Size()
	parentNode, err := DeserializeInternalNode(parentBlock, keySize)
	if err != nil {
		return err
	}

	// Find and replace the old key with the new key
	keyFound := false
	for i := 0; i < len(parentNode.Keys); i++ {
		if idx.Codec.Compare(parentNode.Keys[i], oldKey) == 0 {
			parentNode.Keys[i] = newKey
			keyFound = true
			break
		}
	}

	if !keyFound {
		// This can happen if the leaf is the leftmost child (no separator key for leftmost)
		return nil
	}

	// Serialize the updated parent node
	parentBlock, err = SerializeInternalNode(parentNode, int(idx.Header.BlockSize), keySize)
	if err != nil {
		return err
	}

	// Write the updated parent block back to disk
	err = WriteIndexBlock(idx, int(parentBlockNum), parentBlock)
	if err != nil {
		return ErrBlockWriteFailed
	}

	return nil
}

/*
FindParentKeyForChild finds the separator key in a parent internal node that corresponds to a child pointer.
Parameters:
- parentBlock: serialized parent internal node block
- childBlockNumber: block number of the child we're looking for
- keySize: size of keys in bytes
Returns:
- []byte: the separator key (nil if child is leftmost)
- int: index of the key in the Keys array (-1 if leftmost child)
- error: if any

In a B+ tree, Keys[i] represents the minimum key of the right subtree at Pointers[i+1].
So if childBlockNumber is at Pointers[0], there's no separator key (leftmost child).
If childBlockNumber is at Pointers[i] where i > 0, the separator key is Keys[i-1].
*/
func FindParentKeyForChild(parentBlock []byte, childBlockNumber uint16, keySize int) ([]byte, int, error) {
	// Deserialize parent internal node
	parentNode, err := DeserializeInternalNode(parentBlock, keySize)
	if err != nil {
		return nil, -1, err
	}

	// Find the child in the pointers array
	childIndex := -1
	for i, pointer := range parentNode.Pointers {
		if pointer == childBlockNumber {
			childIndex = i
			break
		}
	}

	if childIndex == -1 {
		return nil, -1, errors.New("child block not found in parent")
	}

	// If child is at position 0, it's the leftmost child with no separator key
	if childIndex == 0 {
		return nil, -1, nil
	}

	// Otherwise, the separator key is at Keys[childIndex-1]
	keyIndex := childIndex - 1
	return parentNode.Keys[keyIndex], keyIndex, nil
}

/*
RemoveKeyFromParent removes a key/pointer pair from a parent internal node when a child becomes empty.
Parameters:
- idx: pointer to Index object
- path: path from root to the child (from FindLeafBlock)
- childBlockNumber: block number of the child being removed
Returns:
- error: if any

This function removes the separator key and pointer corresponding to the child from the parent.
Stage 7: Now includes recursive handling - if parent becomes empty, it's removed from grandparent.
If the root becomes empty (single child remaining), that child becomes the new root (tree height reduction).
*/
func RemoveKeyFromParent(idx *Index, path []uint16, childBlockNumber uint16) error {
	if len(path) < 2 {
		// No parent (single-node tree)
		return nil
	}

	// Get parent block number from path
	parentBlockNum := path[len(path)-2]

	// Read parent block
	parentBlock, err := ReadIndexBlock(idx, int(parentBlockNum))
	if err != nil {
		return ErrBlockReadFailed
	}

	// Find the separator key for this child
	keySize := idx.Codec.Size()
	separatorKey, _, err := FindParentKeyForChild(parentBlock, childBlockNumber, keySize)
	if err != nil {
		return err
	}

	// DEBUG: Log what we're about to do
	// fmt.Printf("RemoveKeyFromParent: removing child %d from parent %d (separator key: %v)\n",
	//	childBlockNumber, parentBlockNum, separatorKey)

	// Deserialize parent to modify it
	parentNode, err := DeserializeInternalNode(parentBlock, keySize)
	if err != nil {
		return err
	}

	// If separatorKey is nil, child was leftmost - need to remove pointer directly
	if separatorKey == nil {
		// Child is leftmost - remove Pointers[0] and Keys[0]
		if len(parentNode.Pointers) > 0 && parentNode.Pointers[0] == childBlockNumber {
			parentNode.Pointers = parentNode.Pointers[1:]

			// Also remove the first key (the separator for the new leftmost child)
			if parentNode.KeyCount > 0 {
				parentNode.Keys = parentNode.Keys[1:]
				parentNode.KeyCount--
			}
		}
	} else {
		// Child is not leftmost - use DeleteKeyAndBlockPointer on fresh parent block
		// Re-serialize the parent node first
		parentBlock, err = SerializeInternalNode(parentNode, int(idx.Header.BlockSize), keySize)
		if err != nil {
			return err
		}

		err = DeleteKeyAndBlockPointer(parentBlock, separatorKey, idx.Codec, keySize)
		if err != nil {
			return err
		}

		// Re-deserialize to get updated node
		parentNode, err = DeserializeInternalNode(parentBlock, keySize)
		if err != nil {
			return err
		}
	}

	// Check if parent is now empty (KeyCount == 0)
	if parentNode.KeyCount == 0 {
		// Parent is empty - check if it's the root
		if len(path) == 2 {
			// Parent is root - tree height reduction
			// The one remaining child becomes the new root
			if len(parentNode.Pointers) != 1 {
				return errors.New("empty root should have exactly 1 pointer")
			}

			newRootBlockNum := parentNode.Pointers[0]

			// Check if the new root is a leaf - if so, clear its chain pointers
			newRootBlock, err := ReadIndexBlock(idx, int(newRootBlockNum))
			if err != nil {
				return err
			}

			if newRootBlock[2] == NodeTypeLeaf {
				// New root is a leaf - it should be the only leaf with no chain
				newRootLeaf, err := DeserializeLeafNode(newRootBlock, idx.Codec, int(idx.Header.ValueSize))
				if err != nil {
					return err
				}

				// Clear leaf chain pointers - this is now a standalone root leaf
				newRootLeaf.NextLeaf = NoNextLeaf
				newRootLeaf.PrevLeaf = NoPrevLeaf

				// Serialize and write back
				newRootBlock, err = SerializeLeafNode(newRootLeaf, int(idx.Header.BlockSize), idx.Codec, int(idx.Header.ValueSize))
				if err != nil {
					return err
				}

				err = WriteIndexBlock(idx, int(newRootBlockNum), newRootBlock)
				if err != nil {
					return err
				}

				// Update FirstLeaf to point to root
				idx.Header.FirstLeaf = newRootBlockNum
			}

			idx.Header.RootNode = newRootBlockNum

			// Convert old root to empty node
			err = ConvertInternalToEmptyNode(idx, parentBlockNum)
			if err != nil {
				return err
			}

			// Write updated header to file
			headerData, err := SerializeIndexHeader(idx.Header)
			if err != nil {
				return err
			}
			_, err = idx.File.WriteAt(headerData, 0)
			if err != nil {
				return ErrHeaderWrite
			}

			return nil
		} else {
			// Parent is not root and has KeyCount==0 (degenerate: 1 pointer, 0 keys)
			// We need to "bypass" this parent by replacing it with its sole child in the grandparent

			if len(parentNode.Pointers) != 1 {
				return errors.New("empty non-root parent should have exactly 1 pointer")
			}

			soleChildBlock := parentNode.Pointers[0]

			// Replace parentBlockNum with soleChildBlock in the grandparent
			// This requires finding parentBlockNum in grandparent and replacing its pointer

			if len(path) < 3 {
				// Grandparent doesn't exist (parent is direct child of root)
				// This shouldn't happen because len(path)==2 means parent IS root
				return errors.New("unexpected: non-root parent with no grandparent")
			}

			grandparentBlockNum := path[len(path)-3]
			grandparentBlock, err := ReadIndexBlock(idx, int(grandparentBlockNum))
			if err != nil {
				return err
			}

			grandparentNode, err := DeserializeInternalNode(grandparentBlock, keySize)
			if err != nil {
				return err
			}

			// Find parentBlockNum in grandparent's pointers and replace with soleChildBlock
			replaced := false
			for i, ptr := range grandparentNode.Pointers {
				if ptr == parentBlockNum {
					grandparentNode.Pointers[i] = soleChildBlock
					replaced = true
					break
				}
			}

			if !replaced {
				return errors.New("parent block not found in grandparent pointers")
			}

			// Write updated grandparent back
			grandparentBlock, err = SerializeInternalNode(grandparentNode, int(idx.Header.BlockSize), keySize)
			if err != nil {
				return err
			}

			err = WriteIndexBlock(idx, int(grandparentBlockNum), grandparentBlock)
			if err != nil {
				return err
			}

			// Now convert the bypassed parent to EmptyNode
			err = ConvertInternalToEmptyNode(idx, parentBlockNum)
			if err != nil {
				return err
			}

			return nil
		}
	}

	// Parent still has keys - serialize and write it back
	parentBlock, err = SerializeInternalNode(parentNode, int(idx.Header.BlockSize), keySize)
	if err != nil {
		return err
	}

	err = WriteIndexBlock(idx, int(parentBlockNum), parentBlock)
	if err != nil {
		return ErrBlockWriteFailed
	}

	return nil
}
