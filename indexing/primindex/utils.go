package primindex

import "errors"

var (
	ErrInvalidPath     = errors.New("invalid path: cannot propagate split from root")
	ErrHeaderWriteFail = errors.New("failed to write header to disk")
)

/* ------------------------ Utility Functions -----------------------

- PropagateSplit: propagates a split key up the B+ tree

- FindLeafBlock: finds the leaf block for a given key and returns the
traversal path

---------------------------------------------------------------------*/

/*
PropagateSplit propagates a split key up the tree using the traversal path.
This function handles splitting internal nodes iteratively and creating a new root if needed.

Parameters:
- idx: pointer to Index struct
- path: []uint16 traversal path from root to the node that was split (last element is the split node)
- splitKey: []byte key to propagate up
- newBlockNumber: uint16 block number of the new right sibling created by the split

Returns:
- error: if any (e.g. failed to read/write blocks)

Algorithm:
1. Start from the parent of the split node (second to last in path)
2. Try to insert splitKey and newBlockNumber into parent
3. If parent is full:
   a. Split the parent node
   b. Continue iterating up the path to propagate the parent's split key
4. If we're at the root and it splits:
   a. Create a new root node
   b. Update header's RootNode pointer
*/
func (idx *Index) PropagateSplit(path []uint16, splitKey []byte, newBlockNumber uint16) error {
	// If path has only one element (root is also leaf), return error
	if len(path) < 2 {
		return ErrInvalidPath
	}

	keySize := idx.Codec.Size()

	// Start from the parent of the split node
	// path[len-1] is the split node, path[len-2] is its parent
	for i := len(path) - 2; i >= 0; i-- {
		parentBlockNumber := path[i]
		parentBlock, err := ReadIndexBlock(idx, int(parentBlockNumber))
		if err != nil {
			return err
		}

		// Check if parent is full
		isFull, err := IsInternalBlockFull(parentBlock, keySize)
		if err != nil {
			return err
		}

		if !isFull {
			// Parent has space - insert the split key and we're done
			err = InsertKeyAndBlockPointer(parentBlock, splitKey, newBlockNumber, idx.Codec, keySize)
			if err != nil {
				return err
			}
			// Write updated parent back to disk
			err = WriteIndexBlock(idx, int(parentBlockNumber), parentBlock)
			if err != nil {
				return err
			}
			return nil
		}

		// Parent is full - need to split it
		// Allocate a new block for the right half of the parent
		newParentBlockNumber, err := GetEmptyBlock(idx)
		if err != nil {
			return err
		}
		newParentBlock := make([]byte, idx.Header.BlockSize)

		// Split the parent node first
		middleKey, err := SplitInternalNodeBlock(parentBlock, newParentBlock, newParentBlockNumber, keySize)
		if err != nil {
			return err
		}

		// Determine which half gets the new key
		if idx.Codec.Compare(splitKey, middleKey) < 0 {
			// Insert into left (original) parent
			err = InsertKeyAndBlockPointer(parentBlock, splitKey, newBlockNumber, idx.Codec, keySize)
			if err != nil {
				return err
			}
		} else {
			// Insert into right (new) parent
			err = InsertKeyAndBlockPointer(newParentBlock, splitKey, newBlockNumber, idx.Codec, keySize)
			if err != nil {
				return err
			}
		}

		// Write both blocks to disk
		err = WriteIndexBlock(idx, int(parentBlockNumber), parentBlock)
		if err != nil {
			return err
		}
		err = WriteIndexBlock(idx, int(newParentBlockNumber), newParentBlock)
		if err != nil {
			return err
		}

		// Check if we just split the root
		if i == 0 {
			// We split the root - need to create a new root
			newRootBlockNumber, err := GetEmptyBlock(idx)
			if err != nil {
				return err
			}
			newRootBlock := make([]byte, idx.Header.BlockSize)

			// Create a new root with one key (middleKey) and two pointers
			newRoot := &InternalNode{
				BlockNumber: newRootBlockNumber,
				NodeType:    1, // InternalNodeType
				KeyCount:    1,
				Keys:        [][]byte{middleKey},
				Pointers:    []uint16{parentBlockNumber, newParentBlockNumber},
			}

			// Serialize the new root into the block
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
				return ErrHeaderWriteFail
			}

			return nil
		}

		// Not the root - continue propagating up with middleKey
		splitKey = middleKey
		newBlockNumber = newParentBlockNumber
		// Continue the loop to propagate to the next parent
	}

	return nil
}

/*
FindLeafBlock finds the leaf node block where the key should be located.
This function traverses the B+ tree from root to leaf, tracking the path.

Parameters:
- idx: pointer to Index struct
- key: key value to search for (type depends on KeyType)

Returns:
- blockNumber: uint16 block number of the found leaf node
- path: []uint16 traversal path from root to leaf (inclusive)
- error: if any (e.g. failed to read blocks)

Algorithm:
1. Start at root node
2. For each internal node, find the correct child pointer
3. Continue until a leaf node is reached
4. Return the leaf block number and the complete path
*/
func (idx *Index) FindLeafBlock(key interface{}) (blockNumber uint16, path []uint16, err error) {
	// Serialize the key for comparison
	keyBytes, err := idx.Codec.Serialize(key)
	if err != nil {
		return 0, nil, err
	}

	keySize := idx.Codec.Size()

	// Start at root node
	rootBlockNumber := idx.Header.RootNode
	path = []uint16{rootBlockNumber}

	rootBlock, err := ReadIndexBlock(idx, int(rootBlockNumber))
	if err != nil {
		return 0, nil, err
	}

	// Check if root is a leaf
	if IsLeafNode(rootBlock) {
		return rootBlockNumber, path, nil
	}

	// Root is an internal node - traverse down
	currentBlockNumber := rootBlockNumber
	currentBlock := rootBlock

	for {
		// Check if current block is a leaf
		if IsLeafNode(currentBlock) {
			return currentBlockNumber, path, nil
		}

		// Current block is internal - find the correct child
		childBlockNumber, err := FindChildBlockPointer(currentBlock, keyBytes, idx.Codec, keySize)
		if err != nil {
			return 0, nil, err
		}

		path = append(path, childBlockNumber)

		// Read the child block
		childBlock, err := ReadIndexBlock(idx, int(childBlockNumber))
		if err != nil {
			return 0, nil, err
		}

		// Move to child
		currentBlockNumber = childBlockNumber
		currentBlock = childBlock
	}
}
