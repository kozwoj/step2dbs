package dicindex128

import "errors"

/*
PropagateSplit propagates a split key up the tree using the traversal path.
This function handles splitting internal nodes iteratively and creating a new root if needed.

Parameters:
- path: []uint16 traversal path from root to the node that was split (last element is the split node)
- splitKey: Hash128 key to propagate up
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
func (idx *Index) PropagateSplit(path []uint16, splitKey Hash128, newBlockNumber uint16) error {
	// If path has only one element (root is also leaf - shouldn't happen in B+ tree), return error
	if len(path) < 2 {
		return errors.New("invalid path: cannot propagate split from root")
	}

	// Start from the parent of the split node
	// path[len-1] is the split node, path[len-2] is its parent
	for i := len(path) - 2; i >= 0; i-- {
		parentBlockNumber := path[i]
		parentBlock, err := ReadIndexBlock(idx, int(parentBlockNumber))
		if err != nil {
			return err
		}

		// Check if parent is full
		isFull, err := IsInternalBlockFull(parentBlock)
		if err != nil {
			return err
		}

		if !isFull {
			// Parent has space - insert the split key and we're done
			err = InsertKeyAndBlockPointer(parentBlock, splitKey, newBlockNumber)
			if err != nil {
				return err
			}
			// Write updated parent back to disk
			err = WriteIndexBlock(idx, int(parentBlockNumber), parentBlock)
			if err != nil {
				return err
			}
			return nil
		} else {
			// Parent is full - need to split it
			// Allocate a new block for the right half of the parent
			newParentBlockNumber := idx.Header.NextEmptyBlock
			newParentBlock := make([]byte, idx.Header.BlockSize)

			// Split the parent node first
			middleKey, err := SplitInternalNodeBlock(parentBlock, newParentBlock, newParentBlockNumber)
			if err != nil {
				return err
			}

			// Determine which half gets the new key
			if CompareHash128(splitKey, middleKey) < 0 {
				// Insert into left (original) parent
				err = InsertKeyAndBlockPointer(parentBlock, splitKey, newBlockNumber)
				if err != nil {
					return err
				}
			} else {
				// Insert into right (new) parent
				err = InsertKeyAndBlockPointer(newParentBlock, splitKey, newBlockNumber)
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

			// Update NextEmptyBlock
			idx.Header.NextEmptyBlock++

			// Check if we just split the root
			if i == 0 {
				// We split the root - need to create a new root
				newRootBlockNumber := idx.Header.NextEmptyBlock
				newRootBlock := make([]byte, idx.Header.BlockSize)

				// Create a new root with one key (middleKey) and two pointers
				newRoot := &InternalNode{
					BlockNumber: newRootBlockNumber,
					NodeType:    InternalNodeType,
					KeyCount:    1,
					Keys:        []Hash128{middleKey},
					Pointers:    []uint16{parentBlockNumber, newParentBlockNumber},
				}

				err = SerializeInternalNode(newRoot, newRootBlock)
				if err != nil {
					return err
				}

				// Write new root to disk
				err = WriteIndexBlock(idx, int(newRootBlockNumber), newRootBlock)
				if err != nil {
					return err
				}

				// Update header
				idx.Header.RootNode = newRootBlockNumber
				idx.Header.NextEmptyBlock++

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
			} else {
				// Not the root - continue propagating up with middleKey
				splitKey = middleKey
				newBlockNumber = newParentBlockNumber
				// Continue the loop to propagate to the next parent
			}
		}
	}

	return nil
}

/*
FindLeafBlock finds the leaf node block where the key should be located.
Parameters:
- key: Hash128 hash key
Returns:
- blockNumber: uint16 block number of the found leaf node
- path: []uint16 traversal path from root to leaf (inclusive)
- error: if any (e.g. key not found)
*/
func (idx *Index) FindLeafBlock(key Hash128) (blockNumber uint16, path []uint16, err error) {
	// Get root node
	rootBlockNumber := int(idx.Header.RootNode)
	path = []uint16{uint16(rootBlockNumber)}

	rootBlock, err := ReadIndexBlock(idx, rootBlockNumber)
	if err != nil {
		return 0, nil, err
	}

	// Deserialize root node
	rootNode, err := DeserializeInternalNode(rootBlock, idx.Header.BlockSize)
	if err != nil {
		return 0, nil, err
	}

	// If root node is empty, return the first leaf node
	if rootNode.KeyCount == 0 {
		leafBlockNum := rootNode.Pointers[0]
		path = append(path, leafBlockNum)
		return leafBlockNum, path, nil
	}

	// Traverse internal nodes to find the correct leaf node
	currentNode := rootNode
	childBlockNumber := FindChildBlock(idx, currentNode, key)
	path = append(path, childBlockNumber)

	// Continue traversing until a leaf node is reached
	for {
		block, err := ReadIndexBlock(idx, int(childBlockNumber))
		if err != nil {
			return 0, nil, err
		}

		// Check if it's a leaf node
		if IsLeafNode(block) {
			return childBlockNumber, path, nil
		}

		// If not a leaf, continue traversing
		internalNode, err := DeserializeInternalNode(block, idx.Header.BlockSize)
		if err != nil {
			return 0, nil, err
		}

		childBlockNumber = FindChildBlock(idx, internalNode, key)
		path = append(path, childBlockNumber)
	}
}

/*
FindChildBlock finds the child block number for a given key in an internal node.
Parameters:
- index: pointer to Index struct
- internalNode: pointer to InternalNode struct
- key: Hash128 key to search for
Returns:
- childBlockNumber: uint16 block number of the child node
*/
func FindChildBlock(index *Index, internalNode *InternalNode, key Hash128) uint16 {
	// Find the correct child pointer for the given key
	for i, k := range internalNode.Keys {
		if CompareHash128(key, k) < 0 {
			return internalNode.Pointers[i]
		}
	}
	// If key is greater than or equal to all keys, return the last pointer
	return internalNode.Pointers[len(internalNode.Pointers)-1]
}
