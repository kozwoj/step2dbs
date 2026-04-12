package primindex

import (
	"fmt"
	"strings"
	"testing"
)

// PrintTreeStructure prints a comprehensive view of the entire tree structure
// including header info, empty chain, and all nodes with their keys
func PrintTreeStructure(t *testing.T, idx *Index, label string) {
	t.Helper()

	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Printf("TREE STRUCTURE: %s\n", label)
	fmt.Println(strings.Repeat("=", 80))

	// Header information
	fmt.Printf("\n[HEADER INFO]\n")
	fmt.Printf("  BlockSize:       %d bytes\n", idx.Header.BlockSize)
	fmt.Printf("  FileLength:      %d blocks\n", idx.Header.FileLength)
	fmt.Printf("  KeyType:         %d\n", idx.Header.KeyType)
	fmt.Printf("  ValueSize:       %d bytes\n", idx.Header.ValueSize)
	fmt.Printf("  RootNode:        Block %d\n", idx.Header.RootNode)
	fmt.Printf("  FirstLeaf:       Block %d\n", idx.Header.FirstLeaf)
	fmt.Printf("  NextEmptyBlock:  Block %d\n", idx.Header.NextEmptyBlock)

	// Empty chain
	fmt.Printf("\n[EMPTY CHAIN - Reusable Blocks]\n")
	emptyBlocks := []uint16{}
	current := idx.Header.NextEmptyBlock
	visited := make(map[uint16]bool)

	for current != NoNextEmptyBlock && len(emptyBlocks) < 100 {
		if visited[current] {
			fmt.Printf("  WARNING: CYCLE DETECTED at block %d\n", current)
			break
		}
		visited[current] = true
		emptyBlocks = append(emptyBlocks, current)

		block, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			fmt.Printf("  WARNING: Block %d: read error: %v\n", current, err)
			break
		}

		if block[2] != NodeTypeEmpty {
			fmt.Printf("  WARNING: Block %d: not empty type (type=%d)\n", current, block[2])
			break
		}

		emptyNode, err := DeserializeEmptyNode(block)
		if err != nil {
			fmt.Printf("  WARNING: Block %d: deserialize error: %v\n", current, err)
			break
		}

		current = emptyNode.NextEmptyBlock
	}

	if len(emptyBlocks) == 0 {
		fmt.Printf("  (none)\n")
	} else {
		fmt.Printf("  Blocks: %v\n", emptyBlocks)
		fmt.Printf("  Count: %d blocks available\n", len(emptyBlocks))
	}

	// Tree structure - starting from root
	fmt.Printf("\n[TREE STRUCTURE]\n")
	rootBlock, err := ReadIndexBlock(idx, int(idx.Header.RootNode))
	if err != nil {
		fmt.Printf("  WARNING: Failed to read root block %d: %v\n", idx.Header.RootNode, err)
		return
	}

	nodeType := rootBlock[2]

	if nodeType == NodeTypeLeaf {
		fmt.Printf("\n  ROOT (Block %d) - LEAF NODE:\n", idx.Header.RootNode)
		printLeafNode(t, idx, idx.Header.RootNode, rootBlock, "    ")
	} else if nodeType == NodeTypeInternal {
		fmt.Printf("\n  ROOT (Block %d) - INTERNAL NODE:\n", idx.Header.RootNode)
		printInternalNodeRecursive(t, idx, idx.Header.RootNode, rootBlock, "    ", 0)
	} else {
		fmt.Printf("  WARNING: Root has invalid type: %d\n", nodeType)
	}

	// Leaf chain
	fmt.Printf("\n[LEAF CHAIN - Forward Traversal]\n")
	leafChain := []uint16{}
	current = idx.Header.FirstLeaf
	visited = make(map[uint16]bool)

	for current != NoNextLeaf && len(leafChain) < 100 {
		if visited[current] {
			fmt.Printf("  WARNING: CYCLE DETECTED at block %d\n", current)
			break
		}
		visited[current] = true
		leafChain = append(leafChain, current)

		block, err := ReadIndexBlock(idx, int(current))
		if err != nil {
			fmt.Printf("  WARNING: Block %d: read error: %v\n", current, err)
			break
		}

		if block[2] != NodeTypeLeaf {
			fmt.Printf("  WARNING: Block %d: not leaf type (type=%d)\n", current, block[2])
			break
		}

		leafNode, err := DeserializeLeafNode(block, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			fmt.Printf("  WARNING: Block %d: deserialize error: %v\n", current, err)
			break
		}

		current = leafNode.NextLeaf
	}

	fmt.Printf("  Blocks: %v\n", leafChain)
	fmt.Printf("  Count: %d leaves\n", len(leafChain))

	fmt.Println()
	fmt.Println(strings.Repeat("=", 80))
	fmt.Println()
}

// printLeafNode prints detailed information about a leaf node
func printLeafNode(t *testing.T, idx *Index, blockNum uint16, blockData []byte, indent string) {
	t.Helper()

	leafNode, err := DeserializeLeafNode(blockData, idx.Codec, int(idx.Header.ValueSize))
	if err != nil {
		fmt.Printf("%sWARNING: Deserialize error: %v\n", indent, err)
		return
	}

	fmt.Printf("%sEntryCount: %d\n", indent, leafNode.EntryCount)
	fmt.Printf("%sNextLeaf:   %d", indent, leafNode.NextLeaf)
	if leafNode.NextLeaf == NoNextLeaf {
		fmt.Printf(" (end)\n")
	} else {
		fmt.Printf("\n")
	}
	fmt.Printf("%sPrevLeaf:   %d", indent, leafNode.PrevLeaf)
	if leafNode.PrevLeaf == NoPrevLeaf {
		fmt.Printf(" (start)\n")
	} else {
		fmt.Printf("\n")
	}

	if leafNode.EntryCount == 0 {
		fmt.Printf("%sKeys: (empty)\n", indent)
	} else {
		fmt.Printf("%sKeys: [", indent)
		for i := 0; i < int(leafNode.EntryCount); i++ {
			if i > 0 {
				fmt.Printf(", ")
			}
			fmt.Printf("%v", leafNode.Entries[i].Key)

			// Limit output for large leaf nodes
			if i >= 9 && int(leafNode.EntryCount) > 10 {
				fmt.Printf(", ... (%d more)", int(leafNode.EntryCount)-10)
				break
			}
		}
		fmt.Printf("]\n")
	}
}

// printInternalNodeRecursive prints an internal node and recursively prints its children
func printInternalNodeRecursive(t *testing.T, idx *Index, blockNum uint16, blockData []byte, indent string, depth int) {
	t.Helper()

	// Prevent infinite recursion
	if depth > 10 {
		fmt.Printf("%sWARNING: Max depth reached\n", indent)
		return
	}

	internalNode, err := DeserializeInternalNode(blockData, idx.Codec.Size())
	if err != nil {
		fmt.Printf("%sWARNING: Deserialize error: %v\n", indent, err)
		return
	}

	fmt.Printf("%sKeyCount: %d\n", indent, internalNode.KeyCount)

	// Print separator keys
	if internalNode.KeyCount > 0 {
		fmt.Printf("%sSeparator Keys: [", indent)
		for i, key := range internalNode.Keys {
			if i > 0 {
				fmt.Printf(", ")
			}
			keyVal, _ := idx.Codec.Deserialize(key)
			fmt.Printf("%v", keyVal)
		}
		fmt.Printf("]\n")
	}

	// Print pointers
	fmt.Printf("%sPointers: %v (count=%d)\n", indent, internalNode.Pointers, len(internalNode.Pointers))

	// Check for duplicate pointers
	seen := make(map[uint16]bool)
	for _, ptr := range internalNode.Pointers {
		if seen[ptr] {
			fmt.Printf("%sWARNING: DUPLICATE POINTER: %d\n", indent, ptr)
		}
		seen[ptr] = true
	}

	// Print each child
	fmt.Printf("%sChildren:\n", indent)
	for i, ptr := range internalNode.Pointers {
		childBlock, err := ReadIndexBlock(idx, int(ptr))
		if err != nil {
			fmt.Printf("%s  [%d] Block %d: WARNING - read error: %v\n", indent, i, ptr, err)
			continue
		}

		childType := childBlock[2]

		if childType == NodeTypeLeaf {
			fmt.Printf("%s  [%d] Block %d - LEAF:\n", indent, i, ptr)
			printLeafNode(t, idx, ptr, childBlock, indent+"      ")
		} else if childType == NodeTypeInternal {
			fmt.Printf("%s  [%d] Block %d - INTERNAL:\n", indent, i, ptr)
			printInternalNodeRecursive(t, idx, ptr, childBlock, indent+"      ", depth+1)
		} else {
			fmt.Printf("%s  [%d] Block %d: WARNING - invalid type %d\n", indent, i, ptr, childType)
		}
	}
}
