package primindex

import (
	"encoding/binary"
)

// EmptyNode represents an empty/reusable block in the index file
type EmptyNode struct {
	BlockNumber    uint16
	NodeType       uint8  // == 3 for empty/reusable block
	NextEmptyBlock uint16 // next empty/reusable block number
}

/*
SerializeEmptyNode serializes an EmptyNode into a byte slice for storage.
Parameters:
- node: pointer to EmptyNode struct
- blockSize: size of the block in bytes
Returns:
- []byte: serialized byte slice of length blockSize
- error: if any
*/
func SerializeEmptyNode(node *EmptyNode, blockSize int) ([]byte, error) {
	buf := make([]byte, blockSize)

	// Header: BlockNumber (2) + NodeType (1) + NextEmptyBlock (2) = 5 bytes
	binary.LittleEndian.PutUint16(buf[0:2], node.BlockNumber)
	buf[2] = node.NodeType
	binary.LittleEndian.PutUint16(buf[3:5], node.NextEmptyBlock)

	return buf, nil
}

/*
DeserializeEmptyNode deserializes a byte slice into an EmptyNode struct.
Parameters:
- data: byte slice of block data
Returns:
- *EmptyNode: deserialized EmptyNode struct
- error: if any (e.g. invalid node type)
*/
func DeserializeEmptyNode(data []byte) (*EmptyNode, error) {
	if len(data) < 5 {
		return nil, ErrBlockTooShort
	}

	node := &EmptyNode{}
	node.BlockNumber = binary.LittleEndian.Uint16(data[0:2])
	node.NodeType = data[2]
	node.NextEmptyBlock = binary.LittleEndian.Uint16(data[3:5])

	if node.NodeType != NodeTypeEmpty {
		return nil, ErrInvalidNodeType
	}

	return node, nil
}
