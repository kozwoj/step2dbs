# KeyCodec‑Based Primary Index Design

This document describes the design for a disk‑backed primary index that supports multiple fixed‑width key types. The index is stored in a block file and uses a B+‑tree internally. 

## Primary Keys

The index primary key can be one of the following types: `uint8`, `uint16`, `uint32`, `uint64`, signed integers (`int16`, `int32`, `int64`), and `fixed‑size byte arrays` (4–32 bytes). Every key type implements the **KeyCodec** interface

```go
package primindex

// KeyCodec defines how keys are handled inside the index.
type KeyCodec interface {
    Serialize(key interface{}) ([]byte, error)
    Deserialize(data []byte) (interface{}, error)
    Compare(a, b []byte) int
    Size() int
}
```

Here is how the key types implement that interface:

### Uint8Codec

```go
import (
    "encoding/binary"
    "errors"
    "bytes"
)

type Uint8Codec struct{}

func (c Uint8Codec) Serialize(key interface{}) ([]byte, error) {
    k, ok := key.(uint8)
    if !ok {
        return nil, errors.New("Uint8Codec: invalid key type")
    }
    return []byte{byte(k)}, nil
}

func (c Uint8Codec) Deserialize(data []byte) (interface{}, error) {
    if len(data) != 1 {
        return nil, errors.New("Uint8Codec: invalid data length")
    }
    return uint8(data[0]), nil
}

func (c Uint8Codec) Compare(a, b []byte) int {
    return int(a[0]) - int(b[0])
}

func (c Uint8Codec) Size() int { return 1 }
```

### Uint16Codec

```go
type Uint16Codec struct{}

func (c Uint16Codec) Serialize(key interface{}) ([]byte, error) {
    k, ok := key.(uint16)
    if !ok {
        return nil, errors.New("Uint16Codec: invalid key type")
    }
    buf := make([]byte, 2)
    binary.BigEndian.PutUint16(buf, k)
    return buf, nil
}

func (c Uint16Codec) Deserialize(data []byte) (interface{}, error) {
    if len(data) != 2 {
        return nil, errors.New("Uint16Codec: invalid data length")
    }
    return binary.BigEndian.Uint16(data), nil
}

func (c Uint16Codec) Compare(a, b []byte) int {
    va := binary.BigEndian.Uint16(a)
    vb := binary.BigEndian.Uint16(b)
    switch {
    case va < vb:
        return -1
    case va > vb:
        return 1
    default:
        return 0
    }
}

func (c Uint16Codec) Size() int { return 2 }
```

### Uint32Codec

```go
type Uint32Codec struct{}

func (c Uint32Codec) Serialize(key interface{}) ([]byte, error) {
    k, ok := key.(uint32)
    if !ok {
        return nil, errors.New("Uint32Codec: invalid key type")
    }
    buf := make([]byte, 4)
    binary.BigEndian.PutUint32(buf, k)
    return buf, nil
}

func (c Uint32Codec) Deserialize(data []byte) (interface{}, error) {
    if len(data) != 4 {
        return nil, errors.New("Uint32Codec: invalid data length")
    }
    return binary.BigEndian.Uint32(data), nil
}

func (c Uint32Codec) Compare(a, b []byte) int {
    va := binary.BigEndian.Uint32(a)
    vb := binary.BigEndian.Uint32(b)
    switch {
    case va < vb:
        return -1
    case va > vb:
        return 1
    default:
        return 0
    }
}

func (c Uint32Codec) Size() int { return 4 }
```

### Uint64Codec

```go
type Uint64Codec struct{}

func (c Uint64Codec) Serialize(key interface{}) ([]byte, error) {
    k, ok := key.(uint64)
    if !ok {
        return nil, errors.New("Uint64Codec: invalid key type")
    }
    buf := make([]byte, 8)
    binary.BigEndian.PutUint64(buf, k)
    return buf, nil
}

func (c Uint64Codec) Deserialize(data []byte) (interface{}, error) {
    if len(data) != 8 {
        return nil, errors.New("Uint64Codec: invalid data length")
    }
    return binary.BigEndian.Uint64(data), nil
}

func (c Uint64Codec) Compare(a, b []byte) int {
    va := binary.BigEndian.Uint64(a)
    vb := binary.BigEndian.Uint64(b)
    switch {
    case va < vb:
        return -1
    case va > vb:
        return 1
    default:
        return 0
    }
}

func (c Uint64Codec) Size() int { return 8 }
```

### FixedBytesCodec

```go
type FixedBytesCodec struct {
    length int
}

func (c FixedBytesCodec) Serialize(key interface{}) ([]byte, error) {
    k, ok := key.([]byte)
    if !ok || len(k) != c.length {
        return nil, errors.New("FixedBytesCodec: invalid key length")
    }
    result := make([]byte, c.length)
    copy(result, k)
    return result, nil
}

func (c FixedBytesCodec) Deserialize(data []byte) (interface{}, error) {
    if len(data) != c.length {
        return nil, errors.New("FixedBytesCodec: invalid data length")
    }
    result := make([]byte, c.length)
    copy(result, data)
    return result, nil
}

func (c FixedBytesCodec) Compare(a, b []byte) int {
    return bytes.Compare(a, b)
}

func (c FixedBytesCodec) Size() int { return c.length }
```
## Index File

The index is stored on disk in a block file. The file is first created, and then must be opened for use.

The file creation function takes the following arguments
- Path - directory path for the index
- BlockSize - the size of the index blocks
- BlockCount - number of preallocated blocks
- KeyType - one of the supported types uint8, uint16, ...
- ValueSize - the length (number of bytes) of the value part of the leaf node entry

The function creates the index file and writes a header at the beginning of the file. The header has the following structure

``` go
type IndexHeader struct {
	BlockSize      uint16
	FileLength     uint16 // initially == BlockCount
    KeyType        uint8
    ValueSize      uint32
	RootNode       uint16
	FirstLeaf      uint16
	NextEmptyBlock uint16 // next empty/reusable block number
}
```
Note: the index header is written at the beginning of the file and therefore the block 0 starts at the offset equal to the length of the serialized header. 

The NextEmptyBlock is the block number of the first empty/reusable block in a linked list of empty blocks. If a leaf or internal block becomes empty due to keys and entries deletes, the block is added at the beginning of that linked list. 

There are three kinds of nodes in the index file with the corresponding blocks. 

### LeafNode block 
This is the B+-tree leaf node block that holds the index entries. Each entry contains:
- Key: the actual key value (typed according to KeyCodec - uint8, uint16, etc.)
- Value: ValueSize byte array with the opaque value associated with the key

The IndexEntry structure is defined as:
``` go
type IndexEntry struct {
	Key   interface{} // actual key value (will be serialized using KeyCodec)
	Value []byte      // value data (fixed size defined in header)
}
```

The block stores the serialized value of the following structure:
``` go
type LeafNode struct {
	BlockNumber uint16
	NodeType    uint8 // == 2 for leaf node
	EntryCount  uint16
	NextLeaf    uint16
	PrevLeaf    uint16
	Entries     []*IndexEntry
}
```

When serialized to disk, each entry is encoded as:
- keySize bytes: serialized key using KeyCodec.Serialize()
- valueSize bytes: opaque value data

The entry encoding/decoding functions are:
``` go
// EncodeIndexEntry encodes an entry using the KeyCodec
func EncodeIndexEntry(entry *IndexEntry, codec KeyCodec) ([]byte, error)

// DecodeIndexEntry decodes a byte slice into IndexEntry
func DecodeIndexEntry(data []byte, codec KeyCodec, valueSize int) (*IndexEntry, error)
```
The NextLeaf and PrevLeaf are, respectively, pointers to the next and previous leaf blocks in the linked list of leaf nodes. Traversing that list, 
starting at the file header, will provide sequential access to the keys in their ascending order.  

### InternalNode block
This is the B+-tree internal node block and holds index keys and references to  to the children nodes. The block stores serialized value of the following structure
``` go
type InternalNode struct {
	BlockNumber uint16
	NodeType    uint8   // == 1 for internal node
	KeyCount    uint16
	Keys        [][]byte // this is a slice of serialized key values
	Pointers    []uint16  // child block pointers
}
```

### EmptyBlock 
This is a empty/reusable node/block and contains the following serialized structure
``` go
type EmptyNode struct {
	BlockNumber    uint16
	NodeType       uint8    // == 3 for reusable block
	NextEmptyBlock uint16   // next empty/reusable block number
}
```

## Index Object
The index file is first created by a separate function. This function closes the index file, and before it can be
used, it needs to be opened. The open function returns the following index object

``` go
type Index struct {
	Header *IndexHeader
    Codec  KeyCodec
	File   *os.File     // the index file handle 
}
```

... where the Codec is returned by the KeyCodecFactory based on the value of KeyType in the index header. 

``` go
type KeyType uint8

const (
    KeyTypeUint8 KeyType = iota + 1
    KeyTypeUint16
    KeyTypeUint32
    KeyTypeUint64
    KeyTypeSMALLINT      // int16
    KeyTypeINT           // int32
    KeyTypeBIGINT        // int64
    KeyTypeBytes4
    KeyTypeBytes5
    // ... KeyTypeBytes6 through KeyTypeBytes31
    KeyTypeBytes32
    KeyTypePrefixBytes8  // Prefix key with 8-byte prefix + 4-byte dictID
)

func KeyCodecFactory(t KeyType) KeyCodec {
    switch t {
    case KeyTypeUint8:
        return Uint8Codec{}
    case KeyTypeUint16:
        return Uint16Codec{}
    case KeyTypeUint32:
        return Uint32Codec{}
    case KeyTypeUint64:
        return Uint64Codec{}
    case KeyTypeSMALLINT:
        return Int16Codec{}
    case KeyTypeINT:
        return Int32Codec{}
    case KeyTypeBIGINT:
        return Int64Codec{}
    case KeyTypeBytes4:
        return FixedBytesCodec{length: 4}
    // ... KeyTypeBytes5 through KeyTypeBytes32 follow the same pattern
    case KeyTypePrefixBytes8:
        return PrefixKeyCodec{prefixLength: 8}
    default:
        panic("unsupported key type")
    }
}
```

The signed integer codecs (`Int16Codec`, `Int32Codec`, `Int64Codec`) follow the same pattern as their unsigned counterparts but use signed type assertions and comparisons. The `PrefixKeyCodec` is used for prefix search functionality and is documented separately in PrefixSearch.md.
The Index objects implements the standard index methods
- Insert : insert entry for the given key value
- Find : find an entry given for the given key value
- Delete : delete entry given the given key value
- Close : close an index

## Handling Delete Operation
The index takes a specific approach to handling index file blocks that become empty. When a leaf node becomes empty, it
is converted to an empty node and added to the linked list of reusable/empty nodes starting at the header of the index
file. If, as a result of deleting multiple leaf nodes, an internal nodes becomes empty, its parent is updates and the
node is also converted to an empty node. 

If all keys are deleted from an index instance, it goes back to its initial state where the root is a an empty leaf node. 
However, unlike the initial state, all blocks that were used for leaf and internal nodes will be lined as empty and reusable 
nodes, so the index can grow again without increasing the size of the index file. 

