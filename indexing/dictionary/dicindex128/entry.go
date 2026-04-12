package dicindex128

import (
	"encoding/binary"
	"errors"
)

var (
	ErrEntryTooShort = errors.New("index entry data too short")
)

// IndexEntry128 represents a dictionary index entry using 128-bit hash.
// Fixed-length: 24 bytes total (16 bytes hash + 4 bytes dictID + 4 bytes postingsRef)
// No string field - 128-bit hash is unique enough to avoid collisions.
type IndexEntry128 struct {
	Hash        Hash128 // 16 bytes (8 high + 8 low)
	DictID      uint32  // 4 bytes
	PostingsRef uint32  // 4 bytes
}

// EncodeIndexEntry128 encodes an entry as [16 bytes hash][4 bytes dictID][4 bytes postingsRef].
// Total: 24 bytes fixed-length.
func EncodeIndexEntry128(entry *IndexEntry128) []byte {
	entryBytes := make([]byte, 24)

	// Encode hash (16 bytes)
	binary.LittleEndian.PutUint64(entryBytes[0:8], entry.Hash.High)
	binary.LittleEndian.PutUint64(entryBytes[8:16], entry.Hash.Low)

	// Encode dictID (4 bytes)
	binary.LittleEndian.PutUint32(entryBytes[16:20], entry.DictID)

	// Encode postingsRef (4 bytes)
	binary.LittleEndian.PutUint32(entryBytes[20:24], entry.PostingsRef)

	return entryBytes
}

// DecodeIndexEntry128 decodes a byte slice into IndexEntry128.
// Expects exactly 24 bytes.
func DecodeIndexEntry128(data []byte) (*IndexEntry128, error) {
	if len(data) < 24 {
		return nil, ErrEntryTooShort
	}

	// Decode hash (16 bytes)
	hashHigh := binary.LittleEndian.Uint64(data[0:8])
	hashLow := binary.LittleEndian.Uint64(data[8:16])

	// Decode dictID (4 bytes)
	dictID := binary.LittleEndian.Uint32(data[16:20])

	// Decode postingsRef (4 bytes)
	postingsRef := binary.LittleEndian.Uint32(data[20:24])

	return &IndexEntry128{
		Hash:        Hash128{High: hashHigh, Low: hashLow},
		DictID:      dictID,
		PostingsRef: postingsRef,
	}, nil
}

// EntrySize returns the fixed size of an encoded IndexEntry128 (24 bytes).
func EntrySize() int {
	return 24
}
