package dicindex128

import (
	"encoding/binary"

	"github.com/zeebo/xxh3"
)

// Hash128 represents a 128-bit hash value as two 64-bit unsigned integers.
type Hash128 struct {
	High uint64 // upper 64 bits
	Low  uint64 // lower 64 bits
}

// HashString128 computes the xxHash128 of a string.
func HashString128(s string) Hash128 {
	h := xxh3.HashString128(s)
	// xxh3.HashString128 returns a Uint128 with Lo and Hi fields
	return Hash128{High: h.Hi, Low: h.Lo}
}

// HashBytes128 computes the xxHash128 of a byte slice.
func HashBytes128(data []byte) Hash128 {
	h := xxh3.Hash128(data)
	return Hash128{High: h.Hi, Low: h.Lo}
}

// ToBytes converts Hash128 to a 16-byte array (big-endian).
func (h Hash128) ToBytes() [16]byte {
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[0:8], h.High)
	binary.BigEndian.PutUint64(buf[8:16], h.Low)
	return buf
}

// FromBytes creates a Hash128 from a 16-byte array (big-endian).
func FromBytes(data [16]byte) Hash128 {
	high := binary.BigEndian.Uint64(data[0:8])
	low := binary.BigEndian.Uint64(data[8:16])
	return Hash128{High: high, Low: low}
}

// CompareHash128 compares two 128-bit hash values using lexicographic ordering.
// Returns:
//
//	-1 if a < b
//	 0 if a == b
//	 1 if a > b
func CompareHash128(a, b Hash128) int {
	if a.High < b.High {
		return -1
	} else if a.High > b.High {
		return 1
	}
	// High bits are equal, compare low bits
	if a.Low < b.Low {
		return -1
	} else if a.Low > b.Low {
		return 1
	}
	return 0
}

// Equal returns true if two Hash128 values are equal.
func (h Hash128) Equal(other Hash128) bool {
	return h.High == other.High && h.Low == other.Low
}

// IsZero returns true if the hash is zero (both high and low are 0).
func (h Hash128) IsZero() bool {
	return h.High == 0 && h.Low == 0
}
