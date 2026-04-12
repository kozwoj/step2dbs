package primindex

import (
	"bytes"
	"encoding/binary"
	"errors"
)

// KeyCodec defines how keys are handled inside the index.
type KeyCodec interface {
	Serialize(key interface{}) ([]byte, error)
	Deserialize(data []byte) (interface{}, error)
	Compare(a, b []byte) int
	Size() int
}

// KeyType represents the type of key used in the index
type KeyType uint8

const (
	KeyTypeUint8 KeyType = iota + 1
	KeyTypeUint16
	KeyTypeUint32
	KeyTypeUint64
	KeyTypeSMALLINT // int16
	KeyTypeINT      // int32
	KeyTypeBIGINT   // int64
	KeyTypeBytes4
	KeyTypeBytes5
	KeyTypeBytes6
	KeyTypeBytes7
	KeyTypeBytes8
	KeyTypeBytes9
	KeyTypeBytes10
	KeyTypeBytes11
	KeyTypeBytes12
	KeyTypeBytes13
	KeyTypeBytes14
	KeyTypeBytes15
	KeyTypeBytes16
	KeyTypeBytes17
	KeyTypeBytes18
	KeyTypeBytes19
	KeyTypeBytes20
	KeyTypeBytes21
	KeyTypeBytes22
	KeyTypeBytes23
	KeyTypeBytes24
	KeyTypeBytes25
	KeyTypeBytes26
	KeyTypeBytes27
	KeyTypeBytes28
	KeyTypeBytes29
	KeyTypeBytes30
	KeyTypeBytes31
	KeyTypeBytes32
	KeyTypePrefixBytes8 // Prefix key with 8-byte prefix + 4-byte dictID
)

// KeyCodecFactory returns the appropriate KeyCodec based on the KeyType
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
	case KeyTypeBytes5:
		return FixedBytesCodec{length: 5}
	case KeyTypeBytes6:
		return FixedBytesCodec{length: 6}
	case KeyTypeBytes7:
		return FixedBytesCodec{length: 7}
	case KeyTypeBytes8:
		return FixedBytesCodec{length: 8}
	case KeyTypeBytes9:
		return FixedBytesCodec{length: 9}
	case KeyTypeBytes10:
		return FixedBytesCodec{length: 10}
	case KeyTypeBytes11:
		return FixedBytesCodec{length: 11}
	case KeyTypeBytes12:
		return FixedBytesCodec{length: 12}
	case KeyTypeBytes13:
		return FixedBytesCodec{length: 13}
	case KeyTypeBytes14:
		return FixedBytesCodec{length: 14}
	case KeyTypeBytes15:
		return FixedBytesCodec{length: 15}
	case KeyTypeBytes16:
		return FixedBytesCodec{length: 16}
	case KeyTypeBytes17:
		return FixedBytesCodec{length: 17}
	case KeyTypeBytes18:
		return FixedBytesCodec{length: 18}
	case KeyTypeBytes19:
		return FixedBytesCodec{length: 19}
	case KeyTypeBytes20:
		return FixedBytesCodec{length: 20}
	case KeyTypeBytes21:
		return FixedBytesCodec{length: 21}
	case KeyTypeBytes22:
		return FixedBytesCodec{length: 22}
	case KeyTypeBytes23:
		return FixedBytesCodec{length: 23}
	case KeyTypeBytes24:
		return FixedBytesCodec{length: 24}
	case KeyTypeBytes25:
		return FixedBytesCodec{length: 25}
	case KeyTypeBytes26:
		return FixedBytesCodec{length: 26}
	case KeyTypeBytes27:
		return FixedBytesCodec{length: 27}
	case KeyTypeBytes28:
		return FixedBytesCodec{length: 28}
	case KeyTypeBytes29:
		return FixedBytesCodec{length: 29}
	case KeyTypeBytes30:
		return FixedBytesCodec{length: 30}
	case KeyTypeBytes31:
		return FixedBytesCodec{length: 31}
	case KeyTypeBytes32:
		return FixedBytesCodec{length: 32}
	case KeyTypePrefixBytes8:
		return PrefixKeyCodec{prefixLength: 8}
	default:
		panic("unsupported key type")
	}
}

// Uint8Codec handles uint8 keys
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

// Uint16Codec handles uint16 keys
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

// Uint32Codec handles uint32 keys
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

// Uint64Codec handles uint64 keys
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

// Int16Codec handles int16 (SMALLINT) keys
type Int16Codec struct{}

func (c Int16Codec) Serialize(key interface{}) ([]byte, error) {
	k, ok := key.(int16)
	if !ok {
		return nil, errors.New("Int16Codec: invalid key type")
	}
	buf := make([]byte, 2)
	binary.BigEndian.PutUint16(buf, uint16(k))
	return buf, nil
}

func (c Int16Codec) Deserialize(data []byte) (interface{}, error) {
	if len(data) != 2 {
		return nil, errors.New("Int16Codec: invalid data length")
	}
	return int16(binary.BigEndian.Uint16(data)), nil
}

func (c Int16Codec) Compare(a, b []byte) int {
	va := int16(binary.BigEndian.Uint16(a))
	vb := int16(binary.BigEndian.Uint16(b))
	switch {
	case va < vb:
		return -1
	case va > vb:
		return 1
	default:
		return 0
	}
}

func (c Int16Codec) Size() int { return 2 }

// Int32Codec handles int32 (INT) keys
type Int32Codec struct{}

func (c Int32Codec) Serialize(key interface{}) ([]byte, error) {
	k, ok := key.(int32)
	if !ok {
		return nil, errors.New("Int32Codec: invalid key type")
	}
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(k))
	return buf, nil
}

func (c Int32Codec) Deserialize(data []byte) (interface{}, error) {
	if len(data) != 4 {
		return nil, errors.New("Int32Codec: invalid data length")
	}
	return int32(binary.BigEndian.Uint32(data)), nil
}

func (c Int32Codec) Compare(a, b []byte) int {
	va := int32(binary.BigEndian.Uint32(a))
	vb := int32(binary.BigEndian.Uint32(b))
	switch {
	case va < vb:
		return -1
	case va > vb:
		return 1
	default:
		return 0
	}
}

func (c Int32Codec) Size() int { return 4 }

// Int64Codec handles int64 (BIGINT) keys
type Int64Codec struct{}

func (c Int64Codec) Serialize(key interface{}) ([]byte, error) {
	k, ok := key.(int64)
	if !ok {
		return nil, errors.New("Int64Codec: invalid key type")
	}
	buf := make([]byte, 8)
	binary.BigEndian.PutUint64(buf, uint64(k))
	return buf, nil
}

func (c Int64Codec) Deserialize(data []byte) (interface{}, error) {
	if len(data) != 8 {
		return nil, errors.New("Int64Codec: invalid data length")
	}
	return int64(binary.BigEndian.Uint64(data)), nil
}

func (c Int64Codec) Compare(a, b []byte) int {
	va := int64(binary.BigEndian.Uint64(a))
	vb := int64(binary.BigEndian.Uint64(b))
	switch {
	case va < vb:
		return -1
	case va > vb:
		return 1
	default:
		return 0
	}
}

func (c Int64Codec) Size() int { return 8 }

// FixedBytesCodec handles fixed-size byte array keys
type FixedBytesCodec struct {
	length int
}

func (c FixedBytesCodec) Serialize(key interface{}) ([]byte, error) {
	k, ok := key.([]byte)
	if !ok || len(k) != c.length {
		return nil, errors.New("FixedBytesCodec: invalid key length")
	}
	// Return a copy to avoid aliasing issues
	result := make([]byte, c.length)
	copy(result, k)
	return result, nil
}

func (c FixedBytesCodec) Deserialize(data []byte) (interface{}, error) {
	if len(data) != c.length {
		return nil, errors.New("FixedBytesCodec: invalid data length")
	}
	// Return a copy to avoid aliasing issues
	result := make([]byte, c.length)
	copy(result, data)
	return result, nil
}

func (c FixedBytesCodec) Compare(a, b []byte) int {
	return bytes.Compare(a, b)
}

func (c FixedBytesCodec) Size() int { return c.length }

// ==================== PrefixKeyCodec ====================

// PrefixKey represents a fixed-size sortable key used for prefix search.
// Prefix is exactly L bytes long (padded with 0x00 for strings shorter than L).
type PrefixKey struct {
	Prefix []byte // length must match codec's prefixLength
	dictID uint32
}

// PrefixKeyCodec handles prefix-based keys for prefix search functionality.
// Keys are serialized as [Prefix(L bytes) | DictID (4 bytes, big-endian)]
type PrefixKeyCodec struct {
	prefixLength int
}

// Serialize encodes the key into a fixed-size byte slice.
// Format: [Prefix(L bytes) | dictID (4 bytes, big-endian)]
func (c PrefixKeyCodec) Serialize(key interface{}) ([]byte, error) {
	pk, ok := key.(PrefixKey)
	if !ok {
		return nil, errors.New("PrefixKeyCodec: invalid key type, expected PrefixKey")
	}

	if len(pk.Prefix) != c.prefixLength {
		return nil, errors.New("PrefixKeyCodec: invalid prefix length")
	}

	out := make([]byte, c.prefixLength+4)
	copy(out, pk.Prefix)
	binary.BigEndian.PutUint32(out[c.prefixLength:], pk.dictID)
	return out, nil
}

// Deserialize decodes a PrefixKey from a byte slice.
func (c PrefixKeyCodec) Deserialize(data []byte) (interface{}, error) {
	if len(data) < c.prefixLength+4 {
		return nil, errors.New("PrefixKeyCodec: data too short")
	}

	prefix := make([]byte, c.prefixLength)
	copy(prefix, data[:c.prefixLength])
	dictID := binary.BigEndian.Uint32(data[c.prefixLength:])

	return PrefixKey{Prefix: prefix, dictID: dictID}, nil
}

// Compare implements lexicographic ordering of PrefixKey keys.
// First compares prefixes lexicographically, then uses dictID as tie-breaker.
func (c PrefixKeyCodec) Compare(a, b []byte) int {
	if len(a) < c.prefixLength+4 || len(b) < c.prefixLength+4 {
		// Fallback to byte comparison if data is malformed
		return bytes.Compare(a, b)
	}

	// Compare prefixes
	prefixA := a[:c.prefixLength]
	prefixB := b[:c.prefixLength]
	if comp := bytes.Compare(prefixA, prefixB); comp != 0 {
		return comp
	}

	// Tie-breaker: compare dictID
	dictIDA := binary.BigEndian.Uint32(a[c.prefixLength:])
	dictIDB := binary.BigEndian.Uint32(b[c.prefixLength:])

	switch {
	case dictIDA < dictIDB:
		return -1
	case dictIDA > dictIDB:
		return 1
	default:
		return 0
	}
}

// Size returns the fixed serialized length of the key.
func (c PrefixKeyCodec) Size() int {
	return c.prefixLength + 4
}

// BuildPrefixKey constructs a PrefixKey from a string and dictID.
// The prefix is the first L bytes of the UTF-8 string, padded with 0x00 if shorter.
func BuildPrefixKey(s string, dictID uint32, prefixLength int) PrefixKey {
	prefix := make([]byte, prefixLength)
	copy(prefix, []byte(s)) // UTF-8 safe: truncates at byte boundary
	return PrefixKey{Prefix: prefix, dictID: dictID}
}

// PrefixUpperBound computes the smallest key strictly greater than all keys
// beginning with the given prefix. Used for prefix-range queries.
// Returns a PrefixKey with dictID set to math.MaxUint32.
func PrefixUpperBound(prefix string, prefixLength int) PrefixKey {
	buf := make([]byte, prefixLength)
	p := []byte(prefix)
	copy(buf, p) // Truncates to prefixLength if longer, pads with 0x00 if shorter

	// Determine effective length (min of actual prefix length and prefixLength)
	effectiveLen := len(p)
	if effectiveLen > prefixLength {
		effectiveLen = prefixLength
	}

	// Increment last non-0xFF byte within the effective prefix
	for i := effectiveLen - 1; i >= 0; i-- {
		if buf[i] != 0xFF {
			buf[i]++
			// Zero out all bytes after the incremented position
			for j := i + 1; j < prefixLength; j++ {
				buf[j] = 0x00
			}
			return PrefixKey{Prefix: buf, dictID: 0xFFFFFFFF}
		}
	}

	// Overflow: prefix is all 0xFF or empty
	// Return maximum possible key
	for i := range buf {
		buf[i] = 0xFF
	}
	return PrefixKey{Prefix: buf, dictID: 0xFFFFFFFF}
}

// GetPrefixKeyDictID returns the dictID from a PrefixKey.
func GetPrefixKeyDictID(key PrefixKey) uint32 {
	return key.dictID
}
