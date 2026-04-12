package primindex

import (
	"errors"
)

var (
	ErrEntryTooShort = errors.New("index entry data too short")
)

// IndexEntry represents a single key-value entry in a leaf node
type IndexEntry struct {
	Key   interface{} // actual key value (will be serialized using KeyCodec)
	Value []byte      // value data (fixed size defined in header)
}

// EncodeIndexEntry encodes an entry as [keySize bytes key][valueSize bytes value].
// The entry size is keySize + valueSize (both fixed for a given index).
func EncodeIndexEntry(entry *IndexEntry, codec KeyCodec) ([]byte, error) {
	// Serialize the key using the codec
	keyBytes, err := codec.Serialize(entry.Key)
	if err != nil {
		return nil, err
	}

	keySize := codec.Size()
	valueSize := len(entry.Value)
	totalSize := keySize + valueSize
	entryBytes := make([]byte, totalSize)

	// Copy key (keySize bytes)
	copy(entryBytes[0:keySize], keyBytes)

	// Copy value (valueSize bytes)
	copy(entryBytes[keySize:keySize+valueSize], entry.Value)

	return entryBytes, nil
}

// DecodeIndexEntry decodes a byte slice into IndexEntry.
// Expects exactly keySize + valueSize bytes.
func DecodeIndexEntry(data []byte, codec KeyCodec, valueSize int) (*IndexEntry, error) {
	keySize := codec.Size()
	totalSize := keySize + valueSize
	if len(data) < totalSize {
		return nil, ErrEntryTooShort
	}

	// Decode key (keySize bytes)
	key, err := codec.Deserialize(data[0:keySize])
	if err != nil {
		return nil, err
	}

	// Decode value (valueSize bytes)
	value := make([]byte, valueSize)
	copy(value, data[keySize:keySize+valueSize])

	return &IndexEntry{
		Key:   key,
		Value: value,
	}, nil
}
