package primindex

import (
	"bytes"
	"testing"
)

func TestPrefixKeyCodec_SerializeDeserialize(t *testing.T) {
	codec := PrefixKeyCodec{prefixLength: 8}

	tests := []struct {
		name    string
		key     PrefixKey
		wantErr bool
	}{
		{
			name: "8-byte prefix with dictID",
			key:  PrefixKey{Prefix: []byte("customer"), dictID: 12345},
		},
		{
			name: "shorter prefix padded with zeros",
			key:  PrefixKey{Prefix: []byte("abc\x00\x00\x00\x00\x00"), dictID: 99},
		},
		{
			name: "all zeros prefix",
			key:  PrefixKey{Prefix: []byte("\x00\x00\x00\x00\x00\x00\x00\x00"), dictID: 1},
		},
		{
			name: "all 0xFF prefix",
			key:  PrefixKey{Prefix: []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"), dictID: 0xFFFFFFFF},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Serialize
			serialized, err := codec.Serialize(tt.key)
			if (err != nil) != tt.wantErr {
				t.Fatalf("Serialize() error = %v, wantErr %v", err, tt.wantErr)
			}
			if tt.wantErr {
				return
			}

			// Check serialized size
			if len(serialized) != 12 {
				t.Errorf("Serialize() length = %d, want 12", len(serialized))
			}

			// Deserialize
			deserialized, err := codec.Deserialize(serialized)
			if err != nil {
				t.Fatalf("Deserialize() error = %v", err)
			}

			// Compare
			pk, ok := deserialized.(PrefixKey)
			if !ok {
				t.Fatalf("Deserialize() returned wrong type")
			}

			if !bytes.Equal(pk.Prefix, tt.key.Prefix) {
				t.Errorf("Prefix mismatch: got %v, want %v", pk.Prefix, tt.key.Prefix)
			}

			if pk.dictID != tt.key.dictID {
				t.Errorf("dictID mismatch: got %d, want %d", pk.dictID, tt.key.dictID)
			}
		})
	}
}

func TestPrefixKeyCodec_SerializeInvalidKey(t *testing.T) {
	codec := PrefixKeyCodec{prefixLength: 8}

	tests := []struct {
		name string
		key  interface{}
	}{
		{
			name: "wrong type (uint32)",
			key:  uint32(123),
		},
		{
			name: "wrong type (string)",
			key:  "test",
		},
		{
			name: "wrong prefix length",
			key:  PrefixKey{Prefix: []byte("short"), dictID: 123},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := codec.Serialize(tt.key)
			if err == nil {
				t.Error("Serialize() expected error but got nil")
			}
		})
	}
}

func TestPrefixKeyCodec_Compare(t *testing.T) {
	codec := PrefixKeyCodec{prefixLength: 8}

	tests := []struct {
		name     string
		keyA     PrefixKey
		keyB     PrefixKey
		expected int // -1, 0, or 1
	}{
		{
			name:     "equal keys",
			keyA:     PrefixKey{Prefix: []byte("customer"), dictID: 123},
			keyB:     PrefixKey{Prefix: []byte("customer"), dictID: 123},
			expected: 0,
		},
		{
			name:     "different prefix, A < B",
			keyA:     PrefixKey{Prefix: []byte("apple\x00\x00\x00"), dictID: 1},
			keyB:     PrefixKey{Prefix: []byte("banana\x00\x00"), dictID: 1},
			expected: -1,
		},
		{
			name:     "different prefix, A > B",
			keyA:     PrefixKey{Prefix: []byte("zebra\x00\x00\x00"), dictID: 1},
			keyB:     PrefixKey{Prefix: []byte("apple\x00\x00\x00"), dictID: 1},
			expected: 1,
		},
		{
			name:     "same prefix, dictID A < B",
			keyA:     PrefixKey{Prefix: []byte("customer"), dictID: 100},
			keyB:     PrefixKey{Prefix: []byte("customer"), dictID: 200},
			expected: -1,
		},
		{
			name:     "same prefix, dictID A > B",
			keyA:     PrefixKey{Prefix: []byte("customer"), dictID: 500},
			keyB:     PrefixKey{Prefix: []byte("customer"), dictID: 300},
			expected: 1,
		},
		{
			name:     "prefix with zeros vs non-zeros",
			keyA:     PrefixKey{Prefix: []byte("abc\x00\x00\x00\x00\x00"), dictID: 1},
			keyB:     PrefixKey{Prefix: []byte("abcdef\x00\x00"), dictID: 1},
			expected: -1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			bytesA, _ := codec.Serialize(tt.keyA)
			bytesB, _ := codec.Serialize(tt.keyB)

			result := codec.Compare(bytesA, bytesB)

			// Normalize result to -1, 0, or 1
			var normalized int
			if result < 0 {
				normalized = -1
			} else if result > 0 {
				normalized = 1
			} else {
				normalized = 0
			}

			if normalized != tt.expected {
				t.Errorf("Compare() = %d, want %d", normalized, tt.expected)
			}
		})
	}
}

func TestPrefixKeyCodec_Size(t *testing.T) {
	codec := PrefixKeyCodec{prefixLength: 8}
	if size := codec.Size(); size != 12 {
		t.Errorf("Size() = %d, want 12", size)
	}
}

func TestBuildPrefixKey(t *testing.T) {
	tests := []struct {
		name         string
		str          string
		dictID       uint32
		prefixLength int
		wantPrefix   []byte
		wantDictID   uint32
	}{
		{
			name:         "exact length string",
			str:          "customer",
			dictID:       123,
			prefixLength: 8,
			wantPrefix:   []byte("customer"),
			wantDictID:   123,
		},
		{
			name:         "shorter string padded with zeros",
			str:          "abc",
			dictID:       456,
			prefixLength: 8,
			wantPrefix:   []byte("abc\x00\x00\x00\x00\x00"),
			wantDictID:   456,
		},
		{
			name:         "longer string truncated",
			str:          "verylongcustomername",
			dictID:       789,
			prefixLength: 8,
			wantPrefix:   []byte("verylong"),
			wantDictID:   789,
		},
		{
			name:         "empty string",
			str:          "",
			dictID:       1,
			prefixLength: 8,
			wantPrefix:   []byte("\x00\x00\x00\x00\x00\x00\x00\x00"),
			wantDictID:   1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := BuildPrefixKey(tt.str, tt.dictID, tt.prefixLength)

			if !bytes.Equal(key.Prefix, tt.wantPrefix) {
				t.Errorf("BuildPrefixKey() prefix = %v, want %v", key.Prefix, tt.wantPrefix)
			}

			if key.dictID != tt.wantDictID {
				t.Errorf("BuildPrefixKey() dictID = %d, want %d", key.dictID, tt.wantDictID)
			}
		})
	}
}

func TestPrefixUpperBound(t *testing.T) {
	tests := []struct {
		name         string
		prefix       string
		prefixLength int
		wantPrefix   []byte
		wantDictID   uint32
	}{
		{
			name:         "normal prefix",
			prefix:       "cust",
			prefixLength: 8,
			wantPrefix:   []byte("cusu\x00\x00\x00\x00"),
			wantDictID:   0xFFFFFFFF,
		},
		{
			name:         "prefix ending with z",
			prefix:       "xyz",
			prefixLength: 8,
			wantPrefix:   []byte("xy{\x00\x00\x00\x00\x00"),
			wantDictID:   0xFFFFFFFF,
		},
		{
			name:         "single character",
			prefix:       "a",
			prefixLength: 8,
			wantPrefix:   []byte("b\x00\x00\x00\x00\x00\x00\x00"),
			wantDictID:   0xFFFFFFFF,
		},
		{
			name:         "prefix with 0xFF bytes",
			prefix:       "abc\xFF",
			prefixLength: 8,
			wantPrefix:   []byte("abd\x00\x00\x00\x00\x00"),
			wantDictID:   0xFFFFFFFF,
		},
		{
			name:         "all 0xFF overflow",
			prefix:       "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF",
			prefixLength: 8,
			wantPrefix:   []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
			wantDictID:   0xFFFFFFFF,
		},
		{
			name:         "empty prefix",
			prefix:       "",
			prefixLength: 8,
			wantPrefix:   []byte("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF"),
			wantDictID:   0xFFFFFFFF,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := PrefixUpperBound(tt.prefix, tt.prefixLength)

			if !bytes.Equal(key.Prefix, tt.wantPrefix) {
				t.Errorf("PrefixUpperBound() prefix = %v, want %v", key.Prefix, tt.wantPrefix)
			}

			if key.dictID != tt.wantDictID {
				t.Errorf("PrefixUpperBound() dictID = %d, want %d", key.dictID, tt.wantDictID)
			}
		})
	}
}

func TestKeyCodecFactory_PrefixBytes8(t *testing.T) {
	codec := KeyCodecFactory(KeyTypePrefixBytes8)

	// Verify it returns PrefixKeyCodec with correct length
	prefixCodec, ok := codec.(PrefixKeyCodec)
	if !ok {
		t.Fatalf("KeyCodecFactory(KeyTypePrefixBytes8) did not return PrefixKeyCodec")
	}

	if prefixCodec.prefixLength != 8 {
		t.Errorf("PrefixKeyCodec.prefixLength = %d, want 8", prefixCodec.prefixLength)
	}

	if prefixCodec.Size() != 12 {
		t.Errorf("PrefixKeyCodec.Size() = %d, want 12", prefixCodec.Size())
	}
}
