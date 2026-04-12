package dictionary

import (
	"errors"
	"github.com/kozwoj/indexing/primindex"
)

/* Prefix index enables fast prefix-based searches on dictionary strings. It maintains a primary index (B+ tree)
that orders strings by their first L bytes (prefix), clustering all strings with the same prefix together for
efficient range queries.

The prefix index is stored in a separate prefix index file, called prefix.dat and managed by primindex package, and uses KeyTypePrefixBytes8
with 8-byte prefix + 4-byte dictID keys. The value is 2 bytes (unused, can be zeros since dictID is in the key).

All strings starting with the same prefix are clustered together in the index, but they are not in lexical order
beyond the prefix. This is acceptable because prefix search only cares about finding all matching strings, not
their order.

The operations of the prefix index are:
- CreatePrefixIndex creates a new prefix index file when a new dictionary is created
- OpenPrefixIndex opens an existing prefix index file when a dictionary is opened
- AddPrefixEntry adds a prefix entry for a newly added dictionary string
- PrefixSearch searches for all strings starting with a given prefix and returns their string IDs

note:
The prefix index should be updated every time a new string is added to the dictionary. Since strings are never
deleted, prefix entries are also never deleted.
*/

// Custom error variables for prefix index file operations
var (
	ErrFileCreate     = errors.New("failed to create prefix index file")
	ErrFileOpen       = errors.New("failed to open prefix index file")
	ErrIndexCorrupted = errors.New("prefix index file is corrupted")
	ErrPrefixIndexNil = errors.New("prefix index is nil")
)

const prefixLength = 8        // 8-byte prefix as per design
const fileName = "prefix.dat"
const valueSize = uint32(2) // 2-byte value (unused, can be zeros)

/*
	CreatePrefixIndex creates a new prefix index file for the dictionary. It is called during dictionary creation

Input:
- dirPath: directory path where the prefix index file will be created
- blockSize: uint16 size of each block in bytes
- initialBlocks: uint16 initial number of blocks to allocate
Output:
- error: if any error occurs during creation

note: the CreateIndexFile function closes the newly created index file. The file is opened by OpenPrefixIndex function.
*/

func CreatePrefixIndex(dirPath string, blockSize uint16, initialBlocks uint16) error {

	// Use KeyTypePrefixBytes8 from primindex package

	err := primindex.CreateIndexFile(dirPath, fileName, blockSize, initialBlocks, primindex.KeyTypePrefixBytes8, valueSize)
	if err != nil {
		return ErrFileCreate
	}

	return nil
}

/*
	OpenPrefixIndex opens an existing prefix index file and returns the primindex.Index instance. Called during opening an

existing directory.

Input:
- dirPath: directory path where the prefix index file is located
Output:
- *primindex.Index: opened prefix index
- error: if any error occurs during opening
*/
func OpenPrefixIndex(dirPath string) (*primindex.Index, error) {

	idx, err := primindex.OpenIndex(dirPath, "prefix.dat")
	if err != nil {
		return nil, ErrFileOpen
	}

	return idx, nil
}

/*
AddPrefixEntry adds a prefix entry for a newly added dictionary string.
Called after adding a string to the dictionary, so we don't have to worry about key duplication.

Input:
- idx: opened prefix index
- s: the string to add
- dictID: dictionary ID for the string
Output:
- error: if any error occurs during insertion

note: The value is 2 bytes of zeros (unused, dictID is in the key itself).
*/
func AddPrefixEntry(idx *primindex.Index, s string, dictID uint32) error {
	if idx == nil {
		return ErrPrefixIndexNil
	}

	// Build the prefix key using primindex helper function
	key := primindex.BuildPrefixKey(s, dictID, prefixLength)

	// Value is unused (2 bytes, zeros)
	value := make([]byte, valueSize)

	// Insert into the index
	err := idx.Insert(key, value)
	if err != nil {
		return err
	}

	return nil
}

/*
PrefixSearch searches for all dictIDs whose keys fall within the prefix range.
Returns slice of dictIDs without verifying actual strings (caller's responsibility).

Note: Due to 8-byte prefix clustering, results may include strings that don't
fully match the prefix beyond 8 bytes. Caller should verify actual strings.

Input:
- idx: opened prefix index
- prefix: the prefix string to search for
Output:
- []uint32: slice of dictIDs for keys in prefix range
- error: if any error occurs during search
*/
func PrefixSearch(idx *primindex.Index, prefix string) ([]uint32, error) {
	if idx == nil {
		return nil, ErrPrefixIndexNil
	}

	// Step 1: Compute range bounds
	lowerKey := primindex.BuildPrefixKey(prefix, 0, prefixLength)
	upperKey := primindex.PrefixUpperBound(prefix, prefixLength)

	// Serialize bounds for comparison
	lowerKeyBytes, err := idx.Codec.Serialize(lowerKey)
	if err != nil {
		return nil, err
	}
	upperKeyBytes, err := idx.Codec.Serialize(upperKey)
	if err != nil {
		return nil, err
	}

	// Step 2: Find the leaf block containing the lower bound
	leafBlockNumber, _, err := idx.FindLeafBlock(lowerKey)
	if err != nil {
		return nil, err
	}

	// Step 3: Scan forward through leaf chain, collecting dictIDs
	var results []uint32
	currentLeafNum := leafBlockNumber

	for currentLeafNum != primindex.NoNextLeaf {
		// Read current leaf block
		leafBlock, err := primindex.ReadIndexBlock(idx, int(currentLeafNum))
		if err != nil {
			return nil, err
		}

		// Deserialize leaf to access entries and NextLeaf pointer
		leafNode, err := primindex.DeserializeLeafNode(leafBlock, idx.Codec, int(idx.Header.ValueSize))
		if err != nil {
			return nil, err
		}

		// Scan all entries in this leaf
		for i := 0; i < int(leafNode.EntryCount); i++ {
			entry := leafNode.Entries[i]

			// Serialize entry key for comparison
			entryKeyBytes, err := idx.Codec.Serialize(entry.Key)
			if err != nil {
				return nil, err
			}

			// Check if we've passed the upper bound
			if idx.Codec.Compare(entryKeyBytes, upperKeyBytes) >= 0 {
				return results, nil
			}

			// Check if entry is >= lower bound
			if idx.Codec.Compare(entryKeyBytes, lowerKeyBytes) >= 0 {
				prefixKey := entry.Key.(primindex.PrefixKey)
				results = append(results, primindex.GetPrefixKeyDictID(prefixKey))
			}
		}

		// Move to next leaf in chain
		currentLeafNum = leafNode.NextLeaf
	}

	return results, nil
}
