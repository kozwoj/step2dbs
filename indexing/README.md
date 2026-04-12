# Dictionary and Prime Index Library

A Go library providing disk-backed data structures for building searchable indexes
- a generic B+ tree primary index
- a string dictionary with inverted index support, and block-based postings lists.

The main use cases supported by the library are: 
- indexing a set of records based on a uniquely valued property (the primary key)
- replacing string-typed record properties with references to dictionaries, so the records can be stored as fixed-size byte arrays
- searching for records with the same value of a string-type property, and
- searching for records with a string-type property starting with a prefix 

## Packages

### `primindex` — Generic B+ Tree Primary Index

A block-based B+ tree index supporting multiple fixed-width key types with configurable block sizes. All data is persisted to disk.

**Supported key types:** `uint8`, `uint16`, `uint32`, `uint64`, `int16`, `int32`, `int64`, fixed-size byte arrays (4 to 32 bytes), and `PrefixBytes8` for prefix search.

```go
// Create and use an index
// func CreateIndexFile(path string, filename string, blockSize uint16, blockCount uint16, keyType KeyType, valueSize uint32) error
primindex.CreateIndexFile(path, "myindex.dat", 4096, 100, primindex.KeyTypeUint32, 8)
idx, _ := primindex.OpenIndex(path, "myindex.dat")
defer idx.Close()

// func (idx *Index) Insert(key interface{}, value []byte) error
idx.Insert(uint32(42), []byte("somedata"))

// func (idx *Index) Delete(key interface{}) error
entry, _ := idx.Find(uint32(42))
idx.Delete(uint32(42))
```

### `dictionary/dictionary` — String Dictionary with Inverted Index

Stores unique string values and maps them to postings lists of record IDs. Combines a raw strings file, offsets file, a 128-bit hash B+ tree index, postings storage, and a prefix index — all persisted as separate files in one directory.

```go
blockSizes := dictionary.DictionaryBlockSizes{
    PostingsBlockSize: 4096,
    IndexBlockSize:    4096,
    PrefixBlockSize:   4096,
}

dictionary.CreateDictionary(dirPath, "cities", blockSizes, 100)
dict, _ := dictionary.OpenDictionary(dirPath, "cities")
defer dict.Close()

dictID, postingsRef, _ := dict.AddString("Seattle")
dict.AddRecordID(postingsRef, recordID, dictID)

// String search returns posting ref with records IDs with that string
_, ref, _ := dict.FindString("Seattle")
recordIDs := dict.RetrievePostings(ref)

// Prefix search returns matching string IDs
matches := dict.PrefixSearch("Sea")
```

Two postings formats are available: slice-based (default) and bitmap-based (Roaring Bitmap, better for large/dense sets). Use `CreateDictionaryWithFormat` to select bitmap format.

### `dictionary/dicindex128` — 128-bit Hash B+ Tree Index

A specialized B+ tree where keys are 128-bit xxHash values. Used internally by the dictionary to map string hashes to `(dictID, postingsRef)` pairs. Fixed 24-byte entries (16-byte hash + 4-byte dictID + 4-byte postingsRef).

```go
dicindex128.CreateDictionaryIndexFile(dirPath, "index.dat", 4096, 100)
idx, _ := dicindex128.OpenDictionaryIndex(dirPath, "index.dat")
defer idx.Close()

hash := dicindex128.HashString128("hello")
idx.Insert(dicindex128.IndexEntry128{Hash: hash, DictID: 1, PostingsRef: 0})
entry, _ := idx.Find(hash)
```

### `dictionary/postings` — Block-Based Postings Lists

Manages lists of record IDs associated with dictionary entries. Postings are stored in fixed-size chained blocks with a free-block list for reuse.

**Formats:**
- **Slice** (`FormatSlice`) — array of uint32 record IDs, straightforward
- **Bitmap** (`FormatBitmap`) — Roaring Bitmap compressed storage, efficient for large or dense sets

## File Layout

Each dictionary creates 5 files in its directory:

| File | Purpose |
|------|---------|
| `strings.dat` | Raw string values, sequentially appended |
| `offsets.dat` | Maps string ID → offset/length in strings file |
| `index.dat` | 128-bit hash B+ tree for string lookup |
| `postings.dat` | Chained blocks of record ID lists |
| `prefix.dat` | B+ tree for prefix search (PrefixBytes8 keys) |

A `primindex` index is a single `.dat` file.

## Install

```
go get github.com/kozwoj/indexing
```

## Dependencies

- [roaring](https://github.com/RoaringBitmap/roaring) — compressed bitmap postings lists
- [xxh3](https://github.com/zeebo/xxh3) — xxHash128 for dictionary index keys

## Design Documents

Detailed design notes are available in the repository:

- [primindex/documents/IndexDesign.md](primindex/documents/IndexDesign.md) — Primary index B+ tree design
- [dictionary/docs/BlockBasedInvertedIndexSummary.md](dictionary/docs/BlockBasedInvertedIndexSummary.md) — Inverted index architecture
- [dictionary/docs/DictionaryDesign.md](dictionary/docs/DictionaryDesign.md) — Dictionary storage design
- [dictionary/docs/DictionaryIndexDesign.md](dictionary/docs/DictionaryIndexDesign.md) — Hash128 index design
- [dictionary/docs/PrefixSearch.md](dictionary/docs/PrefixSearch.md) — Prefix search implementation
