# Summary of the design of dictionary with an index and postings lists

Such a dictionary is be used to store string property values of a collection of records (hance postings list). In most cases one dictionary will be associated with one property of those records.
One dictionary can be also associated with multiple properties of the records, but in this cases postings become ambiguous and should not be stored - the dictionary can be used to restore property values, but not for search.

In the below design we refer to three identifiers
- recordID - identifier of a record in the records file == sequential position of the record in the file
- dictID - identifier of raw string == sequential number of the string in the raw string file
- postingsRef - identifier of a list of records with the given string value **== block number** of the first block of the list in the postings file

## Dictionary Storage
All files of a single dictionary are stored in a single directory named after the dictionary. Inside the directory, the files are:
- file with raw strings (`strings.dat` file)
- file with string offsets (`offsets.dat` file)
- file with the postings lists (`postings.dat` file)
- the index file (`index.dat` file), and
- the prefix file (`prefix.dat` file)

## Raw strings storage
Strings are stored in a blob file where every new string is appended to the end of the file. Each string is assigned the sequential number starting at 0.

The header of the strings file is as follows
``` Go
type StringsFileHeader struct {
  EndOffset    uint64  // the offset of the end of the file where the next strings should be written
  NumOfStrings uint64  // number of strings already stored in the file
}
```
A next string is appended to the file and its offset == EndOffset is stored in the offsets file. The EndOffset is then set to EndOffset + length of the new string.

## Offsets storage
String offsets are stored sequentially, in the same sequence as the corresponding strings. The file does not need a header, as the entries are of the same size (12 bytes), and the number of entries is the same as the number of strings. Each entry in the file has the following structure
``` Go
type OffsetEntry struct {
  Offset      uint64  // offset of the beginning of the corresponding string in the strings file
  PostingsRef uint32  // the block number of the fist block of the postings corresponding to the string
}
```

## Postings storage
Postings lists are organized either as
- lists of record IDs with the given string value, or
- bitmaps, where each bit corresponds to record, managed by the `github.com/RoaringBitmap/roaring` package

Posting lists are stored in a fixed-size blocks chained together. The postingsRef, which is associated with the string via the dictionary index, is the number of
the first block of the postings list in the postings file.

Each postings block has the following header

``` Go
type PostingBlockHeader struct {
	BlockNumber uint32 // the current block number for integrity maintenance
	DictID      uint32 // corresponding dictionary ID for integrity maintenance
	NextBlock   uint32 // block number of the next block of the list
	Count       uint32 // number of records IDs stored in the block
}
```

The block header is followed either by a slice of uint32 (4 bytes) of record IDs, or by a bit array, depending on the list format. The list format (slice of record IDs vs. bitmap) is stored in the posting file header of the following structure

``` Go
type PostingsFileHeader struct {
	BlockSize        uint32         // size of each postings block in bytes
	FirstFreeBlock   uint32         // number of the first block in the free block list
	NumberOfPostings uint32         // total number of postings in the file
	Format           PostingsFormat // format used for storing record IDs (slice or bitmap)
}
```

Initially the first free block is the block after the last used block. However postings lists fluctuate in size, so a block at the end of a list may become empty. If that happens the block is added to the beginning of the free block list pointed to by FirstFreeBlock property in the postings file header - the file header points to a linked list of empty/reusable blocks.

## Prefix storage

See the discussion in `dictionary\docs\PrefixSearch.md` design note.
