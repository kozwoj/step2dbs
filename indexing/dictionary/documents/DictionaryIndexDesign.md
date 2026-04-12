# Design of a dictionary index integrated with a string dictionary and postings lists

## Context 

The context is serializing records, with string type properties, into fixed length memory blocks by replacing the values of those properties (strings) with references to string dictionary.  

## Terms and assumptions

- a table is a collection of records of the same type. each record is internally identified by it sequential position in the table (recordID or rowNr), which does not change. 
- dictionary is a collection of unique strings, each identified by its sequential position in the dictionary (dicID)
- before serializing and storing each record, string property value is replaced by the dicID of that string in the corresponding dictionary. this makes the stored records of fixed size as each variable length string is replaced by an integer. the value of each string property is recovered from the dictionary on output. 
- there is a separate dictionary for every string property defined in a records e.g. Name, State, Street, etc.
- the dictionary index maps a string value into the dicID of that string in the corresponding dictionary - one index is associated with one dictionary
- in addition the index is design in such a way, that the entry for a string stored in a leaf node also contains (1) the string itself (optimization and to identify hash synonyms) and (2) reference to a list (postings list) of recordIDs with that property value (e.g. all records with State == "Colorado")

## Use cases

The main use case is optimizing replacing a sting property value in a record with dicID of the value of that property in the corresponding dictionary. In this cases the index is given a string and either returns dicID or null, which means that the value has not been stored in the dictionary yet. 

The other use case is finding all records with a specific value of a property. In this case the index is given a string and returns the postings list for that value, which is a sequence of recordIDs.

## Design 

The search key in the dictionary index is not strings, but hash value of those strings. Given a string the index first computes hash of that string and uses that hash to find the corresponding index entry. We will use xxHash-128 for computing the hashes. 

The dictionary index will follow the design of the record primary key index, which is given in the inverted/index. In particular 
- dictionary index is stored in one file organized as a sequence of blocks of the size declared in the index header
- each block stores one kind of nodes in the index 
- there are two kinds of nodes (1) internal and (2) leaf
- internal nodes form a B+ tree with value-sorted references to other internal or leaf nodes
- leaf nodes store the entries with **[16 bytes hash][4 bytes dictID][4 bytes postingsRef][var bytes string]**
- the index file has a header describing index properties 

The structure of the leaf node is below, where NextOffset is the byte after the end of the last entry

``` 
LeafNode
	BlockNumber     uint16
    Used            uint8
	NodeType        uint8
	EntryCount      uint16
    NextOffset      uint16
	NextLeaf        uint16
	EntryOffsets    []uint16

Entry
    Hash            [4]byte
    DicID           uint32
    PostingRef      uint32
    StrValue        []byte
```

The structure of the internal node is below, where Kays are the string hashes 

```
InternalNode
	BlockNumber uint16
    Used        uint8
	NodeType    uint8
	KeyCount    uint16
	Keys        [][4]byte
	Pointers    []uint16
```

The index header, stored at the begging of the file, has the following structure 

```
IndexHeader
	BlockSize       uint16
	FileLength      uint16 // in blocks
	RootNode        uint16
	FirstLeaf       uint16
	NextEmptyBlock  uint16 
```

Since dictionaries don't shrink there is no strong need to keep a list of blocks freed by merging block after multiple deletes. Hance Next EmptyBlocks is the sequential number of next empty block in the file. More likely scenario is that a dictionary is compacted and re-indexed from time to time. 

The logic of inserting keys and entries and searching for them should follow the logic implemented in primary key index in the index directory. 

The logic should be also organized in corresponding files: file, leafnode, internalnode, etc.

 