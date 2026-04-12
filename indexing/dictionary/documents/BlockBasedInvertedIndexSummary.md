# Scalable Persistent Inverted Index: Design Summary

## Record Storage and Identification
- String property values in records are replaced with references (uint32) to string IDs in string dictionary
- After replacing strings with dictionary references, records are stored as fixed-length binary blocks in a file, organized in blocks (e.g., 4KB).
- Each record is uniquely identified by its sequential position (record address) in the file.
- The byte offset for a record is: `record address × record length`.
- The file header maintains the address of the first deleted record; each deleted record stores the address of the next deleted record, forming a free list for space reuse.

## Dictionary Index (B+ Tree)
- A B+ tree index maps string hashes to dictionary entries.
- Each leaf node entry contains:
  - The string hash (index key)
  - The actual string (for collision resolution)
  - The dictionary ID (dictID)
  - A reference to the posting list for that dictID
- On lookup or insert:
  - Compute the string hash and search the B+ tree.
  - Scan all entries with the same hash in the leaf node, comparing actual strings to resolve collisions.
  - If found, use the dictID; if not, insert a new entry.

## Posting Lists Organization
- Each posting list (for a dictID) is a chain of blocks, each block containing:
  - A header (count, next block address)
  - A list of record addresses (addresses of records with the string)
- Adding a record: append its address to the last block; allocate a new block if full.
- Deleting a record: scan and remove its address from the posting list; optionally compact blocks.
- Updating a record: if the string changes, remove from the old posting list and add to the new one.
- Posting blocks are managed with a free list for space reuse.

## Query Flow
1. Lookup the string in the B+ tree (using hash, then string compare).
2. Retrieve the posting list reference from the entry.
3. Traverse the posting list blocks to collect all record addresses.
4. Use record addresses to compute offsets and fetch records from the records file.

## Advantages
- Scales to very large datasets (no need to load all data into memory).
- Efficient random access and space reuse via record addresses and free lists.
- Block-based I/O and caching optimize performance for both reads and writes.
- B+ tree indices enable fast lookups, prefix, and range queries.
- Posting lists support dynamic add, update, and delete operations.

---

This design enables a high-performance, persistent inverted index suitable for large-scale data, supporting efficient queries and updates with minimal memory usage.
