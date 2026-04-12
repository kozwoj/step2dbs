# Dictionary and Index Block Size Optimization

**Date:** March 8, 2026
**Status:** Proposed
**Context:** Performance testing revealed inconsistent block sizes and poor space utilization

## Problem Statement

Performance testing with high-cardinality customer data (10K records) revealed storage inefficiency and inconsistent block size configuration across the database:

### Current Implementation - Inconsistent Block Sizes

**Dictionary files** use a **single block size** (512 bytes) for all file types:
- `db/db_create.go:141`: `dictionary.CreateDictionary(dictDirPath, field.Name, 512, 100)`
- Applied to all files: postings.dat, index.dat, prefix.dat

**Primary indexes** already use **larger blocks** (1024 bytes):
- `db/db_create.go:118`: `primindex.CreateIndexFile(tableDirPath, "primindex.dat", 1024, 10, keyType, 4)`

**Problem:** Dictionaries use 512-byte blocks for everything, but primary indexes already recognize that B-tree indexes benefit from larger blocks (1024 bytes). This inconsistency suggests the optimization should be applied uniformly.

### Block Size Summary

| File Type               | Current Size | Proposed Size | Rationale                          |
|-------------------------|--------------|---------------|------------------------------------|
| postings.dat (dict)     | 512 bytes    | 128 bytes     | Reduce waste on sparse lists       |
| index.dat (dicindex128) | 512 bytes    | 1024 bytes    | Match primindex, shallower trees   |
| prefix.dat (primindex)  | 512 bytes    | 1024 bytes    | Match primindex, B-tree efficiency |
| primindex.dat (primary) | 1024 bytes   | 1024 bytes ✓  | Already correct - no change        |

**Key Observation:** Primary indexes already use the optimal size for B-trees. We need to make dictionaries consistent.

### Observed Issues

**High Cardinality Fields** (10,000 unique strings):
- postings.dat size: **5.2 MB per field**
- Cause: Each unique string gets its own postings list in a dedicated 512-byte block
- With only 1 record ID per string: **4 bytes used / 512 bytes allocated = <1% utilization**
- Example fields: Company_name, Contact_name, Address, Phone, Postal_code

**Low Cardinality Fields** (20-91 unique strings):
- postings.dat size: **164 KB per field**
- Each postings list contains 100-500 record IDs
- Block utilization: **50-80%** ✓
- Example fields: Country, Region, City, Contact_title

## Performance Test Results

From `performance/TestCustomerInsert10K` with 512-byte blocks:

| Field         | Unique Strings | Dictionary Size | Block Utilization |
|---------------|----------------|-----------------|-------------------|
| Company_name  | 10,000         | 5.21 MB         | <1% (wasteful)    |
| Contact_name  | 10,000         | 5.19 MB         | <1% (wasteful)    |
| Address       | 10,000         | 5.19 MB         | <1% (wasteful)    |
| Phone         | 10,000         | 5.11 MB         | <1% (wasteful)    |
| Country       | 20             | 164 KB          | ~60% (good)       |
| City          | 91             | 164 KB          | ~50% (good)       |
| Contact_title | 30             | 164 KB          | ~60% (good)       |

**Storage Waste:** ~20 MB wasted in postings files for just 10K records across 10 fields.

## Analysis by File Type

All B-tree based structures (indexes) benefit from larger blocks, while data structures with sparse lists benefit from smaller blocks:

### 1. postings.dat - Record ID Lists (Dictionary)
**Current:** 512-byte blocks
**Access Pattern:** Sequential read of entire list, random write for updates
**Optimization:** **SMALLER blocks (128-256 bytes)**

**Rationale:**
- High-cardinality fields dominate storage cost
- Most postings lists are short (1-10 record IDs)
- Smaller blocks reduce waste for sparse lists
- Low-cardinality fields with long lists still work fine (just use multiple blocks)

**Calculation with 128-byte blocks:**
```
Block structure:
- Header: 16 bytes (BlockNumber, DictID, NextBlock, Count)
- Available: 112 bytes = 28 uint32 record IDs
- Utilization:
  * 1 record ID: 4/128 = 3% (vs <1% with 512-byte)
  * 10 record IDs: 40/112 = 36%
  * 28 record IDs: 100%
```

**Expected Savings:**
- High cardinality fields: 5.2 MB → ~1.3 MB (75% reduction)
- Total for 10K customer records: ~20 MB → ~5 MB

### 2. index.dat - Dictionary Index (dicindex128)
**Current:** 512-byte blocks
**Access Pattern:** Random access B-tree traversal
**Optimization:** **LARGER blocks (1024-2048 bytes)**

**Rationale:**
- More entries per node → shallower tree → fewer I/Os
- Better cache locality for range scans
- Reduced tree depth improves lookup performance
- Industry standard: databases use 4KB-16KB pages for indexes
- **Consistent with primindex which already uses 1024 bytes**

**Benefits:**
- Faster lookups (fewer node loads)
- Better cache utilization
- More efficient bulk operations

### 3. prefix.dat - Prefix Index (primindex for Dictionary)
**Current:** Uses primindex with 512-byte blocks
**Access Pattern:** B-tree for prefix searches
**Optimization:** **LARGER blocks (1024-2048 bytes)**

**Rationale:**
- Same as index.dat and primindex.dat (B-tree structure)
- Prefix searches often scan ranges
- Larger nodes = fewer seeks
- **Should match primindex.dat block size (1024 bytes)**

### 4. primindex.dat - Primary Key Index
**Current:** ✅ Already uses 1024-byte blocks
**Access Pattern:** Random access B-tree traversal
**Status:** **No change needed - already optimized**

**Note:** This is already correctly configured. The problem is that dictionary indexes use smaller 512-byte blocks when they should match this size.

### 5. strings.dat & offsets.dat
**Current:** Sequential byte streams (no blocks)
**Optimization:** N/A (not block-based)

## Proposed Solution

### Option A: Per-File-Type Block Sizes (Recommended)

Configure different block sizes for different file types, **consistent with existing primindex configuration**:

```go
// In db/db_create.go
const (
    // Data structures with sparse lists
    DictionaryPostingsBlockSize = 128   // Small: optimize for sparse postings

    // B-tree indexes (consistent across all index types)
    PrimaryIndexBlockSize       = 1024  // Already in use for primindex.dat
    DictionaryIndexBlockSize    = 1024  // Match primindex size
    DictionaryPrefixBlockSize   = 1024  // Match primindex size
)
```

**Key Principle:** All B-tree indexes should use the same block size (1024 bytes) for consistency and predictability.

**Changes Required:**

1. **dictionary/dictionary/dictionary.go**
   - Modify `CreateDictionary()` signature to accept multiple block sizes:
     ```go
     func CreateDictionary(dirPath, name string,
                          postingsBlockSize, indexBlockSize, prefixBlockSize uint32,
                          initialBlocks uint32) (*Dictionary, error)
     ```
   - Pass appropriate sizes to each file creation

2. **dicindex128/index.go**
   - Modify `CreateDictionaryIndexFile()` to accept block size parameter
   - Currently uses hardcoded 512 bytes
   - Change to accept parameter, default to 1024

3. **db/db_create.go**
   - Add block size constants (as shown above)
   - Update `CreateFilesAndDictionaries()` to pass different sizes:
     ```go
     // For dictionaries
     dict, err := dictionary.CreateDictionary(
         dictDirPath, field.Name,
         DictionaryPostingsBlockSize,  // 128
         DictionaryIndexBlockSize,     // 1024
         DictionaryPrefixBlockSize,    // 1024
         DefaultDictionaryInitialSize)

     // Note: primindex.CreateIndexFile already uses PrimaryIndexBlockSize (1024)
     ```

### Option B: Per-Field Block Size Configuration

Allow schema-level configuration of block sizes per field based on expected cardinality.

**Example DDL Extension:**
```sql
TABLE Customers (
    Customer_id CHAR[10] PRIMARY KEY,
    Company_name STRING(40) BLOCKSIZE(128),  -- High cardinality
    Country STRING(15) BLOCKSIZE(512)        -- Low cardinality
)
```

**Pros:** Maximum flexibility
**Cons:** More complex, requires schema changes, harder to tune

### Option C: Adaptive Block Sizing

Dynamically adjust block size based on observed usage patterns.

**Pros:** Automatic optimization
**Cons:** Complex implementation, runtime overhead, database migration complexity

## Recommendation

**Implement Option A: Per-File-Type Block Sizes**

**Reasoning:**
1. **Consistency:** Align dictionary indexes with existing primindex configuration (1024 bytes)
2. **Proven approach:** Primary indexes already demonstrate that 1024-byte blocks work well for B-trees
3. **Simple implementation:** 4 constants, minimal code changes
4. **Immediate impact:** 75% storage reduction for high-cardinality fields
5. **No schema changes:** Works with existing databases (for new dictionaries)
6. **Clear rationale:** Different data structures have different optimal block sizes

**Key Insight:** The database already "knows" that B-tree indexes should use 1024-byte blocks (primindex), but dictionaries inconsistently use 512-byte blocks for their indexes. This optimization unifies the approach.

**Summary of Changes:**
- **Postings (data):** 512 → 128 bytes (smaller for sparse lists)
- **Dictionary indexes:** 512 → 1024 bytes (match primindex)
- **Prefix indexes:** 512 → 1024 bytes (match primindex)
- **Primary indexes:** 1024 bytes (no change - already correct)

**Trade-offs:**
- Low-cardinality fields with very long lists will use more blocks (negligible overhead)
- Existing databases won't automatically benefit (would need rebuild)

## Implementation Plan

### Phase 1: Core Changes
1. ✅ Identify current block size usage (completed)
2. ⬜ Add block size constants to `db/db_create.go`
3. ⬜ Modify `dictionary.CreateDictionary()` signature
4. ⬜ Update `dicindex128.CreateDictionaryIndexFile()` signature
5. ⬜ Update all callers to pass appropriate block sizes

### Phase 2: Testing
1. ⬜ Update existing dictionary tests with new signatures
2. ⬜ Run performance tests and compare file sizes
3. ⬜ Verify correctness with existing test suites
4. ⬜ Add specific tests for block size variations

### Phase 3: Validation
1. ⬜ Re-run `TestCustomerInsert10K` and compare results
2. ⬜ Document storage savings
3. ⬜ Measure any performance impact (positive or negative)
4. ⬜ Update README.md with new findings

## Expected Outcomes

### Storage Savings (10K Customer Records)
- **Before:** ~31.7 MB total, ~20 MB in postings files
- **After:** ~15-17 MB total, ~5 MB in postings files
- **Savings:** ~50% reduction in total database size

### Performance Impact
- **Postings:** Minimal impact (still sequential access)
- **Indexes:** Improved lookup performance (fewer node loads)
- **Insert throughput:** Potentially slightly faster (less I/O)

## Open Questions

1. Should we make initial block allocation counts configurable per file type?
   - Currently: 100 blocks for all files
   - Postings might need more, indexes might need fewer

2. Should we provide a migration tool for existing databases?
   - Recreate dictionaries with new block sizes
   - Copy data to new structure

3. Should block sizes be stored in database metadata?
   - For potential future dynamic resizing
   - For debugging and monitoring

## References

- Performance test results: `performance/TestCustomerInsert10K`
- Current implementation: `db/db_create.go:136`
- Dictionary creation: `dictionary/dictionary/dictionary.go:73`
- Postings file structure: `dictionary/postings/structs.go`
- Block size impact analysis: This document, sections above

## Future Considerations

### Potential Enhancements
1. **Evaluate larger index blocks:** Consider 2048 or 4096 bytes for all B-tree indexes
   - Would reduce tree depth further
   - Industry databases often use 4KB-16KB pages
   - Requires performance testing to validate benefits

2. **Per-field block size hints** in DDL (Option B)
3. **Automatic block size selection** based on field type and constraints
4. **Runtime monitoring** of block utilization
5. **Dynamic resizing** of dictionary files based on usage patterns
6. **Compression** for postings lists (additional space savings)

### Unified Index Strategy
Once dictionary indexes are aligned with primindex at 1024 bytes, consider a future enhancement to make all index block sizes configurable from a single source:
```go
const (
    DataBlockSize  = 128   // For sparse data structures (postings)
    IndexBlockSize = 1024  // For all B-tree indexes (primindex, dicindex, prefix)
)
```

### Related Work
- Bitmap postings format (already implemented) for low-cardinality fields
- Dictionary compression ratios already good for low-cardinality fields
- This optimization focuses on high-cardinality field storage efficiency
