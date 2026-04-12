# STEP2 Execution Support Inventory

## Purpose

This note inventories the STEP2 functions that are relevant for executing a parsed query pipeline.
The goal is to distinguish between:

- JSON-facing DML wrappers
- real foundation functions that the query engine can call directly
- lower-level storage helpers that those foundation functions depend on
- capabilities that do not currently exist as a single helper and therefore must be composed by the query engine

## Layering

The `step2/dml` package is not the execution surface for `step2query`.
It is a JSON wrapper layer intended for external callers.

For query execution, the relevant layers are:

- `step2/record`: foundation functions operating on tables, records, indices, dictionaries, and sets
- `step2/db`: lower-level helpers for set storage and schema metadata
- index/dictionary internals opened through objects already attached to `DBDefinition`

In practice, the inventory should therefore be organized around `record/*` entry points, with `db/*` and index/dictionary methods treated as support dependencies.

## Existing capabilities

### 1. Sequential access to a table

Available foundation functions:

- `record.GetRecordByID(tableName, recordID, dbDef)`
- `record.GetNextRecord(tableName, currentRecordID, dbDef)`

Query-engine wrappers added in `step2query`:

- `engine.FirstActiveRecordID(tableName, dbDef)`
- `engine.NextActiveRecordID(tableName, currentRecordID, dbDef)`

Observed behavior:

- `GetRecordByID` reads the raw record bytes, checks the deleted flag, strips the header, and deserializes the record into `map[string]interface{}`.
- `GetNextRecord` scans forward from a record ID until it finds the next non-deleted record and returns both the materialized record and the next record ID.
- `engine.FirstActiveRecordID` fills the missing bootstrap operation for DB-backed source scans.
- `engine.NextActiveRecordID` provides ID-only iteration for DB-backed working sets without forcing row deserialization.

Implication for query execution:

- A source table can already be iterated as a sequence of record IDs plus materialized rows.
- This supports a DB-backed source scan.
- The query engine no longer needs to rely on the implicit convention of calling `GetNextRecord(..., 0, ...)` to discover the first active row.
- The query engine can stay at the record-ID level while it remains in a DB-backed working set.

### 2. Primary-key lookup

Available foundation functions:

- `record.GetRecordID(tableName, primeKey, dbDef)`
- `record.GetRecordByKey(tableName, primeKey, dbDef)`

Observed behavior:

- `GetRecordID` validates the table and key type, then uses `tableDescription.PrimeIndex.Find(indexKey)`.
- `GetRecordByKey` is just `GetRecordID` followed by `GetRecordByID`.

Implication for query execution:

- Navigation to a target table by primary key is supported.
- This is a critical building block for foreign-key navigation.

### 3. Exact string filter

Available foundation function:

- `record.GetRecordsByString(tableName, fieldName, fieldValue, dbDef)`

Observed lower-level support:

- validates table and field
- requires `STRING` type
- uses `field.Dictionary.FindString(fieldValue)`
- uses `field.Dictionary.RetrievePostings(postingsRef)`
- returns matching `recordID`s

Implication for query execution:

- `where field = "literal"` on a DB-backed `STRING` field can be executed by index/dictionary lookup instead of row-by-row scan.

### 4. Prefix string filter

Available foundation function:

- `record.GetRecordsBySubstring(tableName, fieldName, substring, dbDef)`

Observed lower-level support:

- requires `STRING` type
- requires non-empty prefix of length `<= 8`
- uses `field.Dictionary.PrefixSearch(substring)`
- for each matching dictionary ID:
  - `field.Dictionary.GetStringByID(dictID)`
  - `field.Dictionary.FindString(actualString)`
  - `field.Dictionary.RetrievePostings(postingsRef)`
- deduplicates record IDs across all matched strings

Implication for query execution:

- `where field like "Koza*"` already has a storage-backed execution path.
- The parser-side limit on prefix length matches a real STEP2 storage limitation rather than an arbitrary language rule.

### 5. Date/time normalization and storage conversion

Available helper functions:

- `record.ParseDate(s)`
- `record.FormatDate(days)`
- `record.ParseCompactTime(s)`
- `record.ConvertCompactTime(s)`
- `record.FormatMillis(millis)`

Observed behavior:

- `ParseDate` converts `YYYY-MM-DD` into internal day-count representation relative to `2000-01-01`.
- `FormatDate` converts the internal day-count back to `YYYY-MM-DD`.
- `ParseCompactTime` validates accepted `TIME` literal formats.
- `ConvertCompactTime` converts `H:MM`, `H:MM:SS`, or `H:MM:SS.mmm` into milliseconds since midnight.
- `FormatMillis` converts the internal millisecond representation back to a compact display form.

Relevant serialization helpers:

- `record.serializeDate(...)` writes the internal date representation as `uint64` days.
- `record.serializeTime(...)` writes the internal time representation as `uint64` milliseconds.
- `record.deserializeDate(...)` returns a formatted date string using `FormatDate`.
- `record.deserializeTime(...)` returns a formatted time string derived from the stored millisecond value.

Implication for query execution:

- Expressions on `DATE` and `TIME` need these helpers even though STEP2 does not appear to provide dedicated indexed search helpers for those types.
- The engine should compare normalized values, not raw strings, whenever it evaluates typed `DATE` and `TIME` predicates.
- For literal handling, the parser/validator can keep typed date/time literals in source form, but the execution layer will need to normalize them with `ParseDate` and `ConvertCompactTime` before comparison.
- For row evaluation, the engine must either:
  - normalize the deserialized string value before comparing, or
  - add a lower-level path that reads the stored binary value directly and avoids string round-tripping.

### 6. Set navigation

Available foundation function:

- `record.GetSetMembers(ownerTableName, ownerRecordID, setName, dbDef)`

Observed lower-level support:

- reads owner record bytes
- determines the set slot from the table's set metadata
- reads the set block pointer from the record header
- if non-empty, uses:
  - `db.GetSetFileHeader(setDesc.MembersFile)`
  - `db.GetSetMembers(setDesc.MembersFile, currentBlockNumber, blockSize)`

Implication for query execution:

- `navigate set <name>` is directly supported as: owner record ID -> member record IDs.
- Materializing member rows still requires `GetRecordByID` or equivalent follow-up reads.

## Foreign-key navigation (implemented)

STEP2 does not provide FK navigation as a first-class storage operation. The query engine owns this as a composed operation in `engine/navigateFK_execute.go`:

1. `loadNavigateFKTargetRecord` — reads the FK field value from the input row, calls `record.GetRecordID` for PK lookup on the target table, then `record.GetRecordByID` to load the target record
2. `executeNavigateFK` — iterates the input state (DB-backed or in-memory), calls `loadNavigateFKTargetRecord` per row, projects output via `buildNavigateOutputRecord`, and collects results into a `ReturnWorkingSet`
3. `buildNavigateOutputRecord` — resolves each return item against the target or input row and writes to qualified output field names (shared by FK and set navigation)

If the engine directly scatters the composition logic across execution stages, the same logic will be duplicated in:

- single-record FK navigation
- FK navigation over a DB-backed working set
- FK navigation followed immediately by `return`
- FK navigation followed by another `where`

### What the wrapper should do

At minimum, the wrapper should:

- read the FK value from the source record
- normalize that value into the type expected by the target primary key lookup
- call `record.GetRecordID(...)` on the target table
- return a structured result that distinguishes:
  - successful navigation
  - no FK value
  - FK value present but no referenced target row found

### Suggested levels of abstraction

There are two useful levels.

#### 1. Single-record FK navigator

This is the core primitive.

Conceptually:

- input: `sourceTable`, `sourceRecordID`, `sourceField`, `targetTable`
- output: `targetRecordID` plus navigation status

This wrapper would internally use existing STEP2 functions such as:

- `record.GetRecordByID(sourceTable, sourceRecordID, dbDef)`
- `record.GetRecordID(targetTable, fkValue, dbDef)`

This is the smallest useful unit and keeps the composition logic in one place.

#### 2. Working-set FK navigator

This is the stage-facing primitive.

Conceptually:

- input: a DB-backed source working set plus the validated FK join description
- output: a DB-backed target working set or a stream/iterator of target record IDs

This wrapper should build on the single-record FK navigator rather than reimplement its logic.

### Why this belongs in `step2query`

The abstraction is query-engine specific for three reasons:

- navigation is a pipeline concept, not a storage primitive
- the engine, not STEP2, owns stage semantics and working-set representation
- the engine has already validated that the join is an FK-to-PK navigation, so the runtime wrapper can assume a narrower contract than generic STEP2 DML

### Immediate design consequence

`navigate <table> on fk = pk` should be implemented against a `step2query` wrapper layer that composes existing STEP2 primitives.
That wrapper should become the only code path used by FK navigation stages.

## Consequences for stage design

### DB-backed `where`

Already supported efficiently for at least:

- exact `STRING` equality
- `STRING` prefix search

Other predicates still appear to require row-by-row evaluation unless more STEP2 support exists elsewhere.
That includes `DATE` and `TIME` predicates: they have normalization helpers, but no dedicated lookup helper was found.

### `navigate set`

Already supported at the record-ID level.

### `navigate <table> on fk = pk`

Semantically supported, but not as a dedicated STEP2 primitive.
Current expectation is composition from existing helpers behind a `step2query` FK-navigation wrapper.

This has an architectural consequence:

- the engine should define its own FK-navigation wrapper and treat STEP2 as the storage-operation layer underneath it
- if the engine wants FK navigation to remain in a DB-backed representation, the working-set version of that wrapper should return target record IDs without forcing immediate full row materialization
- if the engine is willing to materialize during FK navigation, the same wrapper can still be used, with row reads added at the stage boundary

## Proposed execution-oriented inventory buckets

For the next pass, each query-engine operation should be cataloged under one of these buckets:

- source scan
- DB-backed filter by exact string
- DB-backed filter by string prefix
- DB-backed or scan-based filter by normalized date/time comparison
- DB-backed filter by generic predicate scan
- FK navigation via `step2query` wrapper over STEP2 primitives
- set navigation by member-ID expansion
- row materialization for `return`
- in-memory predicate evaluation over already materialized rows

## Preliminary conclusion

The strongest existing STEP2 execution primitives are:

- sequential record scan
- primary-key lookup
- dictionary-backed exact string filter
- dictionary-backed prefix filter
- date/time normalization helpers
- set-member retrieval

The main missing direct primitive for `step2query` is foreign-key navigation as a first-class operation.
The right place to add that primitive is the `step2query` execution layer, as a wrapper that composes existing STEP2 operations.