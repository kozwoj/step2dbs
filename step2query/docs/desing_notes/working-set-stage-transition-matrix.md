# Working Set Stage Transition Matrix

This note enumerates the legal combinations of input working set, stage kind, and output working set.

The purpose is to define the consume/produce rules before stage instantiation types are implemented.

## Working Input Kinds

The current design implies three distinct input categories.

### 1. Source DB Table

This is not yet a working set object. It is the initial table reference from the query source stage.

Properties:

- represented by STEP2 table metadata plus storage access mechanisms
- has full DB schema from `DBDefinition`
- supports storage-backed filtering and navigation

### 2. `DBTableWorkingSet`

This is the filtered working set derived from a DB table.

Properties:

- still tied to one base DB table
- represented by table identity plus selected record IDs
- preserves the original DB table schema
- still eligible for storage-aware operations

### 3. `ReturnWorkingSet`

This is the in-memory working set created by `return` or `navigate ... return`.

Properties:

- explicit in-memory schema
- materialized rows
- fields available only if they survived the most recent projection
- subsequent `where` is evaluated row by row in memory

## Stage Kinds

The stage kinds are:

- `where`
- `navigate` FK
- `navigate set`
- `return`

## Transition Matrix

The matrix below focuses on what each stage consumes and produces.

### Source DB Table -> `where` -> `DBTableWorkingSet`

Status:

- legal

Meaning:

- filter source-table records using a schema-aware predicate
- produce a filtered set of record IDs rather than materialized records

Execution notes:

- may use indexes, dictionaries, prefix lookups, or other STEP2 storage operations

### Source DB Table -> `navigate` FK -> `ReturnWorkingSet`

Status:

- legal

Meaning:

- the implementation treats the source DB table as a directly navigable input
- for each source record, follow the FK to PK relationship to the target table
- combine source and target values as needed for projection
- materialize the `return` result as an in-memory working set

### Source DB Table -> `navigate set` -> `ReturnWorkingSet`

Status:

- legal as first navigation path if source is treated as directly navigable

Meaning:

- access SET postings from source table rows
- fetch member rows
- apply the required `return`
- produce an in-memory working set

Constraint:

- set navigation is only legal while still attached to stored source-table rows

### Source DB Table -> `return` -> `ReturnWorkingSet`

Status:

- syntactically legal if `return` is allowed as a standalone stage after source

Meaning:

- project source table rows into an in-memory working table

Open question:

- whether standalone `return` immediately after source should be encouraged, or whether it is mainly a normalization step used by implementation

### `DBTableWorkingSet` -> `where` -> `DBTableWorkingSet`

Status:

- legal

Meaning:

- chain filtering stages over the same base table
- progressively narrow the selected record IDs

This is one of the key reasons for requiring explicit final `return`.

### `DBTableWorkingSet` -> `navigate` FK -> `ReturnWorkingSet`

Status:

- legal

Meaning:

- use selected source-table record IDs
- follow FK to PK relationships
- combine source and target values as needed for projection
- materialize the `return` result as an in-memory working set

### `DBTableWorkingSet` -> `navigate set` -> `ReturnWorkingSet`

Status:

- legal only if this is still the first navigation from stored source rows

Meaning:

- use the stored postings associated with the selected source rows
- fetch set members
- apply `return`
- materialize in memory

Important constraint:

- once the pipeline moves to `ReturnWorkingSet`, SET postings are no longer available

### `DBTableWorkingSet` -> `return` -> `ReturnWorkingSet`

Status:

- legal

Meaning:

- project selected DB-backed rows into an in-memory working set

Use case:

- explicit final projection after one or more `where` stages

### `ReturnWorkingSet` -> `where` -> `ReturnWorkingSet`

Status:

- legal

Meaning:

- evaluate the predicate row by row over materialized in-memory rows
- preserve the same in-memory schema

### `ReturnWorkingSet` -> `navigate` FK -> `ReturnWorkingSet`

Status:

- legal, but only if the FK field survived earlier projection

Meaning:

- read FK values from the in-memory rows
- look up matching target records
- project result with required `return`
- materialize a new in-memory working set

### `ReturnWorkingSet` -> `navigate set` -> not allowed

Status:

- illegal

Reason:

- SET postings are attached to stored table rows, not to in-memory projected rows

### `ReturnWorkingSet` -> `return` -> `ReturnWorkingSet`

Status:

- legal

Meaning:

- re-project the in-memory schema into a narrower or reordered in-memory schema

Use case:

- explicit final output shaping after in-memory filtering

## Condensed Matrix

```text
Source DB Table   -> where         -> DBTableWorkingSet
Source DB Table   -> navigate SET  -> ReturnWorkingSet
Source DB Table   -> return        -> ReturnWorkingSet
Source DB Table   -> navigate FK   -> ReturnWorkingSet [the concern it the cardinality of the resulting set]


DBTableWorkingSet -> where         -> DBTableWorkingSet
DBTableWorkingSet -> navigate FK   -> ReturnWorkingSet
DBTableWorkingSet -> navigate SET  -> ReturnWorkingSet   [first navigation only]
DBTableWorkingSet -> return        -> ReturnWorkingSet

ReturnWorkingSet  -> where         -> ReturnWorkingSet
ReturnWorkingSet  -> navigate FK   -> ReturnWorkingSet
ReturnWorkingSet  -> return        -> ReturnWorkingSet

ReturnWorkingSet  -> navigate SET  -> illegal 
```

## Design Observations

### Observation 1

The important semantic boundary is not just stage kind, but whether the pipeline is still DB-backed or has crossed into in-memory representation.

### Observation 2

`return` is the main boundary-crossing stage:

- DB-backed input -> in-memory output
- in-memory input -> new in-memory output

### Observation 3

`where` is polymorphic:

- storage-backed filtering on DB-backed input
- row-by-row filtering on in-memory input

### Observation 4

`navigate set` is special because it depends on source-row storage identity, so it is only valid before that identity is lost.

## Immediate Next Design Decision

Before implementing stage instantiation, one remaining point should be made explicit:

- whether the source DB table is treated as its own special case
- or whether it is normalized immediately into an implicit initial `DBTableWorkingSet`

The second option may simplify the transition logic because then all stage instantiation works over working-set objects from the beginning.

## Recommended Follow-Up

After review, the next note or implementation step should define:

- the exact runtime structs for `DBTableWorkingSet` and `ReturnWorkingSet`
- whether source normalization is explicit or implicit
- the minimal instantiation algorithm that walks AST stages and applies this matrix

## Review Comments

Logically there is not different between DB Table and DBTableWorkingSet. Both of them are sequences of recordIDs and and should implement the same interface functions `GetFirst() uint32`, and `GetNext(uint32) uint32` to iterate over those sequences. In both cases we can also find out the cardinality of the recordID sequences. Having a recordID both can be used to get the body of the corresponding record. However there or operations that apply to DB Table but not to DBTableWorkingSet that return one or more recordIDs (get posting list for a string, get posting list for a substring, and get recordID for a primary key). 

So unless there are some cardinality constraints we should support the: `Source DB Table -> navigate FK -> ReturnWorkingSet` case. it should be advised to have a `where` before it, but to small source DB Tables it should be fine. 

The next step, before we analyze the AST to create pipeline description: sequence of `input->stge->output`, we need to go back to STEP2 and identify all supporting functions we need to execute the pipeline. we can use the raw DML functions and/or their supporting functions. We may find out that we will have to add helper functions to STEP2 or write additional wrappers.  