## Where expression analysis for dictionary/index use

If a query begins with `TableXYZ | where BoolExpr | ...`, it may be possible to optimize the search for records in `TableXYZ` for which `BoolExpr(record) == true`.

This is possible when the boolean expression contains searchable leaf predicates, meaning leaf predicates whose exact matching record-ID set can be obtained directly through a dictionary or index lookup, without scanning records one by one.

In the current design, the searchable leaf predicates are:

- exact match on a string field, for example `Students.Last_name == "Johnson"`
- prefix match on a string field, for example `Students.Last_name like "Jo*"`
- exact match on the primary key field, for example `Students.Student_id == "NIP2209001"`

When such leaf predicates appear inside a larger boolean expression, the engine may be able to:

- first obtain the candidate record IDs that satisfy the searchable leaf predicates
- then evaluate the rest of the boolean expression only on those candidate records

Examples:

1. Selection based on primary key

   ```text
   Students
       | where Students.Student_id == "NIP2409002" and (...)
       | return Students.First_name, Students.Last_name
   ```

   This query can be optimized by first finding the record with `Student_id == "NIP2409002"` using the primary-key index, and then evaluating the remaining subexpression `( ... )` only on that record.

2. Selection based on full string value

   ```text
   Students
       | where Students.Last_name == "Perry" or Students.Last_name == "Jackson" and (...)
       | return Students.First_name, Students.Last_name
   ```

   Because `and` binds more strongly than `or`, the expression is interpreted as:

   ```text
   Students.Last_name == "Perry" or (Students.Last_name == "Jackson" and (...))
   ```

   This query can be optimized by:

   - getting the record IDs in the postings list for `"Perry"` from the `Last_name` dictionary
   - getting the record IDs in the postings list for `"Jackson"` from the `Last_name` dictionary and then applying `( ... )` only to those records
   - taking the union of the two result sets

3. Selection based on prefix

   ```text
   Students
       | where Students.Last_name like "Je*" and (...)
       | return Students.First_name, Students.Last_name, Students.Year
   ```

   From the query optimization point of view, `Students.Last_name like "Je*"` is analogous to `Students.Last_name == "Perry"`, because both expressions return a set of matching record IDs.

To generalize, we can call the searchable leaf predicates above searchable set-expressions. The important question is not whether the tree contains a searchable leaf somewhere, but whether the root expression can be executed from a reduced candidate set or whether it requires evaluation over the full input set.

## Expression tree evaluation rules

Use the following bottom-up classification for each boolean subtree:

- `Searchable`: the subtree can return the exact matching record-ID set by dictionary or index lookup
- `FilterOnInput`: the subtree must be evaluated record-by-record on whatever input set it is given
- `NeedsFullInput`: the subtree must be evaluated on the full incoming record set to preserve correctness

Leaf classification:

- searchable leaf predicate -> `Searchable`
- non-searchable leaf predicate -> `FilterOnInput`

Composition rules:

- `A and B`
  If one side is `Searchable`, the result can still be evaluated on that side's candidate set, with the other side applied as a filter to those candidates. Therefore `and` does not by itself create `NeedsFullInput`.

- `A or B`
  If both sides are `Searchable`, the result is `Searchable` because the final record-ID set can be obtained as the union of the two child result sets.

- `A or B`
  If either side is not `Searchable`, the result becomes `NeedsFullInput`, because correctness requires evaluating the `or` expression over the full incoming record set.

The key execution rule is therefore:

- if the root expression, after bottom-up analysis, is classified as `NeedsFullInput`, then the engine should evaluate the entire AST on all input records
- otherwise, the engine may first obtain candidate record IDs from the `Searchable` parts of the tree and then evaluate the remaining predicates only on those candidates