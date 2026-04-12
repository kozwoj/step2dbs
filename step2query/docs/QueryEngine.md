 # Query Engine Notes

This document sketches the beginning of the step2query engine design. It starts after parsing and semantic validation and focuses on how a validated pipeline could be planned and executed.

## Engine Responsibilities

The query engine should:

- accept a validated query pipeline
- execute stages strictly left to right
- maintain the current working relation and working record type
- materialize new in-memory results after schema-transforming stages
- preserve the directed semantics of FK and SET navigation

The engine should not reinterpret the query into a general relational optimizer. The design goal is predictable execution that matches the language model.

## Execution Model

Each pipeline stage consumes one input table and produces one output table.

There are two broad classes of stages:

- filtering stages, which preserve the current schema
- transformation stages, which create a new in-memory table with a new schema

Conceptually:

```text
source table
  -> where
  -> navigate
    -> return projection
  -> where
  -> navigate
    -> return projection
  ...
  -> return result
```

The engine should treat the output of each stage as the only input visible to the next stage. In other words, each stage is atomic fully defined by its input, selection, navigation and projection. 

## Core Runtime Objects

An initial implementation will likely need runtime structures roughly like these:

```text
ValidatedQuery
ValidatedPipeline
WorkingTable
WorkingRecordType
LogicalPlan
PhysicalPlan
ExecutionContext
```

Useful properties include:

- resolved table and field metadata from the schema
- resolved FK and SET navigation metadata
- column layout for each intermediate in-memory table
- row iterators or materialized row buffers

## Stage Semantics

### Source Stage

The source stage opens a base table from the STEP2 database definition and uses the STEP2 DML commands to process selection and navigation (see [step2_DML_commands.md](../../step2/docs/architecture/step2_DML_commands.md))

Responsibilities:

- resolve the source table
- create the initial working record type from the table schema
- create a scan over stored records

### Where Stage

The `where` stage evaluates a boolean expression against the current working relation.

Responsibilities:

- evaluate field references against the current row shape
- compare typed values correctly
- emit only rows for which the predicate is true

It does not change the working record type.

### Navigate FK Stage

FK navigation expands rows by following a foreign-key value from the current row into a target table primary-key lookup.

Responsibilities:

- extract the FK value from the current row
- find the matching target record by primary key
- combine the source row and target record in a transient joined row
- project only the attributes named by `return`
- materialize the projected rows as the next working relation

This stage is directional. It is not a general join enumerator.

### Navigate SET Stage

SET navigation expands a source-table row into zero or more target rows through the stored postings list associated with the source record.

Responsibilities:

- access the SET metadata on the source table
- read the postings list for each selected source row
- fetch the target records referenced by the postings list
- build projected output rows according to `return`

Because postings are attached to stored source rows, SET navigation should only be allowed while the engine is still operating directly on source-table rows from the first stage of navigation.

### Return Stage

The `return` stage is the schema boundary of the pipeline.

Responsibilities:

- define the exact output column layout of the next working relation
- copy only the requested values
- discard all other source and target attributes

In practice, a `return` immediately attached to `navigate` can be treated as part of the same physical operator.

## Logical Plan

The first planning layer should convert a validated AST into a sequence of logical operators.

Example:

```text
Scan(Customers)
 -> Filter(Customers.Status == "Active")
 -> NavigateFK(target=Orders, on=Customers.CustomerID == Orders.CustomerID)
 -> Project(Customers.CustomerName, Orders.OrderID, Orders.Amount)
 -> Filter(Orders.Amount > 1000)
 -> NavigateFK(target=Products, on=Orders.ProductID == Products.ProductID)
 -> Project(Customers.CustomerName, Orders.OrderID, Products.ProductName, Products.Price)
 -> Filter(Products.Price > 50)
```

The logical plan should remain close to source semantics. The main purpose is not aggressive optimization, but a clean transition into execution.

## Physical Plan

The physical plan decides how each logical operator runs against STEP2 storage and in-memory intermediate results.

Possible physical operators include:

- table scan
- in-memory row filter
- FK lookup operator
- SET expansion operator
- projection/materialization operator

For the first implementation, a simple pipeline of materialized stages is likely the most reliable approach:

1. Execute one stage.
2. Materialize the output relation in memory.
3. Pass that relation to the next stage.

This will be less efficient than streaming in some cases, but it aligns well with the language rule that each stage defines the next working record type.

## Validation and Planning Boundary

By the time the engine starts planning, the query should already be validated. That means the planner should not need to guess about:

- whether a table or field exists
- whether a navigation edge is legal
- whether a field survived an earlier projection
- whether operand types are compatible

The planner should work from resolved metadata, not raw names.

## Result Semantics

A few execution semantics still need explicit decisions:

- whether intermediate or final relations preserve duplicate rows
- whether row order is stable or undefined unless explicitly sorted in a future extension
- how null or missing target rows behave during navigation
- whether navigation failures are filtered out or reported as execution errors

These choices should be made early because they affect both validation rules and operator behavior.

## Suggested Initial Implementation Path

A practical build order would be:

1. tokenizer
2. parser and AST
3. schema-aware validator
4. logical plan builder
5. simple materializing execution engine
6. diagnostics and explain-style output

The existing prototype in `main.go` already loads a STEP2 schema and exposes table and field metadata, which is the right starting point for the validator and planner.