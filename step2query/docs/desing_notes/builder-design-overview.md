# Builder Conceptual Design

The builder is a component of the `step2query` engine that takes:

1. a validated AST of a query
2. a `DBDefinition`

and produces a pipeline description.

That pipeline is a sequence of pipeline stages separated by pipeline states, where:

- `pipeline stage` is a transformation from one pipeline state to the next, and corresponds to one query segment between `|` symbols
- `pipeline state` is the resolved runtime data-and-metadata state that is consumed by one pipeline stage and produced by the previous one

## Query stages

The parser breaks a STEP2 query into a sequence of query stages:

``` go
type Query struct {
	Source *SourceStage
	Stages []Stage
	Span   Span
}
```

The AST stage types are:

1. `WhereStage`
2. `NavigateFKStage`
3. `NavigateSetStage`
4. `ReturnStage`

## Pipeline states

There are three pipeline states:

1. `SourceDBTableSet`
    This is the initial DB table state of the pipeline, coming from `Query.Source`.

2. `DBTableWorkingSet`
    This is a DB-backed working set tied to one DB table. It is represented primarily as a sequence of record IDs that still refer to records in the source DB table.

3. `ReturnWorkingSet`
    This is an in-memory working set with explicit schema and materialized rows, defined either by the `return` clause of a navigation stage or by an explicit `return` pipeline stage.

## Pipeline stages 

There are 7 pipeline stages:

1. DBWhereStage
2. MemoryWhereStage
3. DBNavigateFKStage
4. MemoryNavigateFKStage
5. DBNavigateSetStage
6. DBReturnStage
7. MemoryReturnStage

There are more pipeline stages than query stages because the mapping is not 1-to-1.
Although each pipeline stage logically corresponds to one query-stage kind (`where`, `navigate`, `return`, etc.), the same query-stage kind can map to different pipeline-stage kinds depending on the current pipeline state.

The builder iterates over AST stage nodes and, based on their type and the current pipeline state, builds the description of the corresponding pipeline stage.

A pipeline-stage description includes:

- stage type
- description of input state (kind, schema and storage location) 
- description of the stage transformation 
- description of output state (kind, schema and storage location) 

The output state of one stage becomes the input state of the next stage.

Conceptually a pipeline looks like this:

```
initial State (DB Table) -> stage1 -> state -> stage2 -> ... -> stageN -> final/returned state
```

The STEP2 query language currently requires every query to end with a `return`, either as part of a navigation stage or as an explicit return stage.

Below are descriptions of the pipeline stages created based on:

1. AST node type
2. current pipeline state

### AST node type = WhereStage
- if state = SourceDBTableSet create DBWhereStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is the DB table itself
    - the transformation is execution of the `where` predicate over the table rows; it may be a sequential scan or may use STEP2 storage support such as dictionary postings, depending on the predicate
    - the output kind is DBTableWorkingSet (a list of record IDs) and the schema is still DBDefinition.TableDescription

- if state = DBTableWorkingSet create DBWhereStage pipeline stage
    - the state schema is the DBDefinition.TableDescription of the source table
    - the state storage is an in-memory list of record IDs, but those IDs still refer to records in the source DB table, so records are accessed through DB access functions
    - the transformation is sequential scan of the DB table records identified by those record IDs, followed by application of the `where` predicate
    - the output kind is DBTableWorkingSet (a list of record IDs) and the schema is still DBDefinition.TableDescription

- if state = ReturnWorkingSet create MemoryWhereStage pipeline stage
    - the state schema is in-memory table schema (simplified version of the in-DB table)
    - the state storage is an in-memory table where all records/rows are fully instantiated
    - the transformation is sequential scan of all in-memory records followed by application of the predicate to each of them
    - the output kind is ReturnWorkingSet with the same schema as the input state

### AST node type = NavigateFKStage
- if state = SourceDBTableSet create DBNavigateFKStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is the DB table itself
    - the transformation is: for each input record, follow the FK->PK navigation to the target table and generate the output in-memory record defined by the `return` clause
    - the output kind is ReturnWorkingSet with schema defined by the `return` clause of the stage definition

- if state = DBTableWorkingSet create DBNavigateFKStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is an in-memory list of record IDs
    - the transformation is: for each input record, follow the FK->PK navigation to the target table and generate the output in-memory record defined by the `return` clause
    - the output kind is ReturnWorkingSet with schema defined by the `return` clause of the stage definition

- if state = ReturnWorkingSet create MemoryNavigateFKStage pipeline stage
    - the state schema is in-memory table schema (simplified version of the in-DB table)
    - the state storage is an in-memory table where all records/rows are fully instantiated
    - the transformation is: for each input record, read the FK value from the in-memory row, navigate to the target table, and generate the output in-memory record defined by the `return` clause
    - the output kind is ReturnWorkingSet with schema defined by the `return` clause of the stage definition

### AST node type = NavigateSetStage
- if state = SourceDBTableSet create DBNavigateSetStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is the DB table itself
    - the transformation is: for each input record, get the record IDs of its named set, merge the input record with each member record, and generate the output in-memory record defined by the `return` clause
    - the output kind is ReturnWorkingSet with schema defined by the `return` clause of the stage definition 

- if state = DBTableWorkingSet create DBNavigateSetStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is an in-memory list of record IDs
    - the transformation is: for each input record, get the record IDs of its named set, merge the input record with each member record, and generate the output in-memory record defined by the `return` clause
    - the output kind is ReturnWorkingSet with schema defined by the `return` clause of the stage definition

- if state = ReturnWorkingSet return an error - this state-stage combination is not valid because in-memory records do not carry SET identity

### AST node type = ReturnStage
- if state = SourceDBTableSet create DBReturnStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is the DB table itself
    - the transformation is projection of each input record into an in-memory record defined by the return-stage definition
    - the output kind is ReturnWorkingSet with schema defined by the return-stage definition

- if state = DBTableWorkingSet create DBReturnStage pipeline stage
    - the state schema is the DBDefinition.TableDescription
    - the state storage is an in-memory list of record IDs
    - the transformation is projection of each input record into an in-memory record defined by the return-stage definition
    - the output kind is ReturnWorkingSet with schema defined by the return-stage definition

- if state = ReturnWorkingSet create MemoryReturnStage pipeline stage 
    - the state schema is in-memory table schema (simplified version of the in-DB table)
    - the state storage is an in-memory table where all records/rows are fully instantiated
    - the transformation is projection of each input record into an in-memory record defined by the return-stage definition
    - the output kind is ReturnWorkingSet with schema defined by the return-stage definition

## Next steps 
1. define definition of DBTableWorkingSet - should be simplified version of the DB table definition
2. define definition of pipeline stage 
    - abstraction of input state
        - kind
        - schema 
        - storage reference
    - representation of the input to transformations
        - where transformation - should be the predicate 
        - navigate FK->PK should be:  source table + FK + target table + PK
        - navigate set should be: source table + set name + target table
    - representation of the output set
        - kind
        - schema 
        - storage reference
3. define definition of the pipeline
4. outline the logic flow of the builder 

## Observation
- The pipeline builder should just define the pipeline in a way that is sufficient for the engine to execute it. So we should be able to test the builder before we continue the engine.
- The most interesting part of the engine will be analysis of the `where` transformation over DB-backed input to determine whether dictionaries and indexes can be used to optimize it.
