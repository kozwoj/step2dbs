# step2query

A query language and execution engine for [step2](https://github.com/kozwoj/step2) databases. Queries are expressed as left-to-right pipelines that filter, navigate, and project records across tables and set relationships.

## Query Language

Queries follow a pipeline model. The pipeline always starts with a database table - the initial pipeline state. Each stage takes a table as its pipeline input state, and produces a table as its pipeline output state. Stages are separated by `|`. 

```
Students
  | where Students.Last_name == "Smith"
  | navigate Departments on Students.Department == Departments.Department_code
      return Departments.Department_name
```

### Stages

| Stage | Syntax | Description |
|-------|--------|-------------|
| **where** | `where <bool-expr>` | Filter records by condition |
| **navigate FK** | `navigate <Table> on <field> == <field> return <fields>` | Follow a foreign key to another table |
| **navigate set** | `navigate set <Owner.SetName> return <fields>` | Follow a set relationship (1:N expansion) |
| **return** | `return <fields>` | Project specific fields |

### Expressions

Boolean expressions support `and`, `or`, `not`, parentheses, and comparison operators: `==`, `!=`, `<`, `<=`, `>`, `>=`, `like`.

The `like` operator performs prefix matching (up to 8 characters followed by `*`):

```
Students | where Students.Last_name like "Sm*"
```

### Literals

| Type | Example |
|------|---------|
| String | `"Smith"` |
| Number | `42`, `3.14` |
| Boolean | `true`, `false` |
| Date | `2026-03-11` |
| Time | `09:30:00` |

### Constraints

- Pipelines are forward-only — no backward references or global joins
- Set navigation is only available while working with stored table rows
- Every pipeline must end with a projection (`return` or `navigate...return`)

## Example Queries

```
-- All students in a department
Students
  | where Students.Department == "CSCI0001"
  | return Students.First_name, Students.Last_name

-- Students with their department names
Students
  | navigate Departments on Students.Department == Departments.Department_code
      return Departments.Department_name, Students.First_name, Students.Last_name

-- All courses in a department via set navigation
Departments
  | where Departments.Department_code == "CSCI0001"
  | navigate set Departments.DeptCourses
      return Courses.Course_name, Courses.Credits
```

## Packages

### `parser` — Lexer, Parser, and Validator

Tokenizes and parses query strings into an AST. `ValidateAST` performs schema-aware semantic validation against a `DBDefinition`.

```go
query, err := parser.Parse(`Students | where Students.Last_name == "Smith" | return Students.First_name`)
err = parser.ValidateAST(query, dbDef)
```

### `builder` — Execution Plan Builder

Converts a validated AST into a `Pipeline` execution plan. Selects DB-backed or memory-backed stage variants based on the current state. Analyzes `where` expressions for index and dictionary optimization.

```go
plan, err := builder.BuildPipeline(query, dbDef)
```

**Where optimization:** The builder classifies predicates bottom-up into searchable leaves (primary key exact match, string exact match, string prefix match) combined with AND (intersection) and OR (union). Non-searchable predicates become residual filters.

### `pipeline` — Execution Plan Model

Pure data definitions for the pipeline: stage types, state descriptions, field references, and optimization plans. No execution logic.

**Stage types:** `DBWhereStage`, `MemoryWhereStage`, `DBNavigateFKStage`, `MemoryNavigateFKStage`, `DBNavigateSetStage`, `DBReturnStage`, `MemoryReturnStage`

**State model:**
- `StateSourceDBTableSet` — initial full table
- `StateDBTableWorkingSet` — filtered record ID set from a DB table
- `StateReturnWorkingSet` — materialized in-memory rows with projected schema

### `engine` — Pipeline Executor

Executes a `Pipeline` against an open step2 database. Each stage fully materializes its output before passing it to the next stage.

```go
db.OpenDB(dbPath)
defer db.CloseDB()

result, err := engine.ExecutePipeline(plan, db.Definition())
for result.GetFirstRecord() {
    // process result.CurrentRecord()
    if !result.GetNextRecord() {
        break
    }
}
```

## Install

```
go get github.com/kozwoj/step2query
```

## Dependencies

- [step2](https://github.com/kozwoj/step2) — database engine
- [indexing](https://github.com/kozwoj/indexing) — B+ tree indexes and dictionaries (transitive)

## Documentation

- [Overview](docs/Overview.md) — Conceptual overview and stage semantics
- [Query Language Description](docs/QueryLanguageDescription.md) — EBNF grammar and parser structure
- [Query Language Formal Model](docs/QueryLanguageFormalModel.md) — Algebraic model (σ, π, NavFK, NavSet)
- [Query Engine](docs/QueryEngine.md) — Execution model and implementation design
