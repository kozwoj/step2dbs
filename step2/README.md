# STEP2 Database System

STEP2 is a CODASYL-inspired embedded database engine written in Go. It is a successor to STEP, originally designed and implemented in the early 1980s at the Technical University of Wrocław for MERA 400 minicomputers. It removes many historical hardware limitations, yet keeps the minimalistic, file-based approach to the storage layer. It implements a more complete data model while retaining the concept of **Sets** — named 1-to-N record relationships between tables.

## Features

- DDL schema parser for defining tables, columns, primary/foreign keys, and sets
- Fixed-length record storage with deleted-record reuse
- B+ tree primary indexes (via [indexing](https://github.com/kozwoj/indexing))
- STRING fields backed by dictionaries with inverted indexes and prefix search
- Set relationships (owner → members) stored as block-based postings lists
- JSON-in/JSON-out DML layer with atomic batch operations and best-effort undo
- REST API server (Chi router)
- PowerShell client module

## Packages

### `db` — Database Lifecycle and Schema

Creates, opens, and closes databases. Manages a global thread-safe `DBDefinition` singleton that describes all tables, fields, indexes, and sets.

```go
db.CreateDB("/path/to/dbdir", "schema.ddl")
db.OpenDB("/path/to/dbdir")
defer db.CloseDB()

def := db.Definition()
```

### `record` — Record Storage and CRUD

Low-level binary serialization, file I/O, and CRUD operations on records.

```go
id, err := record.AddNewRecord("Students", fields, db.Definition())
rec, err := record.GetRecordByID("Students", id, db.Definition())
record.UpdateRecord("Students", id, updatedFields, db.Definition())
record.DeleteRecord("Students", id, db.Definition())
```

### `dml` — Data Manipulation Language

Wraps `db` and `record` with JSON serialization. Every function takes a JSON string and returns a JSON string with `status` and `errors` fields. Mutating operations are serialized via an internal mutex.

```go
result, _ := dml.AddNewRecord_DML(`{"tableName":"Students","record":{"First_name":"Alice","Last_name":"Smith"}}`)
result, _ := dml.GetRecordByID_DML(`{"tableName":"Students","recordID":1}`)
```

**Batch operations** execute multiple commands atomically. If any command fails, previously executed commands are undone on a best-effort basis:

```go
result, _ := dml.Batch_DML(`{
  "commands": [
    {"add": {"tableName":"Students","record":{...}}},
    {"update": {"tableName":"Students","recordID":1,"record":{...}}},
    {"delete": {"tableName":"Students","recordID":2}},
    {"addSetMember": {"ownerTable":"Departments","ownerRecordID":1,"setName":"Dept_Teachers","memberRecordID":5}},
    {"removeSetMember": {"ownerTable":"Departments","ownerRecordID":1,"setName":"Dept_Teachers","memberRecordID":5}}
  ]
}`)
```

### `server` — REST API

HTTP server built on [chi](https://github.com/go-chi/chi). All endpoints live under `/step2`.

```go
server.Start(8080)
```

| Route | Method | Description |
|-------|--------|-------------|
| `/step2/db/create` | POST | Create database from DDL |
| `/step2/db/open` | POST | Open existing database |
| `/step2/db/close` | POST | Close database |
| `/step2/db/info/schema` | GET | Get schema definition |
| `/step2/db/info/tables` | POST | Get table statistics |
| `/step2/record/add` | POST | Add a record |
| `/step2/record/update` | POST | Update a record |
| `/step2/record/delete` | POST | Delete a record |
| `/step2/record/get/byid` | POST | Get record by ID |
| `/step2/record/get/id` | POST | Get record ID by key |
| `/step2/record/get/next` | POST | Get next record |
| `/step2/record/get/bykey` | POST | Get record by primary key |
| `/step2/record/get/bystring` | POST | Find records by string field |
| `/step2/record/get/bysubstring` | POST | Find records by substring |
| `/step2/set/addmember` | POST | Add member to set |
| `/step2/set/getmembers` | POST | Get set members |
| `/step2/set/removemember` | POST | Remove member from set |
| `/step2/batch/` | POST | Execute batch commands |

Every route also supports GET for usage help.

### `step2DDLparser` — Schema Definition Language

Parses DDL schema files into an AST. Supports tables, typed columns, primary/foreign keys, optional fields, and set declarations.

```
SCHEMA College
TABLE Departments (
    Department_code CHAR[8] PRIMARY KEY,
    Department_name STRING(50),
    Building        STRING(30) OPTIONAL
)
SETS (DeptCourses Courses, DeptTeachers Teachers);

TABLE Students (
    Student_id   INT PRIMARY KEY,
    First_name   STRING(30),
    Last_name    STRING(30),
    Department   CHAR[8] FOREIGN KEY Departments
);
```

**Supported types:** `SMALLINT`, `INT`, `BIGINT`, `DECIMAL`, `FLOAT`, `STRING[(maxlen)]`, `CHAR[size]`, `BOOLEAN`, `DATE`, `TIME`

### `testdb` — Test Fixture

Provides `CreateAndPopulateNIPDatabase` which creates the North Idaho Polytechnic sample database from DDL and JSONL data files. Used by integration tests across step2 and dependent modules.

### `scripts` — PowerShell Client

`step2.ps1` is a PowerShell module with cmdlets for all REST endpoints, including `Process-Batch` and `Process-BatchFile` for batch operations.

## Storage Layout

A database directory has this structure:

```
SchemaName/
├── schema.json
├── TableName/
│   ├── records.dat          # fixed-length record storage
│   ├── primindex.dat        # B+ tree primary key index
│   ├── SetName.dat          # set membership (per set)
│   └── StringField/         # per STRING column
│       ├── strings.dat
│       ├── offsets.dat
│       ├── index.dat
│       ├── postings.dat
│       └── prefix.dat
```

## Install

```
go get github.com/kozwoj/step2
```

## Dependencies

- [indexing](https://github.com/kozwoj/indexing) — B+ tree indexes, string dictionaries, postings lists
- [chi](https://github.com/go-chi/chi) — HTTP router

## Documentation

- [Architecture Overview](docs/architecture/Overview.md)
- [DDL Grammar](docs/architecture/step2_DDL_grammar.md)
- [DML Commands](docs/architecture/step2_DML_commands.md)
- [REST Routes](docs/architecture/step2_REST_routs.md)
