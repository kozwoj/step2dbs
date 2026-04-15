# STEP2 Database System

The four modules (indexing, step2, step2cli and steps2query ) collectively implement a fully-functional, file-based database system called STEP2. 

STEP2 is a CODASYL-inspired embedded, relational database engine written in Go. It is a successor to STEP, originally designed and implemented in the early 1980s at the Technical University of Wrocław for MERA 400 minicomputers. It removes many historical hardware limitations, yet keeps the minimalistic, file-based approach to the storage layer. It implements a more complete data model, while retaining the concept of **Sets** — named 1-to-N record relationships between tables.

STEP2 implements 
- fixed-length record storage
- B+ tree indexes
- string dictionaries with inverted indexes
- named 1-to-N set relationships between tables
- a pipeline query language, and 
- a REST API

## Modules

| Module | Description |
|--------|-------------|
| [step2](step2/) | Core database engine — DDL parser, record CRUD, set operations, DML layer, REST server |
| [step2query](step2query/) | Pipeline query language parser and execution engine |
| [step2cli](step2cli/) | Command-line interface — server, schema validation, query execution |
| [indexing](indexing/) | Disk-backed B+ tree indexes and string dictionaries with postings lists |

## Dependencies

- **step2cli** → step2, step2query
- **step2query** → step2
- **step2** → indexing
- **indexing** → no internal dependencies

The `indexing` module takes no dependency of other modules, and therefore can be used as a library (see the README file in that module for references to the documentation describing both primary index and dictionary functionality). 

## Test Database

The step2 module includes data and functions to populate a small test database called NorthIdahoPolitechnic. Three ways to create and populate that DB are described in `step2\docs\testdata\NortIdahoPolitechnic\Data_Set_Description.md`.

The schema of the test database is defined in `step2\docs\testdata\College.ddl`. All sample queries in the `step2query` module are based on that schema.   

## The CLI and STEP2 REST Interfaces
 
Description of the STEP2 CLI and how to build it are in `step2cli\README.md`. The CLI can be used to query the test DB, validate new schemas, or run the STEP2 server engine, which exposes STEP2 REST interface. 

The simplest way to exercise the STEP2 REST interface is to use the PowerShell functions provided in `step2\scripts\step2.ps1` script file. Each function in that file corresponds to (calls into) one STEP2 DML command. The DML commands are described in `step2\docs\architecture\step2_DML_commands.md`.

## Local Development

This repository uses a Go workspace (`go.work`) to link all modules for local development. Changes in any module are immediately visible to dependent modules.

Each module also has `replace` directives in its `go.mod` pointing to sibling directories (e.g. `../indexing`). These ensure that `go build` and `go test` work correctly inside each module even without `go.work`, for example when running in CI or when a tool does not support workspaces.

To work on the STEP2 DBS locally do the following: 

```
git clone https://github.com/kozwoj/step2dbs.git
cd step2dbs
go work sync
```

The `go work sync` command should be run after cloning, and again whenever a module's dependencies change (e.g. a new external package is added or a `go.mod` is edited). It synchronizes the workspace's dependency graph across all modules.

Build and test all modules:

```
cd step2 && go build ./... && go test ./...
cd ../indexing && go build ./... && go test ./...
cd ../step2query && go build ./... && go test ./...
cd ../step2cli && go build ./... && go test ./...
```

Alternatively to clone the project to a different directory, e.g. `my-db-project` run: 

```
git clone https://github.com/kozwoj/step2dbs.git my-db-project
cd my-db-project
go work sync
```

## License

[MIT](LICENSE)
