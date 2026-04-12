# STEP2 Database System CLI

Command-line interface for the [step2](https://github.com/kozwoj/step2) database engine. Provides three subcommands: 
- starting the STEP2 REST server
- validating a database schema
- executing STEP query language query

## Install

```
go install github.com/kozwoj/step2cli@latest
```

Or build from source:

```
go build -o step2.exe .
```

## Commands

### `server` — Start the REST API

```
step2 server [-port <port>]
```

Starts the step2 REST server. Default port is 8080.

### `schema` — Validate a Database Schema

```
step2 schema -path <schema_file> [-storage]
```

Parses and validates a database schema (a .DDL file) with comprehensive checks:

- Schema, table, and field structure validation
- Primary key type restrictions (SMALLINT, INT, BIGINT, CHAR[4-32])
- Foreign key reference and type compatibility
- Set member table existence
- Duplicate name detection across tables, fields, and sets

All errors are collected and reported grouped by category with contextual source highlighting.

With `-storage` argument present, the command displays the database directory/file tree, that would be created when the database will be instantiated.

Without `-storage` argument, the command displays information about the schema (tables, records, keys, dictionaries, etc.)

**Example:**

```
step2 schema -path College.ddl
```

```
step2 schema -path College.ddl -storage
```

### `query` — Execute a Query

```
step2 query -dbpath <db_path> -file <query_file>
```

Opens an existing database and executes a query pipeline from a text file. Results are output as a JSON array.

**Example query file, command, and output:**

```
Students
  | where Students.State_or_Country == "Colorado" and Students.Year > 2
  | return Students.Student_id, Students.Last_name, Students.Year, Students.State_or_Country
```

```
step2 query -dbpath ./mydb -file query.txt
```

Output on success:

```json
[
  {"Student_id": 105, "Last_name": "Garcia", "Year": 3, "State_or_Country": "Colorado"},
  ...
]
```

Output on error:

```json
{"error": "table 'Foo' does not exist"}
```

## Dependencies

- [step2](https://github.com/kozwoj/step2) — database engine
- [step2query](https://github.com/kozwoj/step2query) — query language parser and execution engine
- [indexing](https://github.com/kozwoj/indexing) — B+ tree indexes and dictionaries (transitive)
