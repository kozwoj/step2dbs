# Design of STEP2 CLI

The document described the refactoring of the code in step2, step2query and step2cli folders to create a CLI for the STEP2 that supports
- schema compilation and analysis 
- executing queries against an existing DB
- starting STEP2 server

Currently the code of the CLI is in two places
- the main of step2
- step2/cli directory

The code in step2/cli takes dependency step2/db and step2/step2DDLparser.

The step2 main takes dependency on step2/cli and step2/server".

The printUsage() function in step2 main described the current functionality of the CLI. 

``` go
func printUsage() {
	fmt.Println("STEP2 - Database Management System")
	fmt.Println("\nUsage:")
	fmt.Println("  step2 <command> [options]")
	fmt.Println("\nAvailable Commands:")
	fmt.Println("  server    Start the REST API server")
	fmt.Println("  schema    Parse and analyze a DDL schema file")
	fmt.Println("\nServer Command Options:")
	fmt.Println("  step2 server -port <port_number>")
	fmt.Println("    -port    Port number for the REST server (default: 8080)")
	fmt.Println("\nSchema Command Options:")
	fmt.Println("  step2 schema -path <schema_file_path> [-storage]")
	fmt.Println("    -path      Path to the DDL schema file (required)")
	fmt.Println("    -storage   Display database storage directory structure")
	fmt.Println("\nExamples:")
	fmt.Println("  step2 server -port 8080")
	fmt.Println("  step2 schema -path docs/testdata/College.ddl")
	fmt.Println("  step2 schema -path docs/testdata/College.ddl -storage")
}
```

The restructuring has two objectives objective
- add the function of executing a query against existing DB to CLI
- move all CLI code into new folder called step2cil and move the main for STEP2 system into that folder. 
- 
This will allow us to avoid dependency cycles. 

The new command in the CLI should be: query with two arguments
- dbpath    - path to the existing DB
- pipeline  - the step2query query pipeline definition

The new command should 
- open the DB
- get its DBDefinition
- parse, validate, plan and execute the pipeline
- return the output state of the last return stage in the pipeline 

The result of the query should be returned as an array of JSON object corresponding to the returned record. 

## Refactoring steps

**Step 1 — Create go.mod in `step2cli`**
- Initialize a new Go module `step2cli` with dependencies on `step2` (for cli, server, db, dml) and `step2query` (for the future `query` command).
- Add `replace` directives pointing to the local `step2` and `step2query` modules (same pattern as `step2query/go.mod`).

**Step 2 — Copy `step2/cli/*.go` into `step2cli/cli/`**
- Copy all five files: `colors.go`, `schema.go`, `schema_display.go`, `schema_validator.go`, `tree_printing.go` (and the `test/` subfolder if it has anything needed).
- Change the import paths from `step2/cli` internal references (if any) but keep the `step2/db` and `step2/step2DDLparser` imports as-is — those still live in the `step2` module and are accessed as external dependencies now.

**Step 3 — Create `step2cli/main.go`**
- Move the content of `step2/main.go` into `step2cli/main.go`.
- Update imports: `step2/cli` → `step2cli/cli`, `step2/server` → `step2/server` (server stays in `step2` module, now referenced as an external dependency).
- Keep the existing server and `schema` subcommands working.

**Step 4 — Run `go mod tidy` in `step2cli`**
- Let Go resolve all transitive dependencies (chi, roaring, xxh3, indexing, etc.) and populate go.sum.

**Step 5 — Verify the build compiles**
- Run `go build src.` in `step2cli` to confirm everything links.

**Step 6 — Run existing CLI tests**
- If `step2/cli/test/` has tests, make sure they're copied and pass from the new location with `go test step2query.`.

**Step 7 — Remove the old `step2/cli/` directory and `step2/main.go`**
- After confirming step2cli works, delete the duplicated code from `step2`.
- `step2` becomes a library-only module (no `main` package), exporting db, dml, record, server, step2DDLparser.

**Step 8 — Update `step2/go.mod` if needed**
- If removing main.go eliminates the need for any direct dependencies (e.g., `chi` was only used by server, and server stays), no changes needed. Otherwise trim unused deps with `go mod tidy`.

**Step 9 — Verify all three modules still build and test**
- `cd step2 && go test src.`
- `cd step2query && go test src.`
- `cd step2cli && go build src.`

---

After these 9 steps, the `step2cli` folder owns the CLI binary and `main`, `step2` is a pure library, and you're ready to add the `query` subcommand that wires in `step2query`. Want to start executing?

## query command implementation

The new command in the CLI is query. It has two arguments
- dbpath    - path to the existing DB
- file  - file with step2query query pipeline definition (like queries in the examples file)

The logic of the command execution is as follows: 
- open existing DB using OpenDB() in step2\db\db_open.go. this will create the global singleton stepdb.Definition 
- parse, validate and build the pipeline following the logic exemplified in the test "TestPipelineExecuteWhereReturnStudentsYearTwoOrThree" in step2query\engine\stage_real_test.go
- convert the resulting output state, which is always an in memory table to JSON array of object, each representing one record in the result table
- return the JSON array
- in case of any errors (DB does not exist, query is not well formed, there was execution error) return the error as a singe JSON object

The function implementing the query command should go to cli\query.go. 

An example of the command would be: 
`./step2.exe query -dbpath "C:\temp\nip_test_db\College" -file ./query.txt`

where the file `query.txt` includes the query string. 
``` text
Students 
| where Students.State_or_Country == "Colorado" and Students.Year > 2 or Students.State_or_Country == "Nevada" and Students.Year == 1 
| return Students.Student_id, Students.Last_name, Students.Year, Students.State_or_Country
```

Putting the pipeline definitions in a separate file makes it bit more useful and bypassed quotation issues (quoting sting field values in `where` expressions)

Then the step2cli\main.go needs to be updated in two places
- the subcommand switch 
- printUsage function (for the example of query we may put a shorter pipeline)