# Schema Command Design

## Overview
The schema command is a CLI utility that parses and validates DDL schema files, displaying table structures and storage layout **without** creating the actual database. It provides comprehensive error reporting with context and shows the directory tree structure that would be created.

## Command-Line Interface

```bash
./step2 schema -path <schema-file.ddl> [-storage]
```

**Arguments:**
- `-path <file>`: Path to DDL schema file (required)
- `-storage`: Optional flag to show database directory tree structure

## Design Principles

1. **Error Accumulation**: Unlike `db.CreateDBDefinition()` which stops on first error (necessary for actual DB creation), the schema validator continues to find ALL validation errors to provide better user feedback.

2. **Context Display**: Parse errors show 2-3 lines of surrounding code context to help users quickly identify issues.

3. **Color Output**: Uses ANSI color codes for better readability (following STEP v1 pattern):
   - Red: Errors
   - Green: Success messages, summaries
   - Yellow: Section headers, warnings
   - Cyan: Schema/table names
   - Bold: Emphasis

4. **No Database Creation**: Only parses and validates; does not create directories or files.

## Package Structure

```
cli/
├── tree_printing.go      (exists - add strings import)
├── colors.go             (NEW - ANSI color constants)
├── schema_validator.go   (NEW - validation with error accumulation)
├── schema_display.go     (NEW - formatting and display functions)
└── schema.go             (NEW - main entry point)
```

## Core Data Structures

### ValidationResult
```go
type ValidationResult struct {
    Schema         *step2DDLparser.Schema  // Parsed schema (may be partial if errors)
    ParseError     error                   // nil if parse succeeded
    ValidationErrs []ValidationError       // All validation errors found
}
```

**Note**: We work directly with the parsed `Schema` structure. We do NOT create a `DBDefinition` since that requires filesystem operations and is unnecessary for validation/display. In the future, `CreateDBDefinition` could be refactored to use these validation helper functions.

### ValidationError
```go
type ValidationError struct {
    Category string // "table", "field", "set", "foreign_key", "duplicate", etc.
    Table    string // Table name where error occurred
    Field    string // Field name (if field-level error)
    Set      string // Set name (if set-level error)
    Message  string // Human-readable error message
}
```

### Color Constants
```go
const (
    ColorReset  = "\033[0m"
    ColorRed    = "\033[31m"
    ColorGreen  = "\033[32m"
    ColorYellow = "\033[33m"
    ColorBlue   = "\033[34m"
    ColorCyan   = "\033[36m"
    ColorBold   = "\033[1m"
)
```

## Key Functions

### cli/schema.go
```go
// Main entry point
func AnalyzeSchema(schemaPath string, showStorage bool) error
```

### cli/schema_validator.go
```go
// Validates schema and accumulates all errors (unlike CreateDBDefinition)
func ValidateSchemaForDisplay(schemaPath string) (*ValidationResult, error)

// Helper: Check if table name exists in schema
func tableExists(tables []step2DDLparser.Table, name string) bool

// Helper: Find table by name
func findTable(tables []step2DDLparser.Table, name string) *step2DDLparser.Table

// Validation checks (returns validation errors, does not stop on first error):
func validateTablesExist(schema *step2DDLparser.Schema) []ValidationError
func validateTableFields(schema *step2DDLparser.Schema) []ValidationError
func validatePrimaryKeys(schema *step2DDLparser.Schema) []ValidationError
func validateForeignKeys(schema *step2DDLparser.Schema) []ValidationError
func validateSets(schema *step2DDLparser.Schema) []ValidationError
```

### cli/schema_display.go
```go
// Format parse error with file context
func FormatParseErrorWithContext(err error, filePath string) string

// Format all validation errors grouped by category
func FormatValidationErrors(result *ValidationResult) string

// Display schema information (success case)
func DisplaySchemaInfo(result *ValidationResult, showStorage bool)

// Build directory tree from parsed Schema
func BuildStorageTree(schema *step2DDLparser.Schema) *Node
```

## Validation Logic

### Checks Performed (All Errors Accumulated)

**Schema-Level:**
1. Schema name must not be empty
2. Schema must contain at least one table
3. All table names must be unique

**Table-Level:**
4. Each table must contain at least one field
5. All field names within a table must be unique
6. All set names within a table must be unique
7. Each table must have at most one field marked as primary key

**Field-Level:**
8. If field has primary key constraint, type must be: SMALLINT, INT, BIGINT, or CHAR[4-32]
9. If field is CHAR type, size must be specified and > 0
10. If field has foreign key, referenced table must exist
11. If field has foreign key, referenced table must have a primary key
12. If field has foreign key, types must match (including CHAR sizes)

**Set-Level:**
13. For each set, member table must exist in schema

## Output Formats

### Parse Error Output
```
[RED]ERROR: Schema parsing failed[RESET]

[RED]Parse Error:[RESET]
  File: Customer_Employee.ddl
  Line 15, Column 23: expected field type, found 'INTGER'

[YELLOW]Context:[RESET]
  14 | CREATE TABLE Employee (
  15 |     employee_id INTGER PRIMARY KEY,
                       [RED]^^^^^^[RESET]
  16 |     name STRING(100),
```

### Validation Error Output
```
[RED]ERROR: Schema validation failed[RESET]

[RED]Found 5 validation errors:[RESET]

[YELLOW]Table-Level Errors:[RESET]
  • Table 'Employee' has no fields defined
  • Table 'Order': Duplicate field name 'id' found

[YELLOW]Field-Level Errors:[RESET]
  • Table 'Customer', field 'status_code': Primary key must be SMALLINT, INT, BIGINT, or CHAR[4-32]
  • Table 'Order', field 'customer_id': Foreign key references non-existent table 'Client'

[YELLOW]Set-Level Errors:[RESET]
  • Table 'Customer', set 'orders': Member table 'Order' not found in schema

[GREEN]Successfully parsed: 3 tables, 15 fields[RESET]
```

### Success Output (Basic Mode)
```
[GREEN]✓ Schema validated successfully[RESET]

Schema: [CYAN]College[RESET]

[YELLOW]Table: Student[RESET] (4 fields, 1 set)
  Primary Key: student_id (INT)
  Fields:
    • student_id      INT           PRIMARY KEY
    • name            STRING[100]   OPTIONAL
    • email           STRING        (unlimited)
    • enrolled        BOOLEAN
  Sets:
    • courses → Course

[YELLOW]Table: Course[RESET] (3 fields)
  Primary Key: course_code (CHAR[8])
  Fields:
    • course_code     CHAR[8]       PRIMARY KEY
    • title           STRING[200]
    • credits         SMALLINT

[GREEN]Summary: 2 tables, 7 fields, 3 dictionaries[RESET]
```

### Success Output (With -storage Flag)
```
[GREEN]✓ Schema validated successfully[RESET]

Schema: [CYAN]College[RESET]

[YELLOW]Database Directory Structure:[RESET]

College/
├── schema.json
├── Student/
│   ├── records.dat
│   ├── primindex.dat
│   ├── name/
│   │   ├── strings.dat
│   │   ├── offsets.dat
│   │   ├── postings.dat
│   │   ├── index.dat
│   │   └── prefix.dat
│   ├── email/
│   │   ├── strings.dat
│   │   ├── offsets.dat
│   │   ├── postings.dat
│   │   ├── index.dat
│   │   └── prefix.dat
│   └── courses.dat
└── Course/
    ├── records.dat
    ├── primindex.dat
    └── title/
        ├── strings.dat
        ├── offsets.dat
        ├── postings.dat
        ├── index.dat
        └── prefix.dat

[GREEN]Summary: 2 tables, 3 dictionaries, 22 files[RESET]
```

## Implementation Flow

```
AnalyzeSchema(path, showStorage)
  │
  ├─→ ValidateSchemaForDisplay(path)
  │     │
  │     ├─→ Read file
  │     ├─→ ParseSchema() [from step2DDLparser]
  │     │     └─→ Return ParseError if fails
  │     │
  │     ├─→ Run all validation checks (accumulate errors):
  │     │     ├─→ validateTablesExist()
  │     │     ├─→ validateTableFields()
  │     │     ├─→ validatePrimaryKeys()
  │     │     ├─→ validateForeignKeys()
  │     │     └─→ validateSets()
  │     │
  │     └─→ Return ValidationResult with Schema and any errors
  │
  ├─→ Handle ParseError:
  │     └─→ FormatParseErrorWithContext()
  │
  ├─→ Handle ValidationErrors:
  │     └─→ FormatValidationErrors()
  │
  └─→ Display Success:
        ├─→ DisplaySchemaInfo()
        └─→ If showStorage: BuildStorageTree() + DrawTree()
```

## Differences from CreateDBDefinition

| Aspect | CreateDBDefinition | ValidateSchemaForDisplay |
|--------|-------------------|------------------------|
| Purpose | Create actual database | Validate and display only |
| Error handling | Stop on first error | Accumulate all errors |
| Directory checking | Verify paths, create dirs | No filesystem operations |
| File creation | Create all DB files | None |
| DBDefinition creation | Creates full DBDefinition with offsets, file refs | Does not create DBDefinition |
| Return value | *DBDefinition or error | ValidationResult with Schema and errors |
| DB directory path | Required parameter | Not needed (no creation) |

**Future Optimization**: `CreateDBDefinition` could be refactored to use the validation helper functions from `ValidateSchemaForDisplay`, reducing code duplication while maintaining the "stop on first error" behavior.

## Tree Structure Building

For `-storage` flag, build tree from parsed Schema:

```go
func BuildStorageTree(schema *step2DDLparser.Schema) *Node {
    root := &Node{Name: schema.Name + "/"}

    // schema.json
    root.Children = append(root.Children, &Node{Name: "schema.json"})

    // For each table
    for _, table := range schema.Tables {
        tableNode := &Node{Name: table.Name + "/"}

        // records.dat
        tableNode.Children = append(tableNode.Children, &Node{Name: "records.dat"})

        // primindex.dat (if has primary key)
        hasPrimaryKey := false
        for _, col := range table.Columns {
            for _, constraint := range col.Constraints {
                if constraint.IsPrimaryKey {
                    hasPrimaryKey = true
                    break
                }
            }
            if hasPrimaryKey {
                break
            }
        }
        if hasPrimaryKey {
            tableNode.Children = append(tableNode.Children, &Node{Name: "primindex.dat"})
        }

        // Dictionary subdirectories (for STRING fields)
        for _, col := range table.Columns {
            if col.Type == uint8(db.STRING) {  // Type 6 = STRING
                dictNode := &Node{Name: col.Name + "/"}
                    {Name: "offsets.dat"},
                    {Name: "postings.dat"},
                    {Name: "index.dat"},
                    {Name: "prefix.dat"},
                }
                tableNode.Children = append(tableNode.Children, dictNode)
            }
        }

        // Set files
        for _, set := range table.Sets {
            tableNode.Children = append(tableNode.Children, &Node{Name: set.Name + ".dat"})
        }

        root.Children = append(root.Children, tableNode)
    }

    return root
}
```

## Future Enhancements (Not in Initial Implementation)

1. **Size Estimator**: Add `-estimate` flag with parameters for:
   - Expected number of records per table
   - Average string lengths
   - Shows projected storage sizes

2. **Export Options**: Add `-json` or `-xml` flags to output schema in structured format

3. **Comparison Mode**: Compare two schema versions and show differences

4. **Warning Messages**: Non-fatal warnings (e.g., "Table has no primary key", "String field has no size limit")

## Testing Strategy

Test cases needed:
1. Valid schema (basic) - should show success
2. Valid schema (complex with sets, foreign keys) - should show success
3. Parse error - should show error with context
4. Multiple validation errors - should accumulate and display all
5. Schema with no tables - validation error
6. Duplicate table names - validation error
7. Invalid primary key type - validation error
8. Foreign key to non-existent table - validation error
9. Set referencing non-existent table - validation error
10. Empty file path - file read error

## Notes

- The validator works directly with the parsed `Schema` structure - no `DBDefinition` is created
- No filesystem operations (directory creation, file creation) are performed
- Storage tree is built from the `Schema` structure by analyzing field types and constraints
- Color output should be consistent with STEP v1 CLI conventions
- Tree drawing uses existing `tree_printing.go` utilities
- Parse errors get line/column context by re-reading the file
- Future optimization: `CreateDBDefinition` could use these validation helper functions to reduce code duplication
