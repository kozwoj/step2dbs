The directory contains test data for North Idaho Politechnic (NIP) DB.

## Creating an Instance of the NIP Database

The code creating the test NIP DB using the files in this directory is in the package `testdb`. There are three ways to create a populated NIP database instance.

### Option 1: Using the `createnipdb` command (simplest)

Build and run the standalone tool:

```
cd step2
go run ./testdb/cmd/createnipdb C:\temp\nip_db
```

Or build it first and run the binary:

```
cd step2
go build -o createnipdb.exe ./testdb/cmd/createnipdb
.\createnipdb.exe C:\temp\nip_db
```

The populated database will be at `C:\temp\nip_db\College`.

### Option 2: Using TestLoadNIPDatabase

The test `TestLoadNIPDatabase` in `db/db_info_test.go` can create and populate the database to a directory of your choice.

1. Open `db/db_info_test.go`
2. Comment out line 13: `tempDir := t.TempDir()`
3. Uncomment line 14: `tempDir := "C:\\temp\\nip_test_db"` (adjust the path as needed)
4. Run the test:
   ```
   go test ./db/ -run TestLoadNIPDatabase -v
   ```
5. The populated database will be at `C:\temp\nip_test_db\College` (or directory of your choice)

### Option 3: Using CreateAndPopulateNIPDatabase as a library function

Import the `testdb` package and call `CreateAndPopulateNIPDatabase` with a target directory:

```go
package main

import (
    "fmt"
    "step2/db"
    "step2/testdb"
)

func main() {
    dbPath, stats, err := testdb.CreateAndPopulateNIPDatabase("C:\\temp\\nip_db")
    if err != nil {
        fmt.Printf("Error: %v\n", err)
        return
    }
    defer db.CloseDB()

    fmt.Printf("Database created at: %s\n", dbPath)
    for table, count := range stats {
        fmt.Printf("  %s: %d records\n", table, count)
    }
}
```

The function creates the database, opens it, loads all JSONL data files (including Enrollment set relationships), and returns the database path and per-table record counts. The database remains open; call `db.CloseDB()` when finished.

## Data Description

- Departments
- Teachers
- Students
- Courses
- Classes
- Grades, and
- Majors

Each file contains lines of the following format {"tableName": string, "record": { field_name: value, ... }}, each corresponding to one row in the named table - all lines in one file are rows of the same table.

The files have to be loaded in a specific sequence because of the ForeignKey integrity constraint enforcement. Suggested sequence is: Departments -> Course -> Teachers -> Students -> Classes -> Majors -> Grades.

- There are 5 departments: Mathematics, Physics, Engineering, Computer Science and Chemistry
- The departments offer the following majors
  - Math: Statistics, Applied Mathematics and Math Education
  - Physics: Applied Physics, Nuclear Physics, Physics Education
  - Engineering: Civil Engineering, Mechanical Engineering and Electrical Engineering
  - Computer Science: Software Engineering, Artificial Intelligence, Robotics and Automation
  - Chemistry: Chemical Engineering and Chemistry Education
- There are bout 15 to 20 teachers in each department
- There are about 20 students in each of the majors. Thee are of both genders, and of multiple ethnicities (implied by their names)
- There are course at 100, 200 and 300 levels
- Grades are consistent with students major and year of studies

## Loader Notes

The current JSONL files load table rows only. For query-engine tests we also need loadable set relationship data, starting with current class enrollments.

### Enrollment relationship file

The canonical fixture format for enrollments is one JSONL line per membership edge, using business keys rather than record IDs.

Proposed line shape:

```json
{"relationship":"Enrollment","classCode":"MATH101-01","studentId":"NIP2409002"}
```

Notes:

- The file must include `"relationship":"Enrollment"`.
- The file represents current enrollments only.
- The loader resolves `classCode` and `studentId` to record IDs at load time.

### Reciprocal set population

The STEP2 schema does not describe reciprocal set relationships. If one enrollment edge should populate both `Classes.Enrollment` and `Students.TakesClasses`, that knowledge must be encoded explicitly in the loader.

For each enrollment edge, the loader should:

- add the student to `Classes.Enrollment`
- add the class to `Students.TakesClasses`

The loader should use `record.AddSetMember()` for these updates.

### Duplicate handling

`record.AddSetMember()` is not idempotent. If the member already exists, the lower-level set write returns a duplicate-member error and leaves the set unchanged.

For the enrollment loader, duplicate-member errors should be treated as a tolerated no-op.

This keeps the loader simple for now. We may later make `AddSetMember()` itself idempotent.

We are starting with enrollments. Other set-backed relationships such as completed classes can be handled separately later.
