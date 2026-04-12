# REST Interface Architecture

**Date**: March 2, 2026
**Purpose**: Document the REST interface pattern used in STEP (v1) for replication in STEP2

---

## Overview

The STEP REST interface exposes DML commands through HTTP endpoints using a standardized pattern with discovery and help endpoints.

---

## Technology Stack

- **HTTP Router**: `chi` (github.com/go-chi/chi/v5)
- **Middleware**: Logger, Recoverer, RequestID (all from chi)
- **Protocol**: HTTP/REST with JSON request/response
- **Port**: should be passed during server invocation as a command line parameter

---

## Package Structure

```
server/
├── server.go              # Main server setup + helper functions
├── db_routes.go           # Database management endpoints (/db)
├── db_info_ routes.go     # Database info endpoints (/db/info)
├── record_routes.go       # Record operations endpoints (/record)
├── record_get_routes.go   # Record get operations endpoints (/record/get)
└── set_routes.go          # Set manipulation endpoints (/set)
```

**Organization Pattern**: One file per domain/route group/subgroup

---

## Server Initialization (server.go)

```go
func Start(port int) error {
    r := chi.NewRouter()

    // Middleware stack
    r.Use(middleware.Logger)
    r.Use(middleware.Recoverer)
    r.Use(middleware.RequestID)

    // Route groups
    r.Route("/step", func(r chi.Router) {
        r.Get("/", handleRootDiscovery)
        r.Route("/db", dbRoutes)
        r.Route("db/info", dbInfoRouts)
        r.Route("/record", recordRoutes)
        r.Route("/record/get", recordGetRoutes)
        r.Route("/set", setRoutes)
    })

    return http.ListenAndServe(fmt.Sprintf(":%d", port), r)
}
```

**Base Path**: `/step` for all routes

---

## Helper Functions

### 1. executeDMLCommand (for commands with parameters)

**Signature**: Commands that take JSON string input and return JSON string
**DML Function Pattern**: `func(string) string`

```go
func executeDMLCommand(w http.ResponseWriter, r *http.Request,
                       commandFunc func(string) string) {
    // 1. Decode JSON from request body into map
    var input map[string]interface{}
    json.NewDecoder(r.Body).Decode(&input)

    // 2. Marshal map back to JSON string
    inputJSON, _ := json.Marshal(input)

    // 3. Call DML command with JSON string
    resultJSON := commandFunc(string(inputJSON))

    // 4. Unmarshal result and send as JSON response
    var result map[string]interface{}
    json.Unmarshal([]byte(resultJSON), &result)
    sendJSON(w, result)
}
```

**Usage Example**:
```go
func handleAddNewRecord(w http.ResponseWriter, r *http.Request) {
    executeDMLCommand(w, r, dml.AddNewRecord_DML)
}
```

### 2. executeNoParamCommand (for parameterless commands)

**Signature**: Commands that take no parameters
**DML Function Pattern**: `func() string`

```go
func executeNoParamCommand(w http.ResponseWriter, r *http.Request,
                           commandFunc func() string) {
    resultJSON := commandFunc()
    w.Header().Set("Content-Type", "application/json")
    w.Write([]byte(resultJSON))
}
```

**Usage Example**:
```go
func handleCloseDB(w http.ResponseWriter, r *http.Request) {
    executeNoParamCommand(w, r, dml.CloseDB)
}
```

### 3. Response Helpers

```go
// Send JSON response
func sendJSON(w http.ResponseWriter, data interface{}) {
    w.Header().Set("Content-Type", "application/json")
    json.NewEncoder(w).Encode(data)
}

// Send error response
func sendError(w http.ResponseWriter, statusCode int, message string) {
    w.Header().Set("Content-Type", "application/json")
    w.WriteHeader(statusCode)
    json.NewEncoder(w).Encode(map[string]string{"error": message})
}
```

---

## Route Organization Pattern

### Standard Route File Structure

Each route file follows this pattern:

```go
package server

import (
    "net/http"
    "step/dml"
    "github.com/go-chi/chi/v5"
)

// Main route registration function
func {domain}Routes(r chi.Router) {
    r.Route("/{subpath}", func(r chi.Router) {
        r.Get("/", handle{Domain}Discovery)
        r.Post("/{command}", handle{Command})
        r.Get("/{command}/", handle{Command}Help)
    })
}

// Discovery endpoint - lists available commands
func handle{Domain}Discovery(w http.ResponseWriter, r *http.Request) {
    sendJSON(w, map[string]interface{}{
        "description": "...",
        "available_routes": []string{...},
    })
}

// Command handler - thin wrapper around DML function
func handle{Command}(w http.ResponseWriter, r *http.Request) {
    executeDMLCommand(w, r, dml.{CommandFunc})
}

// Help endpoint - detailed command documentation
func handle{Command}Help(w http.ResponseWriter, r *http.Request) {
    sendJSON(w, map[string]interface{}{
        "command": "...",
        "description": "...",
        "input": map[string]interface{}{...},
        "example_request": map[string]string{...},
        "returns": map[string]string{...},
    })
}
```

---

## URL Structure

### Three-Tier Pattern

1. **Discovery** (GET): `/step/{domain}/`
   → Lists all available commands in the domain

2. **Command** (POST): `/step/{domain}/{command}`
   → Executes the command with JSON body

3. **Help** (GET): `/step/{domain}/{command}/`
   → Returns detailed documentation for the command

### Examples

```
GET  /step/db/                    # List database commands
POST /step/db/open                # Execute OpenDB command
GET  /step/db/open/               # Get OpenDB documentation

GET  /step/record/add/            # List record add command
POST /step/record/get/bykey       # Execute GetRrcordByKey
GET  /step/record/get/bykey/      # Get documentation of GetREcordByKey
```

---

## Example: Database Routes (db_routes.go)

```go
func dbRoutes(r chi.Router) {
    // Discovery
    r.Get("/", handleDBDiscovery)

    // Commands
    r.Post("/create", handleCreateDB)
    r.Post("/open", handleOpenDB)
    r.Post("/close", handleCloseDB)

    // Help endpoints
    r.Get("/create/", handleCreateDBHelp)
    r.Get("/open/", handleOpenDBHelp)
    r.Get("/close/", handleCloseDBHelp)

}

func handleDBDiscovery(w http.ResponseWriter, r *http.Request) {
    sendJSON(w, map[string]interface{}{
        "description": "Database management commands",
        "available_routes": []string{
            "/step/db/create",
            "/step/db/open",
            "/step/db/close",
        },
        "help": "Add trailing slash for help (e.g., /step/db/open/)",
    })
}

func handleCreateDB(w http.ResponseWriter, r *http.Request) {
    executeDMLCommand(w, r, dml.CreateDB)
}

func handleOpenDBHelp(w http.ResponseWriter, r *http.Request) {
    sendJSON(w, map[string]interface{}{
        "command": "OpenDB",
        "description": "Opens an existing STEP database",
        "method": "POST",
        "endpoint": "/step/db/open",
        "input": map[string]interface{}{
            "dbName": "string - Name of the database to open",
        },
        "example_request": map[string]string{
            "dbName": "myDatabase",
        },
        "returns": map[string]string{
            "status": "success or error message",
        },
    })
}
```
---

## Main Entry Point (main.go)

the entry point is a main that is called from a command line and supports two execution paths
1. `step2 server -port 8080` - this starts the STEP2 REST server
2. `step2 schema -path c:\step2\College [-storage]` - this parses and analyzes a DB schema. if the optional argument `storage` is given, the command also prints the DB directory structure


```go
func main() {
    // parse the command line and determine the mode and parameters
    // path, portNo, storage

    switch mode {
    case "schema":
        // call a function similar to db.CreateDBDefinition that generates console readable output
    case "server":
        startRESTServer()
    default:
        fmt.Printf("Unknown mode: %s\n", mode)
        printUsage()
    }
}

func startRESTServer() {
    port := portNo
    if err := server.Start(port); err != nil {
        log.Fatalf("Failed to start server: %v", err)
    }
}
```

**Usage**:
```bash
./step2 server -port 8080                               # Start REST server
./step2 schema -path c:\step2\College -storage          # parses and analyzed the DB schema
```

---

## Request/Response Flow

### Typical Request Flow

1. **Client** sends POST request with JSON body
2. **Chi Router** matches URL and calls handler
3. **Handler** calls `executeDMLCommand(w, r, dml.Function)`
4. **Helper** decodes JSON → calls DML function → encodes response
5. **Client** receives JSON response

### Example Request/Response

**Request**:
```http
POST /step/db/open HTTP/1.1
Content-Type: application/json

{
    "dbName": "testDB"
}
```

**Flow**:
```
handleOpenDB()
  → executeDMLCommand(w, r, dml.OpenDB)
    → dml.OpenDB(`{"dbName":"testDB"}`)
      → returns `{"status":"success"}`
        → response sent to client
```
---

## Key Design Principles

1. **Thin Handlers**: Route handlers are minimal wrappers around DML functions
2. **DML Independence**: DML layer is unaware of HTTP - only deals with JSON strings
3. **Consistent Pattern**: All commands follow same URL/method conventions
4. **Self-Documenting**: Discovery and help endpoints make API explorable
5. **Separation of Concerns**: Each domain has its own route file
6. **Middleware Stack**: Logging, recovery, and request tracking built-in

---

## Advantages of This Pattern

✅ **Simplicity**: Handlers are 1-3 lines of code
✅ **Consistency**: Same pattern for all commands
✅ **Discoverability**: GET endpoints for exploration
✅ **Documentation**: Built-in help system
✅ **Maintainability**: Easy to add new commands
✅ **Testability**: DML layer can be tested independently

---

## Application to STEP2

### Proposed Mapping

For STEP2, we would create similar structure:

```
step2/
├── server/
│    ├── server.go              # Main server setup + helper functions
│    ├── db_routes.go           # Database management endpoints (/db)
│    ├── db_info_ routes.go     # Database info endpoints (/db/info)
│    ├── db_info_ routes.go     # Database info endpoints (/db/info)
│    ├── db_info_ routes.go     # Database info endpoints (/db/info)
│    ├── db_info_ routes.go     # Database info endpoints (/db/info)
│    ├── record_routes.go       # Record operations endpoints (/record)
│    ├── record_get_routes.go   # Record get operations endpoints (/record/get)
│    └── set_routes.go          # Set manipulation endpoints (/set)
└── main.go
```

### STEP2 Commands to Expose

Based on current DML implementation:

**Database Management** (`/step2/db/`):
- POST `db/create` → `dml.CreateDB_DML`
- POST `db/open` → `dml.OpenDB_DML`
- POST `db/close` → `dml.CloseDB_DML`

**Record Operations** (`/step2/record/`):
- POST `record/add` → `dml.AddNewRecord_DML`
- POST `record/get/next` → `dml.GetNextRecord_DML`
- POST `record/get/bykey` → `dml.GetRecordByKey_DML`
- POST `record/get/bystring` → `dml.GetRecordsByString_DML`
- POST `record/get/bysubstring` → `dml.GetRecordsBySubstring_DML`

---

## Implementation Plan for STEP2

### Available DML Functions to Expose

**Database Management** (`/step2/db/`):
- `CreateDB_DML(input string) (string, error)`
- `OpenDB_DML(input string) (string, error)`
- `CloseDB_DML() (string, error)` ← No parameters

**Record Operations** (`/step2/record/`):
- `AddNewRecord_DML(inputRecord string) (string, error)`
- `UpdateRecord_DML(inputRecord string) (string, error)`
- `DeleteRecord_DML(inputRecord string) (string, error)`

**Record Get Operations** (`/step2/record/get/`):
- `GetRecordByID_DML(inputJSON string) (string, error)`
- `GetRecordID_DML(inputJSON string) (string, error)`
- `GetNextRecord_DML(inputJSON string) (string, error)`
- `GetRecordByKey_DML(inputJSON string) (string, error)`
- `GetRecordsByString_DML(inputJSON string) (string, error)`
- `GetRecordsBySubstring_DML(inputJSON string) (string, error)`

**Set Operations** (`/step2/set/`):
- `AddSetMember_DML(inputJSON string) (string, error)`
- `GetSetMembers_DML(inputJSON string) (string, error)`
- `RemoveSetMember_DML(inputJSON string) (string, error)`

### Implementation Sequence

**Step 1: Add chi router dependency**
```bash
go get github.com/go-chi/chi/v5
```

**Step 2: Implement server.go** (Main server infrastructure)
- `Start(port int) error` - Server initialization with middleware (Logger, Recoverer, RequestID)
- `executeDMLCommand(w, r, func(string)(string, error))` - Helper for DML commands with parameters
- `executeNoParamCommand(w, r, func()(string, error))` - Helper for CloseDB (no parameters)
- `sendJSON(w, data)` - JSON response helper
- `sendError(w, code, message)` - Error response helper
- `handleRootDiscovery()` - Root endpoint listing all domains

**Step 3: Implement db_routes.go** (Database management routes)
- `dbRoutes(r chi.Router)` - Route registration
- Discovery endpoint: GET `/step2/db/`
- Command endpoints: POST `/step2/db/create`, `/step2/db/open`, `/step2/db/close`
- Help endpoints: GET `/step2/db/create/`, `/step2/db/open/`, `/step2/db/close/`

**Step 4: Implement record_routes.go** (Record operations routes)
- `recordRoutes(r chi.Router)` - Route registration
- Discovery endpoint: GET `/step2/record/`
- Command endpoints: POST `/step2/record/add`, `/step2/record/update`, `/step2/record/delete`
- Help endpoints for each command

**Step 5: Implement record_get_routes.go** (Record retrieval routes)
- `recordGetRoutes(r chi.Router)` - Route registration
- Discovery endpoint: GET `/step2/record/get/`
- Command endpoints: POST `/step2/record/get/byid`, `/step2/record/get/id`, `/step2/record/get/next`, `/step2/record/get/bykey`, `/step2/record/get/bystring`, `/step2/record/get/bysubstring`
- Help endpoints for each command

**Step 6: Implement set_routes.go** (Set operations routes)
- `setRoutes(r chi.Router)` - Route registration
- Discovery endpoint: GET `/step2/set/`
- Command endpoints: POST `/step2/set/addmember`, `/step2/set/getmembers`, `/step2/set/removemember`
- Help endpoints for each command

**Step 7: Update main.go** (Command-line interface with two modes)
- Parse command-line arguments for:
  - `step2 server -port 8080` - Start REST server
  - `step2 schema -path <file> [-storage]` - Parse and analyze DDL schema
- Route to `server.Start(port)` or `cli.AnalyzeSchema(path, storage)`
- Implement `printUsage()` for help text

**Step 8: Create PowerShell test script** (scripts/step2.ps1)
- Similar to STEP v1's step.ps1
- Functions for all endpoints:
  - Server: `Start-SRV`, `Stop-SRV`, `Get-SRVStatus`
  - Database: `Create-DB`, `Open-DB`, `Close-DB`
  - Records: `Add-Record`, `Update-Record`, `Delete-Record`, `Get-Record*`
  - Sets: `Add-SetMember`, `Get-SetMembers`, `Remove-SetMember`
- Use `Invoke-RestMethod` with JSON bodies
- Color-coded output (Green/Red/Yellow)

**Step 9: Manual testing**
- Start server: `.\step2.exe server -port 8080`
- Import PowerShell module: `. .\scripts\step2.ps1`
- Test database operations: Create, Open, Close
- Test record operations: Add, Get, Update, Delete
- Test set operations: AddMember, GetMembers, RemoveMember
- Verify error handling and help endpoints

---

## Notes & Questions

<!-- Add your comments and questions here -->

