package server

import (
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
)

// dbRoutes registers all database management endpoints
func dbRoutes(r chi.Router) {
	// Discovery endpoint
	r.Get("/", handleDBDiscovery)

	// Command endpoints
	r.Post("/create", handleCreateDB)
	r.Post("/open", handleOpenDB)
	r.Post("/close", handleCloseDB)

	// Help endpoints
	r.Get("/create/", handleCreateDBHelp)
	r.Get("/open/", handleOpenDBHelp)
	r.Get("/close/", handleCloseDBHelp)
}

// handleDBDiscovery returns information about available database commands
func handleDBDiscovery(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"description": "Database management commands",
		"available_routes": []string{
			"/step2/db/create",
			"/step2/db/open",
			"/step2/db/close",
		},
		"help": "Add trailing slash to command endpoints for detailed help (e.g., /step2/db/create/)",
	})
}

// handleCreateDB creates a new STEP2 database
func handleCreateDB(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.CreateDB_DML)
}

// handleCreateDBHelp returns help information for the CreateDB command
func handleCreateDBHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "CreateDB",
		"description": "Creates a new STEP2 database from a DDL schema file",
		"method":      "POST",
		"endpoint":    "/step2/db/create",
		"input": map[string]string{
			"dirPath":    "string - Full path to the directory where the database will be created",
			"schemaPath": "string - Full path to the DDL schema file",
		},
		"example_request": map[string]string{
			"dirPath":    "C:\\databases\\MyDB",
			"schemaPath": "C:\\schemas\\myschema.ddl",
		},
		"returns": map[string]interface{}{
			"status": "string - 'success' or 'error'",
			"errors": "array of strings - Empty on success, contains error messages on failure",
		},
		"example_response_success": map[string]interface{}{
			"status": "success",
			"errors": []string{},
		},
		"example_response_error": map[string]interface{}{
			"status": "error",
			"errors": []string{"error message"},
		},
	})
}

// handleOpenDB opens an existing STEP2 database
func handleOpenDB(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.OpenDB_DML)
}

// handleOpenDBHelp returns help information for the OpenDB command
func handleOpenDBHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "OpenDB",
		"description": "Opens an existing STEP2 database",
		"method":      "POST",
		"endpoint":    "/step2/db/open",
		"input": map[string]string{
			"dirPath": "string - Full path to the directory containing the database",
		},
		"example_request": map[string]string{
			"dirPath": "C:\\databases\\MyDB",
		},
		"returns": map[string]interface{}{
			"status": "string - 'success' or 'error'",
			"errors": "array of strings - Empty on success, contains error messages on failure",
		},
		"example_response_success": map[string]interface{}{
			"status": "success",
			"errors": []string{},
		},
		"example_response_error": map[string]interface{}{
			"status": "error",
			"errors": []string{"error message"},
		},
	})
}

// handleCloseDB closes the currently opened STEP2 database
func handleCloseDB(w http.ResponseWriter, r *http.Request) {
	executeNoParamCommand(w, r, dml.CloseDB_DML)
}

// handleCloseDBHelp returns help information for the CloseDB command
func handleCloseDBHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "CloseDB",
		"description": "Closes the currently opened STEP2 database",
		"method":      "POST",
		"endpoint":    "/step2/db/close",
		"input":       "No parameters required - send empty JSON body: {}",
		"returns": map[string]interface{}{
			"status": "string - 'success' or 'error'",
			"errors": "array of strings - Empty on success, contains error messages on failure",
		},
		"example_response_success": map[string]interface{}{
			"status": "success",
			"errors": []string{},
		},
		"note": "CloseDB is safe to call even if no database is open",
	})
}
