package server

import (
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
)

// dbInfoRoutes registers all database information endpoints
func dbInfoRoutes(r chi.Router) {
	// Discovery endpoint
	r.Get("/", handleDBInfoDiscovery)

	// Command endpoints
	r.Get("/schema", handleGetSchema)
	r.Post("/tables", handleGetTableStats)

	// Help endpoints
	r.Get("/schema/", handleGetSchemaHelp)
	r.Get("/tables/", handleGetTableStatsHelp)
}

// handleDBInfoDiscovery returns information about available database info commands
func handleDBInfoDiscovery(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"description": "Database information commands",
		"available_routes": []string{
			"/step2/db/info/schema",
			"/step2/db/info/tables",
		},
		"help": "Add trailing slash to command endpoints for detailed help (e.g., /step2/db/info/schema/)",
	})
}

// handleGetSchema retrieves the schema of the currently opened database
func handleGetSchema(w http.ResponseWriter, r *http.Request) {
	executeNoParamCommand(w, r, dml.GetSchema_DML)
}

// handleGetSchemaHelp returns help information for the GetSchema command
func handleGetSchemaHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetSchema",
		"description": "Retrieves the complete schema of the currently opened database",
		"method":      "GET",
		"endpoint":    "/step2/db/info/schema",
		"input":       "No parameters required",
		"returns": map[string]interface{}{
			"status": "string - 'success' or 'error'",
			"errors": "array of strings - Empty on success, contains error messages on failure",
			"schema": "object - Complete database schema including tables, fields, constraints, and sets",
		},
		"example_response_success": map[string]interface{}{
			"status": "success",
			"errors": []string{},
			"schema": map[string]interface{}{
				"name": "College",
				"tables": []map[string]interface{}{
					{
						"name":        "Students",
						"primary_key": "Student_id",
						"fields": []map[string]interface{}{
							{"name": "Student_id", "type": "STRING", "length": 10},
							{"name": "First_name", "type": "STRING", "length": 20},
							{"name": "Last_name", "type": "STRING", "length": 30},
						},
					},
				},
			},
		},
		"note": "Database must be opened before calling this command",
	})
}

// handleGetTableStats retrieves statistics for specified tables
func handleGetTableStats(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetTableStats_DML)
}

// handleGetTableStatsHelp returns help information for the GetTableStats command
func handleGetTableStatsHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetTableStats",
		"description": "Retrieves statistics for one or more tables in the currently opened database",
		"method":      "POST",
		"endpoint":    "/step2/db/info/tables",
		"input": map[string]string{
			"tables": "array of strings - Names of tables to retrieve statistics for",
		},
		"example_request": map[string]interface{}{
			"tables": []string{"Students", "Teachers", "Courses"},
		},
		"returns": map[string]interface{}{
			"status": "string - 'success' or 'error'",
			"errors": "array of strings - May contain warnings for individual tables that failed",
			"tables": "array of objects - Statistics for each successfully processed table",
		},
		"example_response_success": map[string]interface{}{
			"status": "success",
			"errors": []string{},
			"tables": []map[string]interface{}{
				{
					"name":                "Students",
					"allocated_records":   350,
					"deleted_list_length": 13,
					"dictionaries": []map[string]interface{}{
						{"field_name": "First_name", "number_of_strings": 120},
						{"field_name": "Last_name", "number_of_strings": 215},
					},
				},
			},
		},
		"note": "Database must be opened before calling this command. The command returns partial success if some tables succeed.",
	})
}
