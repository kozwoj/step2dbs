package server

import (
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
)

// recordRoutes registers all record management endpoints
func recordRoutes(r chi.Router) {
	// Discovery endpoint
	r.Get("/", handleRecordDiscovery)

	// Command endpoints
	r.Post("/add", handleAddRecord)
	r.Post("/update", handleUpdateRecord)
	r.Post("/delete", handleDeleteRecord)

	// Help endpoints
	r.Get("/add/", handleAddRecordHelp)
	r.Get("/update/", handleUpdateRecordHelp)
	r.Get("/delete/", handleDeleteRecordHelp)
}

// handleRecordDiscovery returns information about available record commands
func handleRecordDiscovery(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"description": "Record management commands",
		"available_routes": []string{
			"/step2/record/add",
			"/step2/record/update",
			"/step2/record/delete",
			"/step2/record/get/",
		},
		"help": "Add trailing slash to command endpoints for detailed help (e.g., /step2/record/add/)",
	})
}

// handleAddRecord adds a new record to the database
func handleAddRecord(w http.ResponseWriter, r *http.Request) {
	executeMutatingDMLCommand(w, r, dml.AddNewRecord_DML)
}

// handleAddRecordHelp returns help information for the AddNewRecord command
func handleAddRecordHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "AddNewRecord",
		"description": "Adds a new record to a table in the database",
		"method":      "POST",
		"endpoint":    "/step2/record/add",
		"input": map[string]string{
			"tableName": "string - Name of the table to add the record to",
			"record":    "object - Field name/value pairs for the record",
		},
		"example_request": map[string]interface{}{
			"tableName": "Customers",
			"record": map[string]interface{}{
				"Customer_id":  "ALFKI",
				"Company_name": "Alfreds Futterkiste",
				"Contact_name": "Maria Anders",
				"City":         "Berlin",
			},
		},
		"returns": map[string]interface{}{
			"status":   "string - 'success' or 'error'",
			"errors":   "array of strings - Empty on success, contains error messages on failure",
			"recordID": "number - Internal record ID (0 on error)",
		},
		"example_response_success": map[string]interface{}{
			"status":   "success",
			"errors":   []string{},
			"recordID": 123,
		},
		"example_response_error": map[string]interface{}{
			"status":   "error",
			"errors":   []string{"error message"},
			"recordID": 0,
		},
	})
}

// handleUpdateRecord updates an existing record in the database
func handleUpdateRecord(w http.ResponseWriter, r *http.Request) {
	executeMutatingDMLCommand(w, r, dml.UpdateRecord_DML)
}

// handleUpdateRecordHelp returns help information for the UpdateRecord command
func handleUpdateRecordHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "UpdateRecord",
		"description": "Updates fields in an existing record",
		"method":      "POST",
		"endpoint":    "/step2/record/update",
		"input": map[string]string{
			"tableName": "string - Name of the table containing the record",
			"recordID":  "number - Internal record ID",
			"record":    "object - Field name/value pairs to update (partial update supported)",
		},
		"example_request": map[string]interface{}{
			"tableName": "Customers",
			"recordID":  123,
			"record": map[string]interface{}{
				"City":  "Hamburg",
				"Phone": "+49-040-1234567",
			},
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

// handleDeleteRecord deletes a record from the database
func handleDeleteRecord(w http.ResponseWriter, r *http.Request) {
	executeMutatingDMLCommand(w, r, dml.DeleteRecord_DML)
}

// handleDeleteRecordHelp returns help information for the DeleteRecord command
func handleDeleteRecordHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "DeleteRecord",
		"description": "Deletes a record from the database",
		"method":      "POST",
		"endpoint":    "/step2/record/delete",
		"input": map[string]string{
			"tableName": "string - Name of the table containing the record",
			"recordID":  "number - Internal record ID",
		},
		"example_request": map[string]interface{}{
			"tableName": "Customers",
			"recordID":  123,
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
