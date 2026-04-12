package server

import (
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
)

// recordGetRoutes registers all record retrieval endpoints
func recordGetRoutes(r chi.Router) {
	// Discovery endpoint
	r.Get("/", handleRecordGetDiscovery)

	// Command endpoints
	r.Post("/byid", handleGetRecordByID)
	r.Post("/id", handleGetRecordID)
	r.Post("/next", handleGetNextRecord)
	r.Post("/bykey", handleGetRecordByKey)
	r.Post("/bystring", handleGetRecordsByString)
	r.Post("/bysubstring", handleGetRecordsBySubstring)

	// Help endpoints
	r.Get("/byid/", handleGetRecordByIDHelp)
	r.Get("/id/", handleGetRecordIDHelp)
	r.Get("/next/", handleGetNextRecordHelp)
	r.Get("/bykey/", handleGetRecordByKeyHelp)
	r.Get("/bystring/", handleGetRecordsByStringHelp)
	r.Get("/bysubstring/", handleGetRecordsBySubstringHelp)
}

// handleRecordGetDiscovery returns information about available record retrieval commands
func handleRecordGetDiscovery(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"description": "Record retrieval commands",
		"available_routes": []string{
			"/step2/record/get/byid",
			"/step2/record/get/id",
			"/step2/record/get/next",
			"/step2/record/get/bykey",
			"/step2/record/get/bystring",
			"/step2/record/get/bysubstring",
		},
		"help": "Add trailing slash to command endpoints for detailed help (e.g., /step2/record/get/byid/)",
	})
}

// handleGetRecordByID retrieves a record by its internal ID
func handleGetRecordByID(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetRecordByID_DML)
}

// handleGetRecordByIDHelp returns help information for the GetRecordByID command
func handleGetRecordByIDHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetRecordByID",
		"description": "Retrieves a record by its internal record ID",
		"method":      "POST",
		"endpoint":    "/step2/record/get/byid",
		"input": map[string]string{
			"tableName": "string - Name of the table",
			"recordID":  "number - Internal record ID",
		},
		"example_request": map[string]interface{}{
			"tableName": "Customers",
			"recordID":  123,
		},
		"returns": map[string]interface{}{
			"status":    "string - 'success' or 'error'",
			"errors":    "array of strings - Empty on success",
			"tableName": "string - Table name",
			"recordID":  "number - Record ID",
			"record":    "object - Record field values",
		},
		"example_response": map[string]interface{}{
			"status":    "success",
			"errors":    []string{},
			"tableName": "Customers",
			"recordID":  123,
			"record": map[string]interface{}{
				"Customer_id":  "ALFKI",
				"Company_name": "Alfreds Futterkiste",
			},
		},
	})
}

// handleGetRecordID retrieves the internal ID of a record by its primary key
func handleGetRecordID(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetRecordID_DML)
}

// handleGetRecordIDHelp returns help information for the GetRecordID command
func handleGetRecordIDHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetRecordID",
		"description": "Retrieves the internal record ID by primary key value",
		"method":      "POST",
		"endpoint":    "/step2/record/get/id",
		"input": map[string]string{
			"tableName": "string - Name of the table",
			"primeKey":  "value - Primary key value (type depends on table definition)",
		},
		"example_request": map[string]interface{}{
			"tableName": "Customers",
			"primeKey":  "ALFKI",
		},
		"returns": map[string]interface{}{
			"status":    "string - 'success' or 'error'",
			"errors":    "array of strings - Empty on success",
			"tableName": "string - Table name",
			"primeKey":  "value - Primary key value",
			"recordID":  "number - Internal record ID",
		},
		"example_response": map[string]interface{}{
			"status":    "success",
			"errors":    []string{},
			"tableName": "Customers",
			"primeKey":  "ALFKI",
			"recordID":  123,
		},
	})
}

// handleGetNextRecord retrieves the next record in sequential order
func handleGetNextRecord(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetNextRecord_DML)
}

// handleGetNextRecordHelp returns help information for the GetNextRecord command
func handleGetNextRecordHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetNextRecord",
		"description": "Retrieves the next record in sequential order after the given record ID",
		"method":      "POST",
		"endpoint":    "/step2/record/get/next",
		"input": map[string]string{
			"tableName":       "string - Name of the table",
			"currentRecordID": "number - Current record ID",
		},
		"example_request": map[string]interface{}{
			"tableName":       "Customers",
			"currentRecordID": 123,
		},
		"returns": map[string]interface{}{
			"status":          "string - 'success' or 'error'",
			"errors":          "array of strings - Empty on success",
			"tableName":       "string - Table name",
			"currentRecordID": "number - Current record ID",
			"nextRecordID":    "number - Next record ID",
			"record":          "object - Next record field values",
		},
		"example_response": map[string]interface{}{
			"status":          "success",
			"errors":          []string{},
			"tableName":       "Customers",
			"currentRecordID": 123,
			"nextRecordID":    124,
			"record": map[string]interface{}{
				"Customer_id":  "ANATR",
				"Company_name": "Ana Trujillo",
			},
		},
	})
}

// handleGetRecordByKey retrieves a record by its primary key (convenience function)
func handleGetRecordByKey(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetRecordByKey_DML)
}

// handleGetRecordByKeyHelp returns help information for the GetRecordByKey command
func handleGetRecordByKeyHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetRecordByKey",
		"description": "Retrieves a record by its primary key value (combines GetRecordID + GetRecordByID)",
		"method":      "POST",
		"endpoint":    "/step2/record/get/bykey",
		"input": map[string]string{
			"tableName": "string - Name of the table",
			"primeKey":  "value - Primary key value (type depends on table definition)",
		},
		"example_request": map[string]interface{}{
			"tableName": "Customers",
			"primeKey":  "ALFKI",
		},
		"returns": map[string]interface{}{
			"status":    "string - 'success' or 'error'",
			"errors":    "array of strings - Empty on success",
			"tableName": "string - Table name",
			"primeKey":  "value - Primary key value",
			"recordID":  "number - Internal record ID",
			"record":    "object - Record field values",
		},
		"example_response": map[string]interface{}{
			"status":    "success",
			"errors":    []string{},
			"tableName": "Customers",
			"primeKey":  "ALFKI",
			"recordID":  123,
			"record": map[string]interface{}{
				"Customer_id":  "ALFKI",
				"Company_name": "Alfreds Futterkiste",
			},
		},
	})
}

// handleGetRecordsByString retrieves all records with a specific STRING field value (exact match)
func handleGetRecordsByString(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetRecordsByString_DML)
}

// handleGetRecordsByStringHelp returns help information for the GetRecordsByString command
func handleGetRecordsByStringHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetRecordsByString",
		"description": "Retrieves all record IDs that have a specific STRING field value (exact match)",
		"method":      "POST",
		"endpoint":    "/step2/record/get/bystring",
		"input": map[string]string{
			"tableName":     "string - Name of the table",
			"propertyName":  "string - Name of the STRING field to search",
			"propertyValue": "string - Exact value to match",
		},
		"example_request": map[string]interface{}{
			"tableName":     "Customers",
			"propertyName":  "City",
			"propertyValue": "Berlin",
		},
		"returns": map[string]interface{}{
			"status":    "string - 'success' or 'error'",
			"errors":    "array of strings - Empty on success",
			"recordIDs": "array of numbers - Record IDs matching the search",
		},
		"example_response": map[string]interface{}{
			"status":    "success",
			"errors":    []string{},
			"recordIDs": []int{123, 456, 789},
		},
	})
}

// handleGetRecordsBySubstring retrieves all records with a STRING field value starting with the given substring
func handleGetRecordsBySubstring(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetRecordsBySubstring_DML)
}

// handleGetRecordsBySubstringHelp returns help information for the GetRecordsBySubstring command
func handleGetRecordsBySubstringHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetRecordsBySubstring",
		"description": "Retrieves all record IDs that have a STRING field value starting with the given substring (prefix search)",
		"method":      "POST",
		"endpoint":    "/step2/record/get/bysubstring",
		"input": map[string]string{
			"tableName":    "string - Name of the table",
			"propertyName": "string - Name of the STRING field to search",
			"substring":    "string - Prefix substring to match",
		},
		"example_request": map[string]interface{}{
			"tableName":    "Customers",
			"propertyName": "Company_name",
			"substring":    "Alf",
		},
		"returns": map[string]interface{}{
			"status":    "string - 'success' or 'error'",
			"errors":    "array of strings - Empty on success",
			"recordIDs": "array of numbers - Record IDs matching the search",
		},
		"example_response": map[string]interface{}{
			"status":    "success",
			"errors":    []string{},
			"recordIDs": []int{123, 456},
		},
		"note": "Prefix search uses the dictionary index for efficient lookups",
	})
}
