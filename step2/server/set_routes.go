package server

import (
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
)

// setRoutes registers all set manipulation endpoints
func setRoutes(r chi.Router) {
	// Discovery endpoint
	r.Get("/", handleSetDiscovery)

	// Command endpoints
	r.Post("/addmember", handleAddSetMember)
	r.Post("/getmembers", handleGetSetMembers)
	r.Post("/removemember", handleRemoveSetMember)

	// Help endpoints
	r.Get("/addmember/", handleAddSetMemberHelp)
	r.Get("/getmembers/", handleGetSetMembersHelp)
	r.Get("/removemember/", handleRemoveSetMemberHelp)
}

// handleSetDiscovery returns information about available set commands
func handleSetDiscovery(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"description": "Set manipulation commands",
		"available_routes": []string{
			"/step2/set/addmember",
			"/step2/set/getmembers",
			"/step2/set/removemember",
		},
		"help": "Add trailing slash to command endpoints for detailed help (e.g., /step2/set/addmember/)",
	})
}

// handleAddSetMember adds a member record to an owner's set
func handleAddSetMember(w http.ResponseWriter, r *http.Request) {
	executeMutatingDMLCommand(w, r, dml.AddSetMember_DML)
}

// handleAddSetMemberHelp returns help information for the AddSetMember command
func handleAddSetMemberHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "AddSetMember",
		"description": "Adds a member record to an owner record's set",
		"method":      "POST",
		"endpoint":    "/step2/set/addmember",
		"input": map[string]string{
			"ownerTableName": "string - Name of the owner table",
			"ownerRecordID":  "number - Internal record ID of the owner",
			"setName":        "string - Name of the set",
			"memberRecordID": "number - Internal record ID of the member to add",
		},
		"example_request": map[string]interface{}{
			"ownerTableName": "Customers",
			"ownerRecordID":  123,
			"setName":        "Orders",
			"memberRecordID": 456,
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

// handleGetSetMembers retrieves all members of an owner's set
func handleGetSetMembers(w http.ResponseWriter, r *http.Request) {
	executeDMLCommand(w, r, dml.GetSetMembers_DML)
}

// handleGetSetMembersHelp returns help information for the GetSetMembers command
func handleGetSetMembersHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "GetSetMembers",
		"description": "Retrieves all member record IDs in an owner record's set",
		"method":      "POST",
		"endpoint":    "/step2/set/getmembers",
		"input": map[string]string{
			"ownerTableName": "string - Name of the owner table",
			"ownerRecordID":  "number - Internal record ID of the owner",
			"setName":        "string - Name of the set",
		},
		"example_request": map[string]interface{}{
			"ownerTableName": "Customers",
			"ownerRecordID":  123,
			"setName":        "Orders",
		},
		"returns": map[string]interface{}{
			"status":          "string - 'success' or 'error'",
			"errors":          "array of strings - Empty on success",
			"ownerTableName":  "string - Owner table name",
			"ownerRecordID":   "number - Owner record ID",
			"setName":         "string - Set name",
			"memberTableName": "string - Member table name",
			"members":         "array of numbers - Member record IDs",
		},
		"example_response": map[string]interface{}{
			"status":          "success",
			"errors":          []string{},
			"ownerTableName":  "Customers",
			"ownerRecordID":   123,
			"setName":         "Orders",
			"memberTableName": "Orders",
			"members":         []int{456, 789, 1011},
		},
	})
}

// handleRemoveSetMember removes a member record from an owner's set
func handleRemoveSetMember(w http.ResponseWriter, r *http.Request) {
	executeMutatingDMLCommand(w, r, dml.RemoveSetMember_DML)
}

// handleRemoveSetMemberHelp returns help information for the RemoveSetMember command
func handleRemoveSetMemberHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "RemoveSetMember",
		"description": "Removes a member record from an owner record's set",
		"method":      "POST",
		"endpoint":    "/step2/set/removemember",
		"input": map[string]string{
			"ownerTableName": "string - Name of the owner table",
			"ownerRecordID":  "number - Internal record ID of the owner",
			"setName":        "string - Name of the set",
			"memberRecordID": "number - Internal record ID of the member to remove",
		},
		"example_request": map[string]interface{}{
			"ownerTableName": "Customers",
			"ownerRecordID":  123,
			"setName":        "Orders",
			"memberRecordID": 456,
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
