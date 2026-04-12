package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

/* gofmt:off

AddNewRecord_DML is a DML wrapper for the AddNewRecord foundation function.
Parameters:
- inputRecord: JSON input of the format {"tableName": string, "record": { field_name: value, ... }}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string], "recordID": uint32}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"record": {
		"fieldName1": value1,
		"fieldName2": value2,
		...
	}
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"recordID": 123
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", "error message 2", ...],
	"recordID": 0
}

gofmt:on */

func AddNewRecord_DML(inputRecord string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildErrorResponse([]string{"database not opened"}, 0), nil
	}

	// Parse JSON input into a map
	var recordMap map[string]interface{}
	err := json.Unmarshal([]byte(inputRecord), &recordMap)
	if err != nil {
		return buildErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}, 0), nil
	}

	// Extract tableName
	tableName, ok := recordMap["tableName"].(string)
	if !ok {
		return buildErrorResponse([]string{"tableName is missing or not a string in the input JSON"}, 0), nil
	}

	// Extract record fields
	recordFields, ok := recordMap["record"].(map[string]interface{})
	if !ok {
		return buildErrorResponse([]string{"record fields are missing or not an object in the input JSON"}, 0), nil
	}

	// Call foundation function
	recordID, err := record.AddNewRecord(tableName, recordFields, dbDef)
	if err != nil {
		return buildErrorResponse([]string{err.Error()}, 0), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":   "success",
		"errors":   []string{},
		"recordID": recordID,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}, 0), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON
func buildErrorResponse(errors []string, recordID uint32) string {
	response := map[string]interface{}{
		"status":   "error",
		"errors":   errors,
		"recordID": recordID,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
