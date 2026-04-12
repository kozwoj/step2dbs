package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

/* gofmt:off

DeleteRecord_DML is a DML wrapper for the DeleteRecord foundation function.
Parameters:
- inputRecord: JSON input of the format {"tableName": string, "recordID": uint32}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"recordID": 123
}

Output JSON format (success):
{
	"status": "success",
	"errors": []
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", "error message 2", ...]
}

gofmt:on */

func DeleteRecord_DML(inputRecord string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildDeleteErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var recordMap map[string]interface{}
	err := json.Unmarshal([]byte(inputRecord), &recordMap)
	if err != nil {
		return buildDeleteErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract tableName
	tableName, ok := recordMap["tableName"].(string)
	if !ok {
		return buildDeleteErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract recordID
	recordIDFloat, ok := recordMap["recordID"].(float64)
	if !ok {
		return buildDeleteErrorResponse([]string{"recordID is missing or not a number in the input JSON"}), nil
	}
	recordID := uint32(recordIDFloat)

	// Call foundation function
	err = record.DeleteRecord(tableName, recordID, dbDef)
	if err != nil {
		return buildDeleteErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildDeleteErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for delete
func buildDeleteErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status": "error",
		"errors": errors,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
