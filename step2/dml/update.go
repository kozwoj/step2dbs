package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

/* gofmt:off

UpdateRecord_DML is a DML wrapper for the UpdateRecord foundation function.
Parameters:
- inputRecord: JSON input of the format {"tableName": string, "recordID": uint32, "record": { partial_record_object }}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"recordID": 123,
	"record": {
		"fieldName1": newValue1,
		"fieldName2": newValue2,
		...
	}
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

func UpdateRecord_DML(inputRecord string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildUpdateErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var recordMap map[string]interface{}
	err := json.Unmarshal([]byte(inputRecord), &recordMap)
	if err != nil {
		return buildUpdateErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract tableName
	tableName, ok := recordMap["tableName"].(string)
	if !ok {
		return buildUpdateErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract recordID
	recordIDFloat, ok := recordMap["recordID"].(float64)
	if !ok {
		return buildUpdateErrorResponse([]string{"recordID is missing or not a number in the input JSON"}), nil
	}
	recordID := uint32(recordIDFloat)

	// Extract record fields to update
	recordFields, ok := recordMap["record"].(map[string]interface{})
	if !ok {
		return buildUpdateErrorResponse([]string{"record fields are missing or not an object in the input JSON"}), nil
	}

	// Call foundation function
	err = record.UpdateRecord(tableName, recordID, recordFields, dbDef)
	if err != nil {
		return buildUpdateErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildUpdateErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for update
func buildUpdateErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status": "error",
		"errors": errors,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
