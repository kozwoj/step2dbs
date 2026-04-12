package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
)

/* gofmt:off

CreateDB_DML is a DML wrapper for the CreateDB foundation function.

Parameters:
- input: JSON input of the format {"dirPath": string, "schemaPath": string}

Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"dirPath": "/path/to/database/directory",
	"schemaPath": "/path/to/schema.ddl"
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

func CreateDB_DML(input string) (string, error) {
	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(input), &inputMap)
	if err != nil {
		return buildDBErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract dirPath
	dirPath, ok := inputMap["dirPath"].(string)
	if !ok {
		return buildDBErrorResponse([]string{"dirPath is missing or not a string in the input JSON"}), nil
	}

	// Extract schemaPath
	schemaPath, ok := inputMap["schemaPath"].(string)
	if !ok {
		return buildDBErrorResponse([]string{"schemaPath is missing or not a string in the input JSON"}), nil
	}

	// Call foundation function
	err = db.CreateDB(dirPath, schemaPath)
	if err != nil {
		return buildDBErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildDBErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

OpenDB_DML is a DML wrapper for the OpenDB foundation function.

Parameters:
- input: JSON input of the format {"dirPath": string}

Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"dirPath": "/path/to/database/directory"
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

func OpenDB_DML(input string) (string, error) {
	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(input), &inputMap)
	if err != nil {
		return buildDBErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract dirPath
	dirPath, ok := inputMap["dirPath"].(string)
	if !ok {
		return buildDBErrorResponse([]string{"dirPath is missing or not a string in the input JSON"}), nil
	}

	// Call foundation function
	err = db.OpenDB(dirPath)
	if err != nil {
		return buildDBErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildDBErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

CloseDB_DML is a DML wrapper for the CloseDB foundation function.

Parameters:
- None

Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

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

Note: CloseDB is safe to call even if no database is open. It will not return an error
in that case.

gofmt:on */

func CloseDB_DML() (string, error) {
	// Call foundation function (no parameters, no return value)
	db.CloseDB()

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildDBErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for database commands
func buildDBErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status": "error",
		"errors": errors,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
