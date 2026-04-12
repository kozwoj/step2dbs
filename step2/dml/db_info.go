package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
)

/* gofmt:off
GetSchema_DML retrieves the schema of the currently opened database as a JSON object.

This command is primarily used for discovery and scripting purposes. It returns the complete
database schema that was originally defined in the DDL file and stored in schema.json when
the database was created.

Parameters:
- None (operates on the currently open database)

Returns:
- string: JSON output containing the schema
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Preconditions:
- A database must be currently opened (otherwise returns an error in the JSON response)

Output JSON format (success):
{
	"status": "success",
	"schema": {
		"name": string,
		"tables": [
			{
				"name": string,
				"primaryKey": string,
				"fields": [
					{
						"name": string,
						"type": string,
						"constraints": [string, ...],
						"size": int (optional, for CHAR and STRING types)
					},
					...
				],
				"sets": [
					{
						"name": string,
						"memberTable": string
					},
					...
				]
			},
			...
		]
	}
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", "error message 2", ...]
}
gofmt:on */

func GetSchema_DML() (string, error) {
	// Call db.GetSchema() foundation function
	schema, err := db.GetSchema()
	if err != nil {
		// Database not opened or other critical error
		if err == db.ErrDBNotOpen {
			return buildInfoErrorResponse([]string{"database is not opened"}), nil
		}
		return buildInfoErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"schema": schema,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildInfoErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off
GetTableStats_DML retrieves statistics for one or more tables in the currently opened database.

For each requested table, returns information about:
- allocated_records: Total number of records allocated (including deleted)
- deleted_list_length: Number of deleted/reusable record spaces
- Active records: allocated_records - deleted_list_length
- Dictionary statistics for each STRING field in the table

Parameters:
- input: JSON input of the format {"tables": [string, ...]}

Returns:
- string: JSON output containing table statistics
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tables": ["TableName1", "TableName2", ...]
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"tables": [
		{
			"name": string,
			"allocated_records": int,
			"deleted_list_length": int,
			"dictionaries": [
				{
					"field_name": string,
					"number_of_strings": int
				},
				...
			]
		},
		...
	]
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", "error message 2", ...],
	"tables": [] (empty or partial results if some tables succeeded)
}
gofmt:on */

func GetTableStats_DML(input string) (string, error) {
	// Parse input JSON to extract table names
	var inputData struct {
		Tables []string `json:"tables"`
	}
	err := json.Unmarshal([]byte(input), &inputData)
	if err != nil {
		return buildInfoErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Validate tables array is not empty
	if len(inputData.Tables) == 0 {
		return buildInfoErrorResponse([]string{"tables array cannot be empty"}), nil
	}

	// Call db.GetTableStats() foundation function
	tableStats, errMsgs, err := db.GetTableStats(inputData.Tables)
	if err != nil {
		// Critical error (database not open)
		if err == db.ErrDBNotOpen {
			return buildInfoErrorResponse([]string{"database is not open"}), nil
		}
		return buildInfoErrorResponse([]string{err.Error()}), nil
	}

	// Build response
	status := "success"
	if len(errMsgs) > 0 {
		status = "error"
	}

	response := map[string]interface{}{
		"status": status,
		"errors": errMsgs,
		"tables": tableStats,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildInfoErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for database info commands
func buildInfoErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status": "error",
		"errors": errors,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
