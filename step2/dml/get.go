package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

/* gofmt:off

GetRecordByID_DML is a DML wrapper for the GetRecordByID foundation function.
Parameters:
- inputJSON: JSON input of the format {"tableName": string, "recordID": uint32}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string], "record": {...}}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"recordID": 123
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"tableName": "TableName",
	"recordID": 123,
	"record": {
		"fieldName1": value1,
		"fieldName2": value2,
		...
	}
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...],
	"tableName": "",
	"recordID": 0,
	"record": null
}

gofmt:on */

func GetRecordByID_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetRecordErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildGetRecordErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract tableName
	tableName, ok := inputMap["tableName"].(string)
	if !ok {
		return buildGetRecordErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract recordID
	recordIDFloat, ok := inputMap["recordID"].(float64)
	if !ok {
		return buildGetRecordErrorResponse([]string{"recordID is missing or not a number in the input JSON"}), nil
	}
	recordID := uint32(recordIDFloat)

	// Call foundation function
	recordData, err := record.GetRecordByID(tableName, recordID, dbDef)
	if err != nil {
		return buildGetRecordErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":    "success",
		"errors":    []string{},
		"tableName": tableName,
		"recordID":  recordID,
		"record":    recordData,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetRecordErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

GetRecordID_DML is a DML wrapper for the GetRecordID foundation function.
Parameters:
- inputJSON: JSON input of the format {"tableName": string, "primeKey": value}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string], "recordID": uint32}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"primeKey": <value>  // Type depends on primary key field type
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"tableName": "TableName",
	"primeKey": <value>,
	"recordID": 123
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...],
	"tableName": "",
	"primeKey": null,
	"recordID": 0
}

gofmt:on */

func GetRecordID_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetRecordIDErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildGetRecordIDErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract tableName
	tableName, ok := inputMap["tableName"].(string)
	if !ok {
		return buildGetRecordIDErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract primeKey
	primeKey, ok := inputMap["primeKey"]
	if !ok {
		return buildGetRecordIDErrorResponse([]string{"primeKey is missing in the input JSON"}), nil
	}

	// Get table definition to determine primary key type
	tableIndex, tableExists := dbDef.TableIndex[tableName]
	if !tableExists {
		return buildGetRecordIDErrorResponse([]string{fmt.Sprintf("table '%s' not found", tableName)}), nil
	}
	tableDesc := dbDef.Tables[tableIndex]

	// Check if table has a primary key
	if tableDesc.Key == -1 {
		return buildGetRecordIDErrorResponse([]string{fmt.Sprintf("table '%s' does not have a primary key", tableName)}), nil
	}

	// Get primary key field description
	pkFieldDesc := tableDesc.RecordLayout.Fields[tableDesc.Key]

	// Convert primeKey from JSON type (float64 for numbers) to appropriate Go type
	var convertedPrimeKey interface{}
	switch pkFieldDesc.Type {
	case db.SMALLINT:
		floatValue, ok := primeKey.(float64)
		if !ok {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a number for SMALLINT primary key"}), nil
		}
		if floatValue != float64(int16(floatValue)) {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a valid int16 for SMALLINT primary key"}), nil
		}
		convertedPrimeKey = int16(floatValue)
	case db.INT:
		floatValue, ok := primeKey.(float64)
		if !ok {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a number for INT primary key"}), nil
		}
		if floatValue != float64(int32(floatValue)) {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a valid int32 for INT primary key"}), nil
		}
		convertedPrimeKey = int32(floatValue)
	case db.BIGINT:
		floatValue, ok := primeKey.(float64)
		if !ok {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a number for BIGINT primary key"}), nil
		}
		if floatValue != float64(int64(floatValue)) {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a valid int64 for BIGINT primary key"}), nil
		}
		convertedPrimeKey = int64(floatValue)
	case db.CHAR:
		strValue, ok := primeKey.(string)
		if !ok {
			return buildGetRecordIDErrorResponse([]string{"primeKey must be a string for CHAR primary key"}), nil
		}
		if len(strValue) != pkFieldDesc.Size {
			return buildGetRecordIDErrorResponse([]string{fmt.Sprintf("primeKey must be exactly %d characters for CHAR[%d] primary key", pkFieldDesc.Size, pkFieldDesc.Size)}), nil
		}
		convertedPrimeKey = strValue
	default:
		return buildGetRecordIDErrorResponse([]string{fmt.Sprintf("unsupported primary key type: %d", pkFieldDesc.Type)}), nil
	}

	// Call foundation function with converted primeKey
	recordID, err := record.GetRecordID(tableName, convertedPrimeKey, dbDef)
	if err != nil {
		return buildGetRecordIDErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":    "success",
		"errors":    []string{},
		"tableName": tableName,
		"primeKey":  primeKey,
		"recordID":  recordID,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetRecordIDErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

GetNextRecord_DML is a DML wrapper for the GetNextRecord foundation function.
Parameters:
- inputJSON: JSON input of the format {"tableName": string, "currentRecordID": uint32}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string], ...}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"currentRecordID": 123
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"tableName": "TableName",
	"currentRecordID": 123,
	"nextRecordID": 124,
	"record": {
		"fieldName1": value1,
		"fieldName2": value2,
		...
	}
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...],
	"tableName": "",
	"currentRecordID": 0,
	"nextRecordID": 0,
	"record": null
}

gofmt:on */

func GetNextRecord_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetNextRecordErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildGetNextRecordErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract tableName
	tableName, ok := inputMap["tableName"].(string)
	if !ok {
		return buildGetNextRecordErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract currentRecordID
	currentRecordIDFloat, ok := inputMap["currentRecordID"].(float64)
	if !ok {
		return buildGetNextRecordErrorResponse([]string{"currentRecordID is missing or not a number in the input JSON"}), nil
	}
	currentRecordID := uint32(currentRecordIDFloat)

	// Call foundation function
	recordData, nextRecordID, err := record.GetNextRecord(tableName, currentRecordID, dbDef)
	if err != nil {
		return buildGetNextRecordErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":          "success",
		"errors":          []string{},
		"tableName":       tableName,
		"currentRecordID": currentRecordID,
		"nextRecordID":    nextRecordID,
		"record":          recordData,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetNextRecordErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

GetRecordByKey_DML is a DML wrapper for the GetRecordByKey foundation function.
This is a convenience function that combines GetRecordID + GetRecordByID.
Parameters:
- inputJSON: JSON input of the format {"tableName": string, "primeKey": value}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string], ...}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"tableName": "TableName",
	"primeKey": <value>  // Type depends on primary key field type
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"tableName": "TableName",
	"primeKey": <value>,
	"recordID": 123,
	"record": {
		"fieldName1": value1,
		"fieldName2": value2,
		...
	}
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...],
	"tableName": "",
	"primeKey": null,
	"recordID": 0,
	"record": null
}

gofmt:on */

func GetRecordByKey_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetRecordByKeyErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildGetRecordByKeyErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract tableName
	tableName, ok := inputMap["tableName"].(string)
	if !ok {
		return buildGetRecordByKeyErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract primeKey
	primeKey, ok := inputMap["primeKey"]
	if !ok {
		return buildGetRecordByKeyErrorResponse([]string{"primeKey is missing in the input JSON"}), nil
	}

	// Find table definition to determine primary key type for conversion
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return buildGetRecordByKeyErrorResponse([]string{fmt.Sprintf("table '%s' not found", tableName)}), nil
	}
	tableDescription := dbDef.Tables[tableIndex]

	if tableDescription.Key == -1 {
		return buildGetRecordByKeyErrorResponse([]string{fmt.Sprintf("table '%s' does not have a primary key", tableName)}), nil
	}

	pkFieldDesc := tableDescription.RecordLayout.Fields[tableDescription.Key]

	// Convert primeKey from JSON format to appropriate Go type
	var convertedPrimeKey interface{}
	switch pkFieldDesc.Type {
	case db.SMALLINT:
		// JSON numbers come as float64
		floatValue, ok := primeKey.(float64)
		if !ok {
			return buildGetRecordByKeyErrorResponse([]string{"primeKey must be a number for SMALLINT primary key"}), nil
		}
		convertedPrimeKey = int16(floatValue)
	case db.INT:
		floatValue, ok := primeKey.(float64)
		if !ok {
			return buildGetRecordByKeyErrorResponse([]string{"primeKey must be a number for INT primary key"}), nil
		}
		convertedPrimeKey = int32(floatValue)
	case db.BIGINT:
		floatValue, ok := primeKey.(float64)
		if !ok {
			return buildGetRecordByKeyErrorResponse([]string{"primeKey must be a number for BIGINT primary key"}), nil
		}
		convertedPrimeKey = int64(floatValue)
	case db.CHAR:
		strValue, ok := primeKey.(string)
		if !ok {
			return buildGetRecordByKeyErrorResponse([]string{"primeKey must be a string for CHAR primary key"}), nil
		}
		if len(strValue) != pkFieldDesc.Size {
			return buildGetRecordByKeyErrorResponse([]string{fmt.Sprintf("primeKey must be exactly %d characters for CHAR[%d] primary key", pkFieldDesc.Size, pkFieldDesc.Size)}), nil
		}
		convertedPrimeKey = strValue
	default:
		return buildGetRecordByKeyErrorResponse([]string{fmt.Sprintf("unsupported primary key type: %d", pkFieldDesc.Type)}), nil
	}

	// Call foundation function with converted primeKey
	recordData, recordID, err := record.GetRecordByKey(tableName, convertedPrimeKey, dbDef)
	if err != nil {
		return buildGetRecordByKeyErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":    "success",
		"errors":    []string{},
		"tableName": tableName,
		"primeKey":  primeKey,
		"recordID":  recordID,
		"record":    recordData,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetRecordByKeyErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for GetRecordByID
func buildGetRecordErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":    "error",
		"errors":    errors,
		"tableName": "",
		"recordID":  0,
		"record":    nil,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}

// Helper function to build error response JSON for GetRecordID
func buildGetRecordIDErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":    "error",
		"errors":    errors,
		"tableName": "",
		"primeKey":  nil,
		"recordID":  0,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}

// Helper function to build error response JSON for GetNextRecord
func buildGetNextRecordErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":          "error",
		"errors":          errors,
		"tableName":       "",
		"currentRecordID": 0,
		"nextRecordID":    0,
		"record":          nil,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}

// Helper function to build error response JSON for GetRecordByKey
func buildGetRecordByKeyErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":    "error",
		"errors":    errors,
		"tableName": "",
		"primeKey":  nil,
		"recordID":  0,
		"record":    nil,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}

/*
GetRecordsByString_DML is a DML wrapper for record.GetRecordsByString.
It retrieves all recordIDs that have a specific STRING field value (exact match).

Input JSON:

	{
	  "tableName": string,
	  "propertyName": string,
	  "propertyValue": string
	}

Output JSON:

	{
	  "status": "success" | "error",
	  "errors": [string, ...],
	  "recordIDs": [uint32, ...]
	}

Returns:
  - string: JSON response
  - error: always nil (errors are encoded in the JSON response)
*/
func GetRecordsByString_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetRecordsByStringErrorResponse([]string{"database not opened"}), nil
	}

	// Parse input JSON
	var input map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &input)
	if err != nil {
		return buildGetRecordsByStringErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract and validate tableName
	tableName, ok := input["tableName"].(string)
	if !ok || tableName == "" {
		return buildGetRecordsByStringErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate propertyName
	propertyName, ok := input["propertyName"].(string)
	if !ok || propertyName == "" {
		return buildGetRecordsByStringErrorResponse([]string{"propertyName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate propertyValue
	propertyValue, ok := input["propertyValue"].(string)
	if !ok {
		return buildGetRecordsByStringErrorResponse([]string{"propertyValue is missing or not a string in the input JSON"}), nil
	}

	// Call foundation function
	recordIDs, err := record.GetRecordsByString(tableName, propertyName, propertyValue, dbDef)
	if err != nil {
		return buildGetRecordsByStringErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":    "success",
		"errors":    []string{},
		"recordIDs": recordIDs,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetRecordsByStringErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for GetRecordsByString
func buildGetRecordsByStringErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":    "error",
		"errors":    errors,
		"recordIDs": []uint32{},
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}

/*
GetRecordsBySubstring_DML is a DML wrapper for record.GetRecordsBySubstring.
It retrieves all recordIDs that have a STRING field value starting with the given substring.

Input JSON:

	{
	  "tableName": string,
	  "propertyName": string,
	  "substring": string
	}

Output JSON:

	{
	  "status": "success" | "error",
	  "errors": [string, ...],
	  "recordIDs": [uint32, ...]
	}

Returns:
  - string: JSON response
  - error: always nil (errors are encoded in the JSON response)
*/
func GetRecordsBySubstring_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetRecordsBySubstringErrorResponse([]string{"database not opened"}), nil
	}

	// Parse input JSON
	var input map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &input)
	if err != nil {
		return buildGetRecordsBySubstringErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract and validate tableName
	tableName, ok := input["tableName"].(string)
	if !ok || tableName == "" {
		return buildGetRecordsBySubstringErrorResponse([]string{"tableName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate propertyName
	propertyName, ok := input["propertyName"].(string)
	if !ok || propertyName == "" {
		return buildGetRecordsBySubstringErrorResponse([]string{"propertyName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate substring
	substring, ok := input["substring"].(string)
	if !ok {
		return buildGetRecordsBySubstringErrorResponse([]string{"substring is missing or not a string in the input JSON"}), nil
	}

	// Call foundation function
	recordIDs, err := record.GetRecordsBySubstring(tableName, propertyName, substring, dbDef)
	if err != nil {
		return buildGetRecordsBySubstringErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":    "success",
		"errors":    []string{},
		"recordIDs": recordIDs,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetRecordsBySubstringErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for GetRecordsBySubstring
func buildGetRecordsBySubstringErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":    "error",
		"errors":    errors,
		"recordIDs": []uint32{},
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
