package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

/* gofmt:off

AddSetMember_DML is a DML wrapper for the AddSetMember foundation function.
Parameters:
- inputJSON: JSON input of the format {"ownerTableName": string, "ownerRecordID": uint32, "setName": string, "memberRecordID": uint32}
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"ownerTableName": "TableName",
	"ownerRecordID": 123,
	"setName": "SetName",
	"memberRecordID": 456
}

Output JSON format (success):
{
	"status": "success",
	"errors": []
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...]
}

gofmt:on */

func AddSetMember_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildSetErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildSetErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract and validate ownerTableName
	ownerTableName, ok := inputMap["ownerTableName"].(string)
	if !ok {
		return buildSetErrorResponse([]string{"ownerTableName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate ownerRecordID
	ownerRecordIDFloat, ok := inputMap["ownerRecordID"].(float64)
	if !ok {
		return buildSetErrorResponse([]string{"ownerRecordID is missing or not a number in the input JSON"}), nil
	}
	ownerRecordID := uint32(ownerRecordIDFloat)

	// Extract and validate setName
	setName, ok := inputMap["setName"].(string)
	if !ok {
		return buildSetErrorResponse([]string{"setName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate memberRecordID
	memberRecordIDFloat, ok := inputMap["memberRecordID"].(float64)
	if !ok {
		return buildSetErrorResponse([]string{"memberRecordID is missing or not a number in the input JSON"}), nil
	}
	memberRecordID := uint32(memberRecordIDFloat)

	// Call foundation function
	err = record.AddSetMember(ownerTableName, ownerRecordID, setName, memberRecordID, dbDef)
	if err != nil {
		return buildSetErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildSetErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

GetSetMembers_DML is a DML wrapper for the GetSetMembers foundation function.
Parameters:
- inputJSON: JSON input of the format {"ownerTableName": string, "ownerRecordID": uint32, "setName": string}
- dbDef: DBDefinition struct with the database definition
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string], "ownerTableName": string, "ownerRecordID": uint32, "setName": string, "memberTableName": string, "members": [uint32]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"ownerTableName": "TableName",
	"ownerRecordID": 123,
	"setName": "SetName"
}

Output JSON format (success):
{
	"status": "success",
	"errors": [],
	"ownerTableName": "TableName",
	"ownerRecordID": 123,
	"setName": "SetName",
	"memberTableName": "MemberTableName",
	"members": [456, 789, ...]
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...],
	"ownerTableName": "",
	"ownerRecordID": 0,
	"setName": "",
	"memberTableName": "",
	"members": []
}

gofmt:on */

func GetSetMembers_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildGetSetMembersErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildGetSetMembersErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract and validate ownerTableName
	ownerTableName, ok := inputMap["ownerTableName"].(string)
	if !ok {
		return buildGetSetMembersErrorResponse([]string{"ownerTableName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate ownerRecordID
	ownerRecordIDFloat, ok := inputMap["ownerRecordID"].(float64)
	if !ok {
		return buildGetSetMembersErrorResponse([]string{"ownerRecordID is missing or not a number in the input JSON"}), nil
	}
	ownerRecordID := uint32(ownerRecordIDFloat)

	// Extract and validate setName
	setName, ok := inputMap["setName"].(string)
	if !ok {
		return buildGetSetMembersErrorResponse([]string{"setName is missing or not a string in the input JSON"}), nil
	}

	// Get the member table name from the set description
	ownerTableIndex, ok := dbDef.TableIndex[ownerTableName]
	if !ok {
		return buildGetSetMembersErrorResponse([]string{fmt.Sprintf("owner table '%s' not found", ownerTableName)}), nil
	}
	ownerTable := dbDef.Tables[ownerTableIndex]

	// Find set description by name
	var memberTableName string
	for _, s := range ownerTable.Sets {
		if s.Name == setName {
			memberTableName = s.MemberTableName
			break
		}
	}

	// Call foundation function
	memberIDs, err := record.GetSetMembers(ownerTableName, ownerRecordID, setName, dbDef)
	if err != nil {
		return buildGetSetMembersErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status":          "success",
		"errors":          []string{},
		"ownerTableName":  ownerTableName,
		"ownerRecordID":   ownerRecordID,
		"setName":         setName,
		"memberTableName": memberTableName,
		"members":         memberIDs,
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildGetSetMembersErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

/* gofmt:off

RemoveSetMember_DML is a DML wrapper for the RemoveSetMember foundation function.
Parameters:
- inputJSON: JSON input of the format {"ownerTableName": string, "ownerRecordID": uint32, "setName": string, "memberRecordID": uint32}
- dbDef: DBDefinition struct with the database definition
Returns:
- string: JSON output of the format {"status": "success"|"error", "errors": [string]}
- error: if there is a critical issue (should rarely occur; most errors are in the JSON response)

Input JSON format:
{
	"ownerTableName": "TableName",
	"ownerRecordID": 123,
	"setName": "SetName",
	"memberRecordID": 456
}

Output JSON format (success):
{
	"status": "success",
	"errors": []
}

Output JSON format (error):
{
	"status": "error",
	"errors": ["error message 1", ...]
}

gofmt:on */

func RemoveSetMember_DML(inputJSON string) (string, error) {
	// Get database definition from singleton
	dbDef := db.Definition()
	if dbDef == nil {
		return buildSetErrorResponse([]string{"database not opened"}), nil
	}

	// Parse JSON input into a map
	var inputMap map[string]interface{}
	err := json.Unmarshal([]byte(inputJSON), &inputMap)
	if err != nil {
		return buildSetErrorResponse([]string{fmt.Sprintf("failed to parse input JSON: %v", err)}), nil
	}

	// Extract and validate ownerTableName
	ownerTableName, ok := inputMap["ownerTableName"].(string)
	if !ok {
		return buildSetErrorResponse([]string{"ownerTableName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate ownerRecordID
	ownerRecordIDFloat, ok := inputMap["ownerRecordID"].(float64)
	if !ok {
		return buildSetErrorResponse([]string{"ownerRecordID is missing or not a number in the input JSON"}), nil
	}
	ownerRecordID := uint32(ownerRecordIDFloat)

	// Extract and validate setName
	setName, ok := inputMap["setName"].(string)
	if !ok {
		return buildSetErrorResponse([]string{"setName is missing or not a string in the input JSON"}), nil
	}

	// Extract and validate memberRecordID
	memberRecordIDFloat, ok := inputMap["memberRecordID"].(float64)
	if !ok {
		return buildSetErrorResponse([]string{"memberRecordID is missing or not a number in the input JSON"}), nil
	}
	memberRecordID := uint32(memberRecordIDFloat)

	// Call foundation function
	err = record.RemoveSetMember(ownerTableName, ownerRecordID, setName, memberRecordID, dbDef)
	if err != nil {
		return buildSetErrorResponse([]string{err.Error()}), nil
	}

	// Build success response
	response := map[string]interface{}{
		"status": "success",
		"errors": []string{},
	}

	responseJSON, err := json.Marshal(response)
	if err != nil {
		return buildSetErrorResponse([]string{fmt.Sprintf("failed to marshal response: %v", err)}), nil
	}

	return string(responseJSON), nil
}

// Helper function to build error response JSON for set operations
func buildSetErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status": "error",
		"errors": errors,
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}

// Helper function to build error response JSON for GetSetMembers
func buildGetSetMembersErrorResponse(errors []string) string {
	response := map[string]interface{}{
		"status":          "error",
		"errors":          errors,
		"ownerTableName":  "",
		"ownerRecordID":   0,
		"setName":         "",
		"memberTableName": "",
		"members":         []uint32{},
	}
	responseJSON, _ := json.Marshal(response)
	return string(responseJSON)
}
