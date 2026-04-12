package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/testdb"
	"testing"
)

// parseBatchResult is a helper to parse batch result JSON and return the map.
func parseBatchResult(t *testing.T, resultJSON string) map[string]interface{} {
	t.Helper()
	var result map[string]interface{}
	err := json.Unmarshal([]byte(resultJSON), &result)
	if err != nil {
		t.Fatalf("Failed to parse result JSON: %v\nJSON: %s", err, resultJSON)
	}
	return result
}

func TestBatch_DML_InvalidJSON(t *testing.T) {
	resultJSON, err := Batch_DML(`{not valid json}`)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_EmptyCommands(t *testing.T) {
	resultJSON, err := Batch_DML(`{"commands": []}`)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_UnsupportedCommand(t *testing.T) {
	input := `{"commands": [{"getRecord": {"tableName": "T"}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_MissingCommandField(t *testing.T) {
	// Two keys in one entry — ambiguous
	input := `{"commands": [{"add": {"tableName": "T"}, "delete": {"tableName": "T", "recordID": 1}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_AddMissingTableName(t *testing.T) {
	input := `{"commands": [{"add": {"record": {"x": 1}}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_AddMissingRecord(t *testing.T) {
	input := `{"commands": [{"add": {"tableName": "T"}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_UpdateMissingRecordID(t *testing.T) {
	input := `{"commands": [{"update": {"tableName": "T", "record": {"x": 1}}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_DeleteMissingRecordID(t *testing.T) {
	input := `{"commands": [{"delete": {"tableName": "T"}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_AddSetMemberMissingFields(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"missing ownerTableName", `{"commands": [{"addSetMember": {"ownerRecordID": 1, "setName": "S", "memberRecordID": 2}}]}`},
		{"missing ownerRecordID", `{"commands": [{"addSetMember": {"ownerTableName": "T", "setName": "S", "memberRecordID": 2}}]}`},
		{"missing setName", `{"commands": [{"addSetMember": {"ownerTableName": "T", "ownerRecordID": 1, "memberRecordID": 2}}]}`},
		{"missing memberRecordID", `{"commands": [{"addSetMember": {"ownerTableName": "T", "ownerRecordID": 1, "setName": "S"}}]}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			resultJSON, err := Batch_DML(tt.input)
			if err != nil {
				t.Fatalf("Batch_DML returned error: %v", err)
			}
			result := parseBatchResult(t, resultJSON)
			if result["status"] != "error" {
				t.Errorf("Expected status=error, got %v", result["status"])
			}
		})
	}
}

func TestBatch_DML_RemoveSetMemberMissingFields(t *testing.T) {
	input := `{"commands": [{"removeSetMember": {"ownerTableName": "T", "ownerRecordID": 1}}]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error, got %v", result["status"])
	}
}

func TestBatch_DML_SecondCommandInvalid(t *testing.T) {
	input := `{"commands": [
		{"add": {"tableName": "T", "record": {"x": 1}}},
		{"delete": {"tableName": "T"}}
	]}`
	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Errorf("Expected status=error for missing recordID on second command, got %v", result["status"])
	}
}

func TestBatch_DML_ValidationPassesAllCommands(t *testing.T) {
	// This test only checks that validation passes — it needs a DB for execution.
	// Use NIP database.
	tempDir := t.TempDir()
	_, _, err := testdb.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create NIP database: %v", err)
	}
	defer db.CloseDB()

	// Get a teacher recordID to use in set operations
	teacherJSON, err := GetRecordByKey_DML(`{"tableName": "Teachers", "primeKey": "2010T342"}`)
	if err != nil {
		t.Fatalf("GetRecordByKey_DML failed: %v", err)
	}
	teacherResult := parseBatchResult(t, teacherJSON)
	if teacherResult["status"] != "success" {
		t.Fatalf("Could not find teacher 2010T342: %v", teacherResult["errors"])
	}
	teacherRecordID := teacherResult["recordID"].(float64)

	// Get a student recordID for set member
	studentJSON, err := GetRecordByKey_DML(`{"tableName": "Students", "primeKey": "NIP2209001"}`)
	if err != nil {
		t.Fatalf("GetRecordByKey_DML failed: %v", err)
	}
	studentResult := parseBatchResult(t, studentJSON)
	if studentResult["status"] != "success" {
		t.Fatalf("Could not find student NIP2209001: %v", studentResult["errors"])
	}
	studentRecordID := studentResult["recordID"].(float64)

	input := fmt.Sprintf(`{"commands": [
		{"add": {"tableName": "Departments", "record": {"Name": "Batch Test Dept", "Department_code": "BATCHTES", "Building_name": "Main Hall", "Building_code": "MAIN0001"}}},
		{"addSetMember": {"ownerTableName": "Teachers", "ownerRecordID": %v, "setName": "Advises", "memberRecordID": %v}},
		{"removeSetMember": {"ownerTableName": "Teachers", "ownerRecordID": %v, "setName": "Advises", "memberRecordID": %v}}
	]}`, teacherRecordID, studentRecordID, teacherRecordID, studentRecordID)

	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "success" {
		t.Errorf("Expected status=success, got %v, error=%v", result["status"], result["failedError"])
	}

	// Verify results array
	results, ok := result["results"].([]interface{})
	if !ok || len(results) != 3 {
		t.Fatalf("Expected 3 results, got %v", results)
	}
}

// --- Integration tests using NIP database ---

// setupNIPDatabase creates and populates the NIP database, returning cleanup via t.Cleanup.
func setupNIPDatabase(t *testing.T) {
	t.Helper()
	tempDir := t.TempDir()
	_, _, err := testdb.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create NIP database: %v", err)
	}
	t.Cleanup(func() { db.CloseDB() })
}

func TestBatch_DML_HappyPath_AddUpdateDelete(t *testing.T) {
	setupNIPDatabase(t)

	// Batch: add a department, then update it, then delete it
	input := `{"commands": [
		{"add": {"tableName": "Departments", "record": {"Name": "Testing Department", "Department_code": "TESTDEPT", "Building_name": "West Hall", "Building_code": "WEST0001"}}},
		{"add": {"tableName": "Departments", "record": {"Name": "Another Department", "Department_code": "ANOTHERD", "Building_name": "East Hall", "Building_code": "EAST0001"}}}
	]}`

	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "success" {
		t.Fatalf("Expected success, got %v, error=%v", result["status"], result["failedError"])
	}

	results := result["results"].([]interface{})
	if len(results) != 2 {
		t.Fatalf("Expected 2 results, got %d", len(results))
	}

	// Get the recordIDs from the add results
	firstResult := results[0].(map[string]interface{})
	firstRecordID := firstResult["recordID"].(float64)
	secondResult := results[1].(map[string]interface{})
	secondRecordID := secondResult["recordID"].(float64)

	// Now update first and delete second in a batch
	input2 := fmt.Sprintf(`{"commands": [
		{"update": {"tableName": "Departments", "recordID": %v, "record": {"Department_code": "TESTDEPT", "Name": "Updated Testing Department"}}},
		{"delete": {"tableName": "Departments", "recordID": %v}}
	]}`, firstRecordID, secondRecordID)

	resultJSON2, err := Batch_DML(input2)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result2 := parseBatchResult(t, resultJSON2)
	if result2["status"] != "success" {
		t.Fatalf("Expected success, got %v, error=%v", result2["status"], result2["failedError"])
	}

	// Verify the update took effect
	getJSON, err := GetRecordByID_DML(fmt.Sprintf(`{"tableName": "Departments", "recordID": %v}`, firstRecordID))
	if err != nil {
		t.Fatalf("GetRecordByID_DML failed: %v", err)
	}
	getResult := parseBatchResult(t, getJSON)
	rec := getResult["record"].(map[string]interface{})
	if rec["Name"] != "Updated Testing Department" {
		t.Errorf("Expected updated name, got %v", rec["Name"])
	}

	// Verify the delete took effect
	getJSON2, err := GetRecordByID_DML(fmt.Sprintf(`{"tableName": "Departments", "recordID": %v}`, secondRecordID))
	if err != nil {
		t.Fatalf("GetRecordByID_DML failed: %v", err)
	}
	getResult2 := parseBatchResult(t, getJSON2)
	if getResult2["status"] != "error" {
		t.Errorf("Expected error for deleted record, got %v", getResult2["status"])
	}
}

func TestBatch_DML_FailureMidBatch(t *testing.T) {
	setupNIPDatabase(t)

	// First command succeeds (add), second fails (delete nonexistent record)
	input := `{"commands": [
		{"add": {"tableName": "Departments", "record": {"Name": "Will Be Undone", "Department_code": "UNDODEPT", "Building_name": "Main", "Building_code": "MAIN0001"}}},
		{"delete": {"tableName": "Departments", "recordID": 99999}}
	]}`

	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Fatalf("Expected error, got %v", result["status"])
	}

	failedAtIndex := result["failedAtIndex"].(float64)
	if failedAtIndex != 1 {
		t.Errorf("Expected failedAtIndex=1, got %v", failedAtIndex)
	}
	if result["failedCommand"] != "delete" {
		t.Errorf("Expected failedCommand=delete, got %v", result["failedCommand"])
	}
}

func TestBatch_DML_UndoAdd(t *testing.T) {
	setupNIPDatabase(t)

	// Add succeeds, then delete of nonexistent fails — add should be undone
	input := `{"commands": [
		{"add": {"tableName": "Departments", "record": {"Name": "Undo Me", "Department_code": "UNDOADD1", "Building_name": "Main", "Building_code": "MAIN0001"}}},
		{"delete": {"tableName": "Departments", "recordID": 99999}}
	]}`

	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Fatalf("Expected error, got %v", result["status"])
	}

	// The add's recordID should be in results
	results := result["results"].([]interface{})
	if len(results) != 1 {
		t.Fatalf("Expected 1 result, got %d", len(results))
	}
	addResult := results[0].(map[string]interface{})
	addedRecordID := addResult["recordID"].(float64)

	// Verify the added record was undone (deleted)
	getInput := fmt.Sprintf(`{"tableName":"Departments","recordID":%v}`, addedRecordID)
	getJSON, _ := GetRecordByID_DML(getInput)
	getResult := parseBatchResult(t, getJSON)
	if getResult["status"] == "success" {
		t.Errorf("Expected the added record (ID %v) to be undone, but it still exists", addedRecordID)
	}

	// undoErrors should be empty
	if undoErrs, ok := result["undoErrors"]; ok && undoErrs != nil {
		if errs, ok := undoErrs.([]interface{}); ok && len(errs) > 0 {
			t.Errorf("Expected no undoErrors, got %v", undoErrs)
		}
	}
}

func TestBatch_DML_UndoUpdate(t *testing.T) {
	setupNIPDatabase(t)

	// First, add a record we can update
	addInput := `{"tableName":"Departments","record":{"Name":"Original Name","Department_code":"ORIGDEPT","Building_name":"North Hall","Building_code":"NORT0001"}}`
	addJSON, _ := AddNewRecord_DML(addInput)
	addResult := parseBatchResult(t, addJSON)
	recordID := addResult["recordID"].(float64)

	// Batch: update the record, then fail
	input := fmt.Sprintf(`{"commands": [
		{"update": {"tableName": "Departments", "recordID": %v, "record": {"Name": "Updated Name"}}},
		{"delete": {"tableName": "Departments", "recordID": 99999}}
	]}`, recordID)

	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Fatalf("Expected error, got %v", result["status"])
	}

	// Verify the record was reverted to original values
	getInput := fmt.Sprintf(`{"tableName":"Departments","recordID":%v}`, recordID)
	getJSON, _ := GetRecordByID_DML(getInput)
	getResult := parseBatchResult(t, getJSON)
	if getResult["status"] != "success" {
		t.Fatalf("Expected to find record after undo, got status=%v", getResult["status"])
	}
	record := getResult["record"].(map[string]interface{})
	if record["Name"] != "Original Name" {
		t.Errorf("Expected Name to be reverted to 'Original Name', got %v", record["Name"])
	}
}

func TestBatch_DML_UndoMultipleAdds(t *testing.T) {
	setupNIPDatabase(t)

	// Three adds succeed, fourth command (delete nonexistent) fails — all three adds should be undone
	input := `{"commands": [
		{"add": {"tableName": "Departments", "record": {"Name": "Dept A", "Department_code": "DEPTAAA1", "Building_name": "Main", "Building_code": "MAIN0001"}}},
		{"add": {"tableName": "Departments", "record": {"Name": "Dept B", "Department_code": "DEPTBBB2", "Building_name": "Main", "Building_code": "MAIN0002"}}},
		{"add": {"tableName": "Departments", "record": {"Name": "Dept C", "Department_code": "DEPTCCC3", "Building_name": "Main", "Building_code": "MAIN0003"}}},
		{"delete": {"tableName": "Departments", "recordID": 99999}}
	]}`

	resultJSON, err := Batch_DML(input)
	if err != nil {
		t.Fatalf("Batch_DML returned error: %v", err)
	}
	result := parseBatchResult(t, resultJSON)
	if result["status"] != "error" {
		t.Fatalf("Expected error, got %v", result["status"])
	}

	// Verify all three added records were undone
	results := result["results"].([]interface{})
	if len(results) != 3 {
		t.Fatalf("Expected 3 results, got %d", len(results))
	}
	for idx, r := range results {
		addResult := r.(map[string]interface{})
		addedRecordID := addResult["recordID"].(float64)
		getInput := fmt.Sprintf(`{"tableName":"Departments","recordID":%v}`, addedRecordID)
		getJSON, _ := GetRecordByID_DML(getInput)
		getResult := parseBatchResult(t, getJSON)
		if getResult["status"] == "success" {
			t.Errorf("Command %d: expected record %v to be undone, but it still exists", idx, addedRecordID)
		}
	}
}
