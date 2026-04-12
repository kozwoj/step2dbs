package dml

import (
	"encoding/json"
	"fmt"
	"path/filepath"
	"github.com/kozwoj/step2/db"
	"testing"
)

// TestAddNewRecord_DML tests the AddNewRecord DML wrapper
func TestAddNewRecord_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	// Create database
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Open database
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Test successful add
	t.Run("Add record success", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"record": {
				"Integer_value": 100,
				"Small_int_value": 1,
				"Big_int_value": 1000,
				"Decimal_value": "10.50",
				"Float_value": 1.5,
				"String_size_value": "Test",
				"String_no_size_value": "Test",
				"Char_array_value": "TESTCHAR0000000",
				"Boolean_value": true,
				"Date_value": "2024-03-01",
				"Time_value": "10:00:00"
			}
		}`

		resultJSON, err := AddNewRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("AddNewRecord_DML returned error: %v", err)
		}

		// Parse result
		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		// Check status
		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		// Check recordID
		recordID, ok := result["recordID"].(float64)
		if !ok || recordID != 1 {
			t.Errorf("Expected recordID=1, got %v", result["recordID"])
		}
	})

	// Test invalid JSON
	t.Run("Add record - invalid JSON", func(t *testing.T) {
		inputJSON := `{"tableName": "AllTypes", "record": {invalid}}`

		resultJSON, err := AddNewRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("AddNewRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for invalid JSON")
		}
	})

	// Test missing tableName
	t.Run("Add record - missing tableName", func(t *testing.T) {
		inputJSON := `{
			"record": {
				"Integer_value": 200
			}
		}`

		resultJSON, err := AddNewRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("AddNewRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing tableName")
		}
	})
}

// TestGetRecordByID_DML tests the GetRecordByID DML wrapper
func TestGetRecordByID_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	// Create and open database
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "AllTypesTes")

	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add a record first
	addJSON := `{
		"tableName": "AllTypes",
		"record": {
			"Integer_value": 100,
			"Small_int_value": 1,
			"Big_int_value": 1000,
			"Decimal_value": "10.50",
			"Float_value": 1.5,
			"String_size_value": "Test",
			"String_no_size_value": "Test",
			"Char_array_value": "TESTCHAR0000000",
			"Boolean_value": true,
			"Date_value": "2024-03-01",
			"Time_value": "10:00:00"
		}
	}`
	AddNewRecord_DML(addJSON)

	// Test get record
	t.Run("Get record success", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"recordID": 1
		}`

		resultJSON, err := GetRecordByID_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByID_DML returned error: %v", err)
		}

		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		// Check record data
		record, ok := result["record"].(map[string]interface{})
		if !ok {
			t.Fatalf("Record data not found or not a map")
		}

		if record["Integer_value"] != float64(100) {
			t.Errorf("Expected Integer_value=100, got %v", record["Integer_value"])
		}
	})

	// Test non-existent record
	t.Run("Get non-existent record", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"recordID": 999
		}`

		resultJSON, err := GetRecordByID_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByID_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for non-existent record")
		}
	})
}

// TestUpdateRecord_DML tests the UpdateRecord DML wrapper
func TestUpdateRecord_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	db.CreateDB(tempDir, schemaFile)
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	err := db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add a record first
	addJSON := `{
		"tableName": "AllTypes",
		"record": {
			"Integer_value": 100,
			"Small_int_value": 1,
			"Big_int_value": 1000,
			"Decimal_value": "10.50",
			"Float_value": 1.5,
			"String_size_value": "Original",
			"String_no_size_value": "Test",
			"Char_array_value": "TESTCHAR0000000",
			"Boolean_value": true,
			"Date_value": "2024-03-01",
			"Time_value": "10:00:00"
		}
	}`
	AddNewRecord_DML(addJSON)

	// Test update
	t.Run("Update record success", func(t *testing.T) {
		updateJSON := `{
			"tableName": "AllTypes",
			"recordID": 1,
			"record": {
				"Small_int_value": 200,
				"Boolean_value": false
			}
		}`

		resultJSON, err := UpdateRecord_DML(updateJSON)
		if err != nil {
			t.Fatalf("UpdateRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}
	})
}

// TestDeleteRecord_DML tests the DeleteRecord DML wrapper
func TestDeleteRecord_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	db.CreateDB(tempDir, schemaFile)
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	err := db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add a record first
	addJSON := `{
		"tableName": "AllTypes",
		"record": {
			"Integer_value": 100,
			"Small_int_value": 1,
			"Big_int_value": 1000,
			"Decimal_value": "10.50",
			"Float_value": 1.5,
			"String_size_value": "Test",
			"String_no_size_value": "Test",
			"Char_array_value": "TESTCHAR0000000",
			"Boolean_value": true,
			"Date_value": "2024-03-01",
			"Time_value": "10:00:00"
		}
	}`
	AddNewRecord_DML(addJSON)

	// Test delete
	t.Run("Delete record success", func(t *testing.T) {
		deleteJSON := `{
			"tableName": "AllTypes",
			"recordID": 1
		}`

		resultJSON, err := DeleteRecord_DML(deleteJSON)
		if err != nil {
			t.Fatalf("DeleteRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}
	})
}

// TestGetRecordID_DML tests the GetRecordID DML wrapper
func TestGetRecordID_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	db.CreateDB(tempDir, schemaFile)
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	err := db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add a record first
	addJSON := `{
		"tableName": "AllTypes",
		"record": {
			"Integer_value": 100,
			"Small_int_value": 1,
			"Big_int_value": 1000,
			"Decimal_value": "10.50",
			"Float_value": 1.5,
			"String_size_value": "Test",
			"String_no_size_value": "Test",
			"Char_array_value": "TESTCHAR0000000",
			"Boolean_value": true,
			"Date_value": "2024-03-01",
			"Time_value": "10:00:00"
		}
	}`
	AddNewRecord_DML(addJSON)

	// Test get record ID
	t.Run("Get record ID success", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"primeKey": 100
		}`

		resultJSON, err := GetRecordID_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordID_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		if result["recordID"] != float64(1) {
			t.Errorf("Expected recordID=1, got %v", result["recordID"])
		}
	})
}

// TestGetNextRecord_DML tests the GetNextRecord DML wrapper
func TestGetNextRecord_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	db.CreateDB(tempDir, schemaFile)
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	err := db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add multiple test records
	for i := 1; i <= 3; i++ {
		addJSON := fmt.Sprintf(`{
			"tableName": "AllTypes",
			"record": {
				"Integer_value": %d,
				"Small_int_value": %d,
				"Big_int_value": 1000,
				"Decimal_value": "10.50",
				"Float_value": 1.5,
				"String_size_value": "Test%d",
				"String_no_size_value": "Test",
				"Char_array_value": "TESTCHAR0000000",
				"Boolean_value": true,
				"Date_value": "2024-03-01",
				"Time_value": "10:00:00"
			}
		}`, i*100, i, i)
		AddNewRecord_DML(addJSON)
	}

	// Test get next record from start
	t.Run("Get next record from start", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"currentRecordID": 0
		}`

		resultJSON, err := GetNextRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetNextRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		if result["nextRecordID"] != float64(1) {
			t.Errorf("Expected nextRecordID=1, got %v", result["nextRecordID"])
		}

		// Check record data
		record, ok := result["record"].(map[string]interface{})
		if !ok {
			t.Fatalf("Record data not found or not a map")
		}

		if record["Small_int_value"] != float64(1) {
			t.Errorf("Expected Small_int_value=1, got %v", record["Small_int_value"])
		}
	})

	// Test get next record from middle
	t.Run("Get next record from middle", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"currentRecordID": 1
		}`

		resultJSON, err := GetNextRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetNextRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		if result["nextRecordID"] != float64(2) {
			t.Errorf("Expected nextRecordID=2, got %v", result["nextRecordID"])
		}
	})

	// Test no more records
	t.Run("No more records", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"currentRecordID": 3
		}`

		resultJSON, err := GetNextRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetNextRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error when no more records")
		}
	})

	// Test invalid JSON
	t.Run("Invalid JSON", func(t *testing.T) {
		inputJSON := `{invalid json}`

		resultJSON, err := GetNextRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetNextRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for invalid JSON")
		}
	})

	// Test missing tableName
	t.Run("Missing tableName", func(t *testing.T) {
		inputJSON := `{
			"currentRecordID": 1
		}`

		resultJSON, err := GetNextRecord_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetNextRecord_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing tableName")
		}
	})
}

// TestGetRecordByKey_DML tests the GetRecordByKey DML wrapper
func TestGetRecordByKey_DML(t *testing.T) {
	// Create a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	db.CreateDB(tempDir, schemaFile)
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	err := db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add test records with known primary keys
	for i := 1; i <= 3; i++ {
		addJSON := fmt.Sprintf(`{
			"tableName": "AllTypes",
			"record": {
				"Integer_value": %d,
				"Small_int_value": %d,
				"Big_int_value": 1000,
				"Decimal_value": "10.50",
				"Float_value": 1.5,
				"String_size_value": "Record%d",
				"String_no_size_value": "Test",
				"Char_array_value": "TESTCHAR0000000",
				"Boolean_value": true,
				"Date_value": "2024-03-01",
				"Time_value": "10:00:00"
			}
		}`, i*100, i, i*100)
		AddNewRecord_DML(addJSON)
	}

	// Test successful retrieval with INT primary key
	t.Run("Get record by INT primary key", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"primeKey": 200
		}`

		resultJSON, err := GetRecordByKey_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByKey_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		if result["recordID"] != float64(2) {
			t.Errorf("Expected recordID=2, got %v", result["recordID"])
		}

		// Check record data
		record, ok := result["record"].(map[string]interface{})
		if !ok {
			t.Fatalf("Record data not found or not a map")
		}

		if record["Integer_value"] != float64(200) {
			t.Errorf("Expected Integer_value=200, got %v", record["Integer_value"])
		}

		if record["String_size_value"] != "Record200" {
			t.Errorf("Expected String_size_value='Record200', got %v", record["String_size_value"])
		}
	})

	// Test non-existent primary key
	t.Run("Non-existent primary key", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes",
			"primeKey": 999
		}`

		resultJSON, err := GetRecordByKey_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByKey_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for non-existent primary key")
		}
	})

	// Test invalid JSON
	t.Run("Invalid JSON", func(t *testing.T) {
		inputJSON := `{invalid json}`

		resultJSON, err := GetRecordByKey_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByKey_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for invalid JSON")
		}
	})

	// Test missing tableName
	t.Run("Missing tableName", func(t *testing.T) {
		inputJSON := `{
			"primeKey": 100
		}`

		resultJSON, err := GetRecordByKey_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByKey_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing tableName")
		}
	})

	// Test missing primeKey
	t.Run("Missing primeKey", func(t *testing.T) {
		inputJSON := `{
			"tableName": "AllTypes"
		}`

		resultJSON, err := GetRecordByKey_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordByKey_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing primeKey")
		}
	})
}

// TestGetRecordsByString_DML tests the GetRecordsByString DML wrapper
func TestGetRecordsByString_DML(t *testing.T) {
	// Setup - use Customer_Employee schema which has many STRING fields
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl")

	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "TestSchema")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add test customers
	t.Run("Setup: Add test customers", func(t *testing.T) {
		customers := []map[string]interface{}{
			{
				"Customer_id":   "CUST000001",
				"Company_name":  "TechCorp",
				"Contact_name":  "Alice Smith",
				"Contact_title": "CEO",
				"Address":       "100 Tech Ave",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "206-555-1000",
				"Fax":           "206-555-1001",
			},
			{
				"Customer_id":   "CUST000002",
				"Company_name":  "DataInc",
				"Contact_name":  "Bob Johnson",
				"Contact_title": "CTO",
				"Address":       "200 Data St",
				"City":          "Portland",
				"Region":        "OR",
				"Postal_code":   "97201",
				"Country":       "USA",
				"Phone":         "503-555-2000",
				"Fax":           "503-555-2001",
			},
			{
				"Customer_id":   "CUST000003",
				"Company_name":  "CloudSys",
				"Contact_name":  "Carol White",
				"Contact_title": "VP",
				"Address":       "300 Cloud Blvd",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98102",
				"Country":       "USA",
				"Phone":         "206-555-3000",
				"Fax":           "206-555-3001",
			},
		}

		for _, customer := range customers {
			inputMap := map[string]interface{}{
				"tableName": "Customers",
				"record":    customer,
			}
			inputJSON, _ := json.Marshal(inputMap)
			resultJSON, err := AddNewRecord_DML(string(inputJSON))
			if err != nil {
				t.Fatalf("AddNewRecord_DML failed: %v", err)
			}

			var result map[string]interface{}
			json.Unmarshal([]byte(resultJSON), &result)
			if result["status"] != "success" {
				t.Fatalf("Failed to add customer: %v", result["errors"])
			}
		}
		t.Log("Added 3 test customers")
	})

	// Test successful search with multiple results
	t.Run("Search City=Seattle (2 results)", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "City",
			"propertyValue": "Seattle"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		recordIDs, ok := result["recordIDs"].([]interface{})
		if !ok {
			t.Fatalf("recordIDs field is not an array")
		}

		if len(recordIDs) != 2 {
			t.Errorf("Expected 2 records with City=Seattle, got %d", len(recordIDs))
		} else {
			t.Logf("Found 2 customers in Seattle: %v", recordIDs)
		}
	})

	// Test successful search with single result
	t.Run("Search City=Portland (1 result)", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "City",
			"propertyValue": "Portland"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v", result["status"])
		}

		recordIDs := result["recordIDs"].([]interface{})
		if len(recordIDs) != 1 {
			t.Errorf("Expected 1 record, got %d", len(recordIDs))
		}
	})

	// Test search with no results
	t.Run("Search for non-existent city (0 results)", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "City",
			"propertyValue": "Tokyo"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("Expected status=success for no results, got %v", result["status"])
		}

		recordIDs := result["recordIDs"].([]interface{})
		if len(recordIDs) != 0 {
			t.Errorf("Expected 0 records, got %d", len(recordIDs))
		} else {
			t.Log("Correctly returned empty array for non-existent city")
		}
	})

	// Test invalid JSON
	t.Run("Invalid JSON", func(t *testing.T) {
		inputJSON := `{invalid json}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for invalid JSON")
		}
	})

	// Test missing tableName
	t.Run("Missing tableName", func(t *testing.T) {
		inputJSON := `{
			"propertyName": "City",
			"propertyValue": "Seattle"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing tableName")
		}
	})

	// Test missing propertyName
	t.Run("Missing propertyName", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyValue": "Seattle"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing propertyName")
		}
	})

	// Test missing propertyValue
	t.Run("Missing propertyValue", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "City"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for missing propertyValue")
		}
	})

	// Test invalid table
	t.Run("Invalid table name", func(t *testing.T) {
		inputJSON := `{
			"tableName": "InvalidTable",
			"propertyName": "City",
			"propertyValue": "Seattle"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for invalid table")
		}
	})

	// Test non-STRING field
	t.Run("Non-STRING field (CHAR)", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Customer_id",
			"propertyValue": "CUST000001"
		}`

		resultJSON, err := GetRecordsByString_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsByString_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Errorf("Expected status=error for non-STRING field")
		}
	})
}

// TestGetRecordsBySubstring_DML tests GetRecordsBySubstring_DML function
func TestGetRecordsBySubstring_DML(t *testing.T) {
	// Setup - use Customer_Employee schema
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl")

	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "TestSchema")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Add test customers
	t.Run("Add test customers", func(t *testing.T) {
		customers := []map[string]interface{}{
			{
				"Customer_id":   "CUST000001",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Smith John",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
			{
				"Customer_id":   "CUST000002",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Smithson Alice",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
			{
				"Customer_id":   "CUST000003",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Johnson Mary",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
			{
				"Customer_id":   "CUST000004",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Smithers Bob",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
			{
				"Customer_id":   "CUST000005",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Kowalski Tom",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
			{
				"Customer_id":   "CUST000006",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Kozaczynski Wojtek",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
			{
				"Customer_id":   "CUST000007",
				"Company_name":  "ACME Corp",
				"Contact_name":  "Anderson Jane",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          "Seattle",
				"Region":        "WA",
				"Postal_code":   "98101",
				"Country":       "USA",
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			},
		}

		for _, customer := range customers {
			inputMap := map[string]interface{}{
				"tableName": "Customers",
				"record":    customer,
			}
			inputJSON, _ := json.Marshal(inputMap)
			resultJSON, err := AddNewRecord_DML(string(inputJSON))
			if err != nil {
				t.Fatalf("AddNewRecord_DML failed: %v", err)
			}

			var result map[string]interface{}
			if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
				t.Fatalf("Failed to unmarshal response: %v", err)
			}

			if result["status"] != "success" {
				t.Fatalf("Failed to add customer: %v", result["errors"])
			}
		}
		t.Log("Added 7 test customers")
	})

	// Search with "Smith" prefix (3 matches)
	t.Run("Search Contact_name starting with 'Smith'", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": "Smith"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		recordIDs, ok := result["recordIDs"].([]interface{})
		if !ok {
			t.Fatalf("recordIDs field is not an array")
		}

		if len(recordIDs) != 3 {
			t.Errorf("Expected 3 records, got %d", len(recordIDs))
		}
	})

	// Search with "Koz" prefix (1 match)
	t.Run("Search Contact_name starting with 'Koz'", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": "Koz"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		recordIDs, ok := result["recordIDs"].([]interface{})
		if !ok {
			t.Fatalf("recordIDs field is not an array")
		}

		if len(recordIDs) != 1 {
			t.Errorf("Expected 1 record, got %d", len(recordIDs))
		}
	})

	// Search with "J" prefix (1 match)
	t.Run("Search Contact_name starting with 'J'", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": "J"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		recordIDs, ok := result["recordIDs"].([]interface{})
		if !ok {
			t.Fatalf("recordIDs field is not an array")
		}

		if len(recordIDs) != 1 {
			t.Errorf("Expected 1 record, got %d", len(recordIDs))
		}
	})

	// Search with no matches
	t.Run("Search with no matches", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": "Xyz"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		recordIDs, ok := result["recordIDs"].([]interface{})
		if !ok {
			t.Fatalf("recordIDs field is not an array")
		}

		if len(recordIDs) != 0 {
			t.Errorf("Expected 0 records, got %d", len(recordIDs))
		}
	})

	// Test 8-character substring
	t.Run("8-character substring", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": "Smithson"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			t.Fatalf("Failed to unmarshal response: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		recordIDs, ok := result["recordIDs"].([]interface{})
		if !ok {
			t.Fatalf("recordIDs field is not an array")
		}

		if len(recordIDs) != 1 {
			t.Errorf("Expected 1 record, got %d", len(recordIDs))
		}
	})

	// Test substring exceeds 8 characters
	t.Run("Substring exceeds 8 characters", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": "VeryLongName"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for substring > 8 characters")
		}
	})

	// Test empty substring
	t.Run("Empty substring", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name",
			"substring": ""
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for empty substring")
		}
	})

	// Test invalid JSON
	t.Run("Invalid JSON input", func(t *testing.T) {
		inputJSON := `{"tableName": "Customers", "propertyName": "Contact_name"` // Missing closing brace

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for invalid JSON")
		}
	})

	// Test missing tableName
	t.Run("Missing tableName", func(t *testing.T) {
		inputJSON := `{
			"propertyName": "Contact_name",
			"substring": "Smith"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for missing tableName")
		}
	})

	// Test missing propertyName
	t.Run("Missing propertyName", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"substring": "Smith"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for missing propertyName")
		}
	})

	// Test missing substring
	t.Run("Missing substring", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Contact_name"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for missing substring")
		}
	})

	// Test invalid table name
	t.Run("Invalid table name", func(t *testing.T) {
		inputJSON := `{
			"tableName": "InvalidTable",
			"propertyName": "Contact_name",
			"substring": "Smith"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for invalid table name")
		}
	})

	// Test non-STRING field
	t.Run("Non-STRING field", func(t *testing.T) {
		inputJSON := `{
			"tableName": "Customers",
			"propertyName": "Customer_id",
			"substring": "CUST"
		}`

		resultJSON, err := GetRecordsBySubstring_DML(inputJSON)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for non-STRING field")
		}
	})
}

// TestCreateDB_DML tests the CreateDB DML wrapper
func TestCreateDB_DML(t *testing.T) {
	tempDir := filepath.ToSlash(t.TempDir())
	schemaFile := filepath.ToSlash(filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl"))

	// Test successful create
	t.Run("Create database success", func(t *testing.T) {
		inputJSON := fmt.Sprintf(`{
			"dirPath": "%s",
			"schemaPath": "%s"
		}`, tempDir, schemaFile)

		resultJSON, err := CreateDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("CreateDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}
	})

	// Test invalid JSON
	t.Run("Invalid JSON input", func(t *testing.T) {
		inputJSON := `{"dirPath": "test", invalid}`

		resultJSON, err := CreateDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("CreateDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for invalid JSON")
		}
	})

	// Test missing dirPath
	t.Run("Missing dirPath", func(t *testing.T) {
		inputJSON := `{
			"schemaPath": "/path/to/schema.ddl"
		}`

		resultJSON, err := CreateDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("CreateDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for missing dirPath")
		}
	})

	// Test missing schemaPath
	t.Run("Missing schemaPath", func(t *testing.T) {
		inputJSON := `{
			"dirPath": "/tmp/testdb"
		}`

		resultJSON, err := CreateDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("CreateDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for missing schemaPath")
		}
	})

	// Test invalid schema file
	t.Run("Invalid schema file", func(t *testing.T) {
		inputJSON := fmt.Sprintf(`{
			"dirPath": "%s",
			"schemaPath": "/nonexistent/schema.ddl"
		}`, tempDir)

		resultJSON, err := CreateDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("CreateDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for invalid schema file")
		}
	})
}

// TestOpenDB_DML tests the OpenDB DML wrapper
func TestOpenDB_DML(t *testing.T) {
	// Setup - create a test database first
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl")

	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.ToSlash(filepath.Join(tempDir, "TestSchema"))

	// Test successful open
	t.Run("Open database success", func(t *testing.T) {
		inputJSON := fmt.Sprintf(`{
			"dirPath": "%s"
		}`, dbDir)

		resultJSON, err := OpenDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("OpenDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		// Clean up - close the database
		db.CloseDB()
	})

	// Test invalid JSON
	t.Run("Invalid JSON input", func(t *testing.T) {
		inputJSON := `{"dirPath": invalid}`

		resultJSON, err := OpenDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("OpenDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for invalid JSON")
		}
	})

	// Test missing dirPath
	t.Run("Missing dirPath", func(t *testing.T) {
		inputJSON := `{}`

		resultJSON, err := OpenDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("OpenDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for missing dirPath")
		}
	})

	// Test nonexistent database
	t.Run("Nonexistent database", func(t *testing.T) {
		inputJSON := `{
			"dirPath": "/nonexistent/database"
		}`

		resultJSON, err := OpenDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("OpenDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error for nonexistent database")
		}
	})

	// Test opening when database already open
	t.Run("Database already open", func(t *testing.T) {
		// First open
		inputJSON := fmt.Sprintf(`{
			"dirPath": "%s"
		}`, dbDir)

		resultJSON, err := OpenDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("First OpenDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "success" {
			t.Errorf("First open failed: %v", result["errors"])
		}

		// Try to open again
		resultJSON, err = OpenDB_DML(inputJSON)
		if err != nil {
			t.Fatalf("Second OpenDB_DML returned error: %v", err)
		}

		json.Unmarshal([]byte(resultJSON), &result)

		if result["status"] != "error" {
			t.Error("Expected status=error when database already open")
		}

		// Clean up
		db.CloseDB()
	})
}

// TestCloseDB_DML tests the CloseDB DML wrapper
func TestCloseDB_DML(t *testing.T) {
	// Setup - create and open a test database
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl")

	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "TestSchema")

	// Test close success
	t.Run("Close database success", func(t *testing.T) {
		// Open database first
		err = db.OpenDB(dbDir)
		if err != nil {
			t.Fatalf("OpenDB failed: %v", err)
		}

		// Close using DML wrapper
		resultJSON, err := CloseDB_DML()
		if err != nil {
			t.Fatalf("CloseDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success, got status=%v, errors=%v", result["status"], result["errors"])
		}

		// Verify database is closed by trying to get definition
		if db.DefinitionInitialized() {
			t.Error("Expected database to be closed")
		}
	})

	// Test close when no database is open (should succeed)
	t.Run("Close when no database open", func(t *testing.T) {
		resultJSON, err := CloseDB_DML()
		if err != nil {
			t.Fatalf("CloseDB_DML returned error: %v", err)
		}

		var result map[string]interface{}
		err = json.Unmarshal([]byte(resultJSON), &result)
		if err != nil {
			t.Fatalf("Failed to parse result JSON: %v", err)
		}

		if result["status"] != "success" {
			t.Errorf("Expected status=success even when no DB open, got status=%v, errors=%v", result["status"], result["errors"])
		}
	})
}
