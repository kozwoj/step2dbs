package record

import (
	"path/filepath"
	"testing"

	"github.com/kozwoj/step2/db"
)

// TestUpdateRecord_BasicFields tests updating various field types in the AllTypes table
func TestUpdateRecord_BasicFields(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the AllTypes.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	// Create the database
	t.Log("Creating database from AllTypes.ddl schema...")
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Open the database
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	t.Log("Opening database...")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Get the database definition
	dbDef := db.Definition()

	// Add an initial record
	t.Run("Add initial record", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Small_int_value":      float64(100),
			"Integer_value":        float64(1000),
			"Big_int_value":        float64(100000),
			"Decimal_value":        "123.45",
			"Float_value":          3.14159,
			"String_size_value":    "Original string",
			"String_no_size_value": "Another original",
			"Char_array_value":     "CHAR000000000AB",
			"Boolean_value":        true,
			"Date_value":           "2024-01-15",
			"Time_value":           "9:30:00.000",
		}

		recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add initial record: %v", err)
		}
		if recordID != 1 {
			t.Errorf("Expected recordID 1, got %d", recordID)
		}
		t.Logf("Added initial record with recordID=%d", recordID)
	})

	// Update multiple fields
	t.Run("Update multiple basic fields", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Small_int_value": float64(200),
			"Big_int_value":   float64(200000),
			"Float_value":     2.71828,
			"Boolean_value":   false,
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed: %v", err)
		}
		t.Log("Successfully updated multiple fields")

		// Verify the update
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		// Verify updated values
		if record["Small_int_value"].(int) != 200 {
			t.Errorf("Small_int_value: expected 200, got %v", record["Small_int_value"])
		}
		if record["Integer_value"].(int) != 1000 { // primary key should not change
			t.Errorf("Integer_value: expected 1000, got %v", record["Integer_value"])
		}
		if record["Big_int_value"].(int64) != 200000 {
			t.Errorf("Big_int_value: expected 200000, got %v", record["Big_int_value"])
		}
		if record["Float_value"].(float64) != 2.71828 {
			t.Errorf("Float_value: expected 2.71828, got %v", record["Float_value"])
		}
		if record["Boolean_value"].(bool) != false {
			t.Errorf("Boolean_value: expected false, got %v", record["Boolean_value"])
		}

		// Verify unchanged values
		if record["String_size_value"].(string) != "Original string" {
			t.Errorf("String_size_value should remain unchanged, got %v", record["String_size_value"])
		}

		t.Log("Verified all field updates")
	})

	// Update DATE field
	t.Run("Update DATE field", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Date_value": "2025-12-25",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed for DATE: %v", err)
		}

		// Verify
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Date_value"].(string) != "2025-12-25" {
			t.Errorf("Date_value: expected 2025-12-25, got %v", record["Date_value"])
		}
		t.Log("DATE field updated successfully")
	})

	// Update TIME field
	t.Run("Update TIME field", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Time_value": "14:45:30.500",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed for TIME: %v", err)
		}

		// Verify
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Time_value"].(string) != "14:45:30.500" {
			t.Errorf("Time_value: expected 14:45:30.500, got %v", record["Time_value"])
		}
		t.Log("TIME field updated successfully")
	})

	// Update DECIMAL field
	t.Run("Update DECIMAL field", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Decimal_value": "999.99",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed for DECIMAL: %v", err)
		}

		// Verify
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Decimal_value"].(string) != "999.99" {
			t.Errorf("Decimal_value: expected 999.99, got %v", record["Decimal_value"])
		}
		t.Log("DECIMAL field updated successfully")
	})

	// Update STRING field (tests dictionary/postings)
	t.Run("Update STRING field", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"String_size_value":    "Updated string value",
			"String_no_size_value": "Also updated",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed for STRING: %v", err)
		}

		// Verify
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["String_size_value"].(string) != "Updated string value" {
			t.Errorf("String_size_value: expected 'Updated string value', got %v", record["String_size_value"])
		}
		if record["String_no_size_value"].(string) != "Also updated" {
			t.Errorf("String_no_size_value: expected 'Also updated', got %v", record["String_no_size_value"])
		}
		t.Log("STRING fields updated successfully")
	})

	// Update CHAR field
	t.Run("Update CHAR field", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Char_array_value": "NEWCHARVALUE123",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed for CHAR: %v", err)
		}

		// Verify
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Char_array_value"].(string) != "NEWCHARVALUE123" {
			t.Errorf("Char_array_value: expected 'NEWCHARVALUE123', got %v", record["Char_array_value"])
		}
		t.Log("CHAR field updated successfully")
	})
}

// TestUpdateRecord_PrimaryKeyValidation tests that primary key cannot be changed
func TestUpdateRecord_PrimaryKeyValidation(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the AllTypes.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	// Create the database
	t.Log("Creating database from AllTypes.ddl schema...")
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Open the database
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	t.Log("Opening database...")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Get the database definition
	dbDef := db.Definition()

	// Add an initial record
	recordFields := map[string]interface{}{
		"Small_int_value":      float64(100),
		"Integer_value":        float64(1000),
		"Big_int_value":        float64(100000),
		"Decimal_value":        "100.00",
		"Float_value":          3.14,
		"String_size_value":    "Test",
		"String_no_size_value": "Test",
		"Char_array_value":     "CHAR0000000TEST",
		"Boolean_value":        true,
		"Date_value":           "2024-01-01",
		"Time_value":           "10:00:00.000",
	}

	recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
	if err != nil {
		t.Fatalf("Failed to add initial record: %v", err)
	}
	t.Logf("Added initial record with recordID=%d", recordID)

	// Try to update primary key (should fail)
	t.Run("Attempt to change primary key", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Integer_value":   float64(2000),
			"Small_int_value": float64(200),
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err == nil {
			t.Fatal("Expected error when changing primary key, but got none")
		}

		// Verify error message mentions primary key
		errMsg := err.Error()
		if errMsg != "primary key field 'Integer_value' cannot be changed" {
			t.Errorf("Expected primary key error message, got: %v", errMsg)
		}
		t.Logf("Correctly rejected primary key change with error: %v", err)
	})

	// Verify record is unchanged
	t.Run("Verify record remains unchanged", func(t *testing.T) {
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Integer_value"].(int) != 1000 {
			t.Errorf("Primary key should remain 1000, got %v", record["Integer_value"])
		}
		if record["Small_int_value"].(int) != 100 {
			t.Errorf("Small_int_value should remain 100, got %v", record["Small_int_value"])
		}
		t.Log("Record unchanged after failed primary key update")
	})
}

// TestUpdateRecord_ValidationErrors tests various validation error cases
func TestUpdateRecord_ValidationErrors(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

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

	dbDef := db.Definition()

	// Add an initial record
	recordFields := map[string]interface{}{
		"Small_int_value":      float64(100),
		"Integer_value":        float64(1000),
		"Big_int_value":        float64(100000),
		"Decimal_value":        "100.00",
		"Float_value":          3.14,
		"String_size_value":    "Test",
		"String_no_size_value": "Test",
		"Char_array_value":     "CHAR0000000TEST",
		"Boolean_value":        true,
		"Date_value":           "2024-01-01",
		"Time_value":           "10:00:00.000",
	}

	recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
	if err != nil {
		t.Fatalf("Failed to add initial record: %v", err)
	}
	t.Logf("Added initial record with recordID=%d", recordID)

	// Test invalid DATE format
	t.Run("Invalid DATE format", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Date_value": "2024/01/01",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err == nil {
			t.Error("Expected error for invalid DATE format")
		} else {
			t.Logf("Correctly rejected invalid DATE with error: %v", err)
		}
	})

	// Test invalid TIME format
	t.Run("Invalid TIME format", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Time_value": "invalid_time",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err == nil {
			t.Error("Expected error for invalid TIME format")
		} else {
			t.Logf("Correctly rejected invalid TIME with error: %v", err)
		}
	})

	// Test invalid DECIMAL format
	t.Run("Invalid DECIMAL format", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Decimal_value": "not-a-number",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err == nil {
			t.Error("Expected error for invalid DECIMAL format")
		} else {
			t.Logf("Correctly rejected invalid DECIMAL with error: %v", err)
		}
	})

	// Test invalid CHAR length
	t.Run("Invalid CHAR length", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Char_array_value": "TOO_SHORT",
		}

		err := UpdateRecord("AllTypes", 1, recordFields, dbDef)
		if err == nil {
			t.Error("Expected error for invalid CHAR length")
		} else {
			t.Logf("Correctly rejected invalid CHAR length with error: %v", err)
		}
	})

	// Test non-existent record
	t.Run("Non-existent record", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"String_size_value": "Test",
		}

		err := UpdateRecord("AllTypes", 999, recordFields, dbDef)
		if err == nil {
			t.Error("Expected error for non-existent record")
		} else {
			t.Logf("Correctly rejected non-existent record with error: %v", err)
		}
	})
}

// TestUpdateRecord_StringFieldsWithDictionary tests STRING field updates and verifies dictionary/postings management
func TestUpdateRecord_StringFieldsWithDictionary(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "College")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	dbDef := db.Definition()

	// Add initial department record
	t.Run("Add initial record", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Name":            "Computer Science",
			"Department_code": "COMPSCI ",
			"Building_name":   "Engineering Hall",
			"Building_code":   "ENG0001H",
		}

		recordID, err := AddNewRecord("Departments", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add record: %v", err)
		}
		if recordID != 1 {
			t.Errorf("Expected recordID 1, got %d", recordID)
		}
		t.Logf("Added department with recordID=%d", recordID)
	})

	// Update STRING field
	t.Run("Update department name", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Name": "Computer Science and Engineering",
		}

		err := UpdateRecord("Departments", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed: %v", err)
		}
		t.Log("Updated department name")

		// Verify the update
		record, err := GetRecordByID("Departments", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Name"].(string) != "Computer Science and Engineering" {
			t.Errorf("Name: expected 'Computer Science and Engineering', got %v", record["Name"])
		}
		t.Log("Verified department name update")
	})

	// Update building name
	t.Run("Update building name", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Building_name": "New Science Building",
		}

		err := UpdateRecord("Departments", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed: %v", err)
		}
		t.Log("Updated building name")

		// Verify
		record, err := GetRecordByID("Departments", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Building_name"].(string) != "New Science Building" {
			t.Errorf("Building_name: expected 'New Science Building', got %v", record["Building_name"])
		}
		// Verify other fields unchanged
		if record["Name"].(string) != "Computer Science and Engineering" {
			t.Errorf("Name should remain unchanged, got %v", record["Name"])
		}
		t.Log("Verified building name update and Name field unchanged")
	})
}

// TestUpdateRecord_ForeignKeyValidation tests foreign key constraint validation during updates
func TestUpdateRecord_ForeignKeyValidation(t *testing.T) {
	// Setup
	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "college")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	dbDef := db.Definition()

	// Add a department (referenced table)
	t.Run("Add department", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Name":            "Mathematics",
			"Department_code": "MATH    ",
			"Building_name":   "Science Hall",
			"Building_code":   "SCI0001H",
		}

		recordID, err := AddNewRecord("Departments", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add department: %v", err)
		}
		t.Logf("Added department with recordID=%d", recordID)
	})

	// Add a teacher (with foreign key to department)
	t.Run("Add teacher", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Employee_id":   "EMP00001",
			"First_name":    "John",
			"Last_name":     "Smith",
			"Building_code": "SCI0001H",
			"Office":        "Room 101  ",
			"Works_for":     "MATH    ",
		}

		recordID, err := AddNewRecord("Teachers", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add teacher: %v", err)
		}
		t.Logf("Added teacher with recordID=%d", recordID)
	})

	// Update teacher's foreign key to valid department (should succeed)
	t.Run("Update to valid foreign key", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Works_for": "MATH    ",
			"Office":    "Room 102  ",
		}

		err := UpdateRecord("Teachers", 1, recordFields, dbDef)
		if err != nil {
			t.Fatalf("UpdateRecord failed: %v", err)
		}
		t.Log("Successfully updated teacher with valid foreign key")
	})

	// Try to update teacher's foreign key to invalid department (should fail)
	t.Run("Update to invalid foreign key", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Works_for": "INVALID ",
		}

		err := UpdateRecord("Teachers", 1, recordFields, dbDef)
		if err == nil {
			t.Fatal("Expected error for invalid foreign key, but got none")
		}
		t.Logf("Correctly rejected invalid foreign key with error: %v", err)

		// Verify record unchanged
		record, err := GetRecordByID("Teachers", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Works_for"].(string) != "MATH    " {
			t.Errorf("Works_for should remain 'MATH    ', got %v", record["Works_for"])
		}
	})
}
