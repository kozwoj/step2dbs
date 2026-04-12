package record

import (
	"encoding/json"
	"path/filepath"
	"github.com/kozwoj/step2/db"
	"testing"
)

// TestValidateRecord tests the ValidateRecord function using the AllTypes table schema
func TestValidateRecord(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the AllTypes.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "AllTypes.ddl")

	// Step 1: Create the database
	t.Log("Creating database from AllTypes.ddl schema...")
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Step 2: Open the database
	dbDir := filepath.Join(tempDir, "AllTypesTes")
	t.Log("Opening database...")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Step 3: Get the AllTypes table description
	dbDef := db.Definition()
	tableIdx, exists := dbDef.TableIndex["AllTypes"]
	if !exists {
		t.Fatalf("AllTypes table not found in database")
	}
	tableDesc := dbDef.Tables[tableIdx]

	// Test cases for valid records
	validTests := []struct {
		name       string
		jsonRecord string
		wantErr    bool
	}{
		{
			name: "All fields valid - full format",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 9223372036854775807,
				"Decimal_value": "123.45",
				"Float_value": 3.14159,
				"String_size_value": "Test string",
				"String_no_size_value": "This is a longer string with no size limit",
				"Char_array_value": "Exactly15chars!",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
			wantErr: false,
		},
		{
			name: "Minimum values",
			jsonRecord: `{
				"Small_int_value": -32768,
				"Integer_value": -2147483648,
				"Big_int_value": -9223372036854775808,
				"Decimal_value": "0.00",
				"Float_value": 0.0,
				"String_size_value": "",
				"String_no_size_value": "",
				"Char_array_value": "000000000000000",
				"Boolean_value": false,
				"Date_value": "2000-01-01",
				"Time_value": "0:00"
			}`,
			wantErr: false,
		},
		{
			name: "Maximum values",
			jsonRecord: `{
				"Small_int_value": 32767,
				"Integer_value": 2147483647,
				"Big_int_value": 9223372036854775807,
				"Decimal_value": "999999999.999",
				"Float_value": 1.7976931348623157e+308,
				"String_size_value": "This is exactly sixty characters long for testing purposes!",
				"String_no_size_value": "This string can be as long as needed without any size restrictions",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "2099-12-31",
				"Time_value": "23:59:59.999"
			}`,
			wantErr: false,
		},
		{
			name: "Time with seconds only (H:MM:SS)",
			jsonRecord: `{
				"Small_int_value": 1,
				"Integer_value": 1,
				"Big_int_value": 1,
				"Decimal_value": "1.00",
				"Float_value": 1.0,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "test12345678901",
				"Boolean_value": true,
				"Date_value": "2024-01-01",
				"Time_value": "14:30:45"
			}`,
			wantErr: false,
		},
		{
			name: "Time with minutes only (H:MM)",
			jsonRecord: `{
				"Small_int_value": 1,
				"Integer_value": 1,
				"Big_int_value": 1,
				"Decimal_value": "1.00",
				"Float_value": 1.0,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "test12345678901",
				"Boolean_value": true,
				"Date_value": "2024-01-01",
				"Time_value": "14:30"
			}`,
			wantErr: false,
		},
	}

	for _, tt := range validTests {
		t.Run(tt.name, func(t *testing.T) {
			// Unmarshal JSON to map
			var recordMap map[string]interface{}
			err := json.Unmarshal([]byte(tt.jsonRecord), &recordMap)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			fieldValues, err := ValidateRecord(recordMap, tableDesc)
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateRecord() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !tt.wantErr && fieldValues == nil {
				t.Error("ValidateRecord() returned nil fieldValues for valid record")
			}
			if !tt.wantErr && len(fieldValues) != len(tableDesc.RecordLayout.Fields) {
				t.Errorf("ValidateRecord() returned %d field values, want %d", len(fieldValues), len(tableDesc.RecordLayout.Fields))
			}
		})
	}

	// Test cases for invalid records
	invalidTests := []struct {
		name       string
		jsonRecord string
	}{
		{
			name: "Missing required field",
			jsonRecord: `{
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
		},
		{
			name: "String too long",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "This string is definitely longer than 60 characters and should fail validation",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
		},
		{
			name: "Char array wrong length",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "TooShort",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
		},
		{
			name: "Invalid date format",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "06/15/2024",
				"Time_value": "14:30:45.123"
			}`,
		},
		{
			name: "Invalid time format",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45:123"
			}`,
		},
		{
			name: "Wrong type for integer",
			jsonRecord: `{
				"Small_int_value": "not a number",
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
		},
		{
			name: "Wrong type for boolean",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "123.45",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": "yes",
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
		},
		{
			name: "Invalid decimal format",
			jsonRecord: `{
				"Small_int_value": 100,
				"Integer_value": 50000,
				"Big_int_value": 1000,
				"Decimal_value": "not-a-decimal",
				"Float_value": 3.14,
				"String_size_value": "test",
				"String_no_size_value": "test",
				"Char_array_value": "123456789012345",
				"Boolean_value": true,
				"Date_value": "2024-06-15",
				"Time_value": "14:30:45.123"
			}`,
		},
	}

	for _, tt := range invalidTests {
		t.Run(tt.name, func(t *testing.T) {
			// Unmarshal JSON to map
			var recordMap map[string]interface{}
			err := json.Unmarshal([]byte(tt.jsonRecord), &recordMap)
			if err != nil {
				t.Fatalf("Failed to unmarshal JSON: %v", err)
			}

			_, err = ValidateRecord(recordMap, tableDesc)
			if err == nil {
				t.Error("ValidateRecord() expected error for invalid record, got nil")
			}
		})
	}
}

// TestValidateRecord_PrimaryKeyTypeCheck tests that ValidateRecord validates primary key field type consistency
func TestValidateRecord_PrimaryKeyTypeCheck(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file which has tables with primary keys
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Create the database
	t.Log("Creating database from College.ddl schema...")
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Open the database
	dbDir := filepath.Join(tempDir, "college")
	t.Log("Opening database...")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Get the database definition
	dbDef := db.Definition()

	// Test with Departments table (has CHAR[8] primary key: Department_code)
	t.Run("Valid CHAR primary key", func(t *testing.T) {
		tableIdx, exists := dbDef.TableIndex["Departments"]
		if !exists {
			t.Fatal("Departments table not found")
		}
		tableDesc := dbDef.Tables[tableIdx]

		recordMap := map[string]interface{}{
			"Name":            "Computer Science",
			"Department_code": "CS00001H", // CHAR[8] primary key
			"Building_name":   "Engineering",
			"Building_code":   "ENG0001H",
		}

		_, err := ValidateRecord(recordMap, tableDesc)
		if err != nil {
			t.Errorf("ValidateRecord failed for valid CHAR primary key: %v", err)
		}
	})

	// Test with Teachers table (has CHAR[8] primary key: Employee_id)
	t.Run("Valid CHAR primary key - Teachers", func(t *testing.T) {
		tableIdx, exists := dbDef.TableIndex["Teachers"]
		if !exists {
			t.Fatal("Teachers table not found")
		}
		tableDesc := dbDef.Tables[tableIdx]

		recordMap := map[string]interface{}{
			"Employee_id":   "T0000001", // CHAR[8] primary key
			"First_name":    "John",
			"Last_name":     "Smith",
			"Building_code": "ENG0001H",
			"Office":        "Room 301  ",
			"Works_for":     "CS00001H",
		}

		_, err := ValidateRecord(recordMap, tableDesc)
		if err != nil {
			t.Errorf("ValidateRecord failed for valid CHAR primary key: %v", err)
		}
	})

	// Test with wrong length CHAR primary key (should fail during field validation, not codec validation)
	t.Run("Invalid CHAR primary key length", func(t *testing.T) {
		tableIdx, exists := dbDef.TableIndex["Departments"]
		if !exists {
			t.Fatal("Departments table not found")
		}
		tableDesc := dbDef.Tables[tableIdx]

		recordMap := map[string]interface{}{
			"Name":            "Computer Science",
			"Department_code": "CS001", // Wrong length - should be CHAR[8]
			"Building_name":   "Engineering",
			"Building_code":   "ENG0001H",
		}

		_, err := ValidateRecord(recordMap, tableDesc)
		if err == nil {
			t.Error("ValidateRecord should have failed for wrong length CHAR primary key")
		}
	})
}
