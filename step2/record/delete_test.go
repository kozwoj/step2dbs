package record

import (
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"

	"github.com/kozwoj/step2/db"
)

// TestDeleteRecord_BasicDeletion tests basic record deletion
func TestDeleteRecord_BasicDeletion(t *testing.T) {
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

	// Add a record
	t.Run("Add record", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Small_int_value":      float64(100),
			"Integer_value":        float64(1000),
			"Big_int_value":        float64(100000),
			"Decimal_value":        "123.45",
			"Float_value":          3.14159,
			"String_size_value":    "Test string",
			"String_no_size_value": "Another string",
			"Char_array_value":     "CHAR0000000TEST",
			"Boolean_value":        true,
			"Date_value":           "2024-01-15",
			"Time_value":           "9:30:00.000",
		}

		recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add record: %v", err)
		}
		if recordID != 1 {
			t.Errorf("Expected recordID 1, got %d", recordID)
		}
		t.Logf("Added record with recordID=%d", recordID)
	})

	// Verify record exists
	t.Run("Verify record exists before deletion", func(t *testing.T) {
		record, err := GetRecordByID("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("GetRecordByID failed: %v", err)
		}

		if record["Integer_value"].(int) != 1000 {
			t.Errorf("Expected Integer_value 1000, got %v", record["Integer_value"])
		}
		t.Log("Record exists and is readable")
	})

	// Delete the record
	t.Run("Delete record", func(t *testing.T) {
		err := DeleteRecord("AllTypes", 1, dbDef)
		if err != nil {
			t.Fatalf("DeleteRecord failed: %v", err)
		}
		t.Log("Successfully deleted record")
	})

	// Verify record is deleted
	t.Run("Verify record is deleted", func(t *testing.T) {
		_, err := GetRecordByID("AllTypes", 1, dbDef)
		if err == nil {
			t.Error("Expected error when reading deleted record, but got none")
		} else if err.Error() != "record 1 in table AllTypes is deleted" {
			t.Errorf("Expected 'record is deleted' error, got: %v", err)
		} else {
			t.Logf("Correctly rejected reading deleted record with error: %v", err)
		}
	})

	// Try to delete again (should fail)
	t.Run("Try to delete already deleted record", func(t *testing.T) {
		err := DeleteRecord("AllTypes", 1, dbDef)
		if err == nil {
			t.Error("Expected error when deleting already deleted record")
		} else {
			t.Logf("Correctly rejected deleting already deleted record with error: %v", err)
		}
	})
}

// TestDeleteRecord_WithPrimaryKey tests deletion with primary key index removal
func TestDeleteRecord_WithPrimaryKey(t *testing.T) {
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

	// Add multiple records
	t.Run("Add multiple records", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			recordFields := map[string]interface{}{
				"Small_int_value":      float64(i * 100),
				"Integer_value":        float64(i * 1000),
				"Big_int_value":        float64(i * 100000),
				"Decimal_value":        "123.45",
				"Float_value":          3.14,
				"String_size_value":    "Test",
				"String_no_size_value": "Test",
				"Char_array_value":     "CHAR0000000TEST",
				"Boolean_value":        true,
				"Date_value":           "2024-01-15",
				"Time_value":           "9:30:00.000",
			}

			recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add record %d: %v", i, err)
			}
			t.Logf("Added record %d with recordID=%d", i, recordID)
		}
	})

	// Verify records exist in primary index
	t.Run("Verify records in primary index", func(t *testing.T) {
		tableIdx := dbDef.TableIndex["AllTypes"]
		table := dbDef.Tables[tableIdx]

		// Check primary index contains the keys
		if table.PrimeIndex == nil {
			t.Fatal("Primary index not initialized")
		}

		for i := 1; i <= 3; i++ {
			key := int32(i * 1000)
			value, err := table.PrimeIndex.Find(key)
			if err != nil {
				t.Errorf("Failed to find key %d in primary index: %v", key, err)
			} else {
				recordID := binary.LittleEndian.Uint32(value)
				if recordID != uint32(i) {
					t.Errorf("Expected recordID %d for key %d, got %d", i, key, recordID)
				}
			}
		}
		t.Log("All records found in primary index")
	})

	// Delete middle record
	t.Run("Delete middle record", func(t *testing.T) {
		err := DeleteRecord("AllTypes", 2, dbDef)
		if err != nil {
			t.Fatalf("DeleteRecord failed: %v", err)
		}
		t.Log("Successfully deleted record 2")
	})

	// Verify deleted record not in primary index
	t.Run("Verify deleted record removed from primary index", func(t *testing.T) {
		tableIdx := dbDef.TableIndex["AllTypes"]
		table := dbDef.Tables[tableIdx]

		// Try to find the deleted key
		key := int32(2000)
		_, err := table.PrimeIndex.Find(key)
		if err == nil {
			t.Error("Expected error when finding deleted key in primary index")
		} else {
			t.Logf("Correctly reported key not found in primary index: %v", err)
		}
	})

	// Verify other records still exist in primary index
	t.Run("Verify other records still in primary index", func(t *testing.T) {
		tableIdx := dbDef.TableIndex["AllTypes"]
		table := dbDef.Tables[tableIdx]

		for _, i := range []int{1, 3} {
			key := int32(i * 1000)
			value, err := table.PrimeIndex.Find(key)
			if err != nil {
				t.Errorf("Failed to find key %d in primary index: %v", key, err)
			} else {
				recordID := binary.LittleEndian.Uint32(value)
				if recordID != uint32(i) {
					t.Errorf("Expected recordID %d for key %d, got %d", i, key, recordID)
				}
			}
		}
		t.Log("Other records still accessible in primary index")
	})
}

// TestDeleteRecord_WithStringFields tests deletion with STRING field dictionary cleanup
func TestDeleteRecord_WithStringFields(t *testing.T) {
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

	// Add departments
	t.Run("Add departments", func(t *testing.T) {
		departments := []struct {
			name string
			code string
		}{
			{"Computer Science", "COMPSCI "},
			{"Mathematics", "MATH    "},
			{"Physics", "PHYSICS "},
		}

		for i, dept := range departments {
			recordFields := map[string]interface{}{
				"Name":            dept.name,
				"Department_code": dept.code,
				"Building_name":   "Science Hall",
				"Building_code":   "SCI0001H",
			}

			recordID, err := AddNewRecord("Departments", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add department %d: %v", i+1, err)
			}
			t.Logf("Added department '%s' with recordID=%d", dept.name, recordID)
		}
	})

	// Delete middle department
	t.Run("Delete department", func(t *testing.T) {
		err := DeleteRecord("Departments", 2, dbDef)
		if err != nil {
			t.Fatalf("DeleteRecord failed: %v", err)
		}
		t.Log("Successfully deleted department record 2")
	})

	// Verify deleted record cannot be retrieved
	t.Run("Verify deleted record not accessible", func(t *testing.T) {
		_, err := GetRecordByID("Departments", 2, dbDef)
		if err == nil {
			t.Error("Expected error when reading deleted record")
		} else {
			t.Logf("Correctly rejected reading deleted record: %v", err)
		}
	})

	// Verify other records still accessible
	t.Run("Verify other records still accessible", func(t *testing.T) {
		for _, recordID := range []uint32{1, 3} {
			_, err := GetRecordByID("Departments", recordID, dbDef)
			if err != nil {
				t.Errorf("Failed to read record %d: %v", recordID, err)
			} else {
				t.Logf("Successfully read record %d", recordID)
			}
		}
	})
}

// TestDeleteRecord_WithSets tests deletion of records with set members
func TestDeleteRecord_WithSets(t *testing.T) {
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

	// Add a department (owner)
	t.Run("Add department", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Name":            "Computer Science",
			"Department_code": "COMPSCI ",
			"Building_name":   "Engineering Hall",
			"Building_code":   "ENG0001H",
		}

		recordID, err := AddNewRecord("Departments", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add department: %v", err)
		}
		t.Logf("Added department with recordID=%d", recordID)
	})

	// Add teachers (members)
	t.Run("Add teachers", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			recordFields := map[string]interface{}{
				"Employee_id":   fmt.Sprintf("EMP0000%d", i),
				"First_name":    "Teacher",
				"Last_name":     fmt.Sprintf("Name%d", i),
				"Building_code": "ENG0001H",
				"Office":        fmt.Sprintf("Room 10%d  ", i),
				"Works_for":     "COMPSCI ",
			}

			recordID, err := AddNewRecord("Teachers", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add teacher %d: %v", i, err)
			}
			t.Logf("Added teacher %d with recordID=%d", i, recordID)
		}
	})

	// Add teachers to department's Faculty set
	t.Run("Add teachers to department set", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			err := AddSetMember("Departments", 1, "Faculty", uint32(i), dbDef)
			if err != nil {
				t.Fatalf("Failed to add teacher %d to set: %v", i, err)
			}
		}
		t.Log("Added all teachers to department's Faculty set")
	})

	// Verify set has members
	t.Run("Verify set has members", func(t *testing.T) {
		members, err := GetSetMembers("Departments", 1, "Faculty", dbDef)
		if err != nil {
			t.Fatalf("GetSetMembers failed: %v", err)
		}

		if len(members) != 3 {
			t.Errorf("Expected 3 members, got %d", len(members))
		}
		t.Logf("Department set has %d members", len(members))
	})

	// Delete the department (should clean up all sets)
	t.Run("Delete department with sets", func(t *testing.T) {
		err := DeleteRecord("Departments", 1, dbDef)
		if err != nil {
			t.Fatalf("DeleteRecord failed: %v", err)
		}
		t.Log("Successfully deleted department with sets")
	})

	// Verify department is deleted
	t.Run("Verify department is deleted", func(t *testing.T) {
		_, err := GetRecordByID("Departments", 1, dbDef)
		if err == nil {
			t.Error("Expected error when reading deleted department")
		} else {
			t.Logf("Correctly rejected reading deleted department: %v", err)
		}
	})

	// Verify teachers still exist (members are not deleted when owner is deleted)
	t.Run("Verify teachers still exist", func(t *testing.T) {
		for i := 1; i <= 3; i++ {
			_, err := GetRecordByID("Teachers", uint32(i), dbDef)
			if err != nil {
				t.Errorf("Failed to read teacher %d: %v", i, err)
			} else {
				t.Logf("Teacher %d still exists", i)
			}
		}
	})
}

// TestDeleteRecord_ValidationErrors tests error cases
func TestDeleteRecord_ValidationErrors(t *testing.T) {
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

	// Add a record
	recordFields := map[string]interface{}{
		"Small_int_value":      float64(100),
		"Integer_value":        float64(1000),
		"Big_int_value":        float64(100000),
		"Decimal_value":        "123.45",
		"Float_value":          3.14,
		"String_size_value":    "Test",
		"String_no_size_value": "Test",
		"Char_array_value":     "CHAR0000000TEST",
		"Boolean_value":        true,
		"Date_value":           "2024-01-15",
		"Time_value":           "9:30:00.000",
	}

	recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
	if err != nil {
		t.Fatalf("Failed to add record: %v", err)
	}
	t.Logf("Added record with recordID=%d", recordID)

	// Test non-existent table
	t.Run("Non-existent table", func(t *testing.T) {
		err := DeleteRecord("NonExistentTable", 1, dbDef)
		if err == nil {
			t.Error("Expected error for non-existent table")
		} else {
			t.Logf("Correctly rejected non-existent table: %v", err)
		}
	})

	// Test non-existent record
	t.Run("Non-existent record", func(t *testing.T) {
		err := DeleteRecord("AllTypes", 999, dbDef)
		if err == nil {
			t.Error("Expected error for non-existent record")
		} else {
			t.Logf("Correctly rejected non-existent record: %v", err)
		}
	})

	// Test missing tableName
	t.Run("Missing tableName", func(t *testing.T) {
		err := DeleteRecord("", 1, dbDef)
		if err == nil {
			t.Error("Expected error for missing tableName")
		} else {
			t.Logf("Correctly rejected missing tableName: %v", err)
		}
	})

	// Test missing recordID (recordID 0)
	t.Run("Missing recordID", func(t *testing.T) {
		err := DeleteRecord("AllTypes", 0, dbDef)
		if err == nil {
			t.Error("Expected error for missing recordID")
		} else {
			t.Logf("Correctly rejected missing recordID: %v", err)
		}
	})
}
