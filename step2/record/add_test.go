package record

import (
	"encoding/binary"
	"path/filepath"
	"github.com/kozwoj/step2/db"
	"testing"
)

// TestAddNewRecord_Departments tests adding department records to the College database
// and verifying they can be found via the primary index.
func TestAddNewRecord_Departments(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
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

	// Department records to insert
	departments := []struct {
		name         string
		code         string
		buildingName string
		buildingCode string
	}{
		{"Computer Science", "COMPSCI ", "Engineering Building", "ENG0001H"},
		{"Mathematics", "MATH    ", "Science Hall", "SCI0001H"},
		{"Physics", "PHYSICS ", "Science Hall", "SCI0001H"},
	}

	// Track inserted recordIDs for verification
	recordIDs := make([]uint32, 0, len(departments))

	t.Run("Add department records", func(t *testing.T) {
		for i, dept := range departments {
			recordFields := map[string]interface{}{
				"Name":            dept.name,
				"Department_code": dept.code,
				"Building_name":   dept.buildingName,
				"Building_code":   dept.buildingCode,
			}

			recordID, err := AddNewRecord("Departments", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add department %d (%s): %v", i, dept.name, err)
			}

			if recordID == 0 {
				t.Errorf("Department %d (%s): got invalid recordID 0", i, dept.name)
			}

			recordIDs = append(recordIDs, recordID)
			t.Logf("Added department %d: %s (code=%s) with recordID=%d", i, dept.name, dept.code, recordID)
		}

		// Verify we got sequential recordIDs
		if len(recordIDs) == 3 {
			if recordIDs[0] != 1 || recordIDs[1] != 2 || recordIDs[2] != 3 {
				t.Errorf("Expected sequential recordIDs 1,2,3, got %v", recordIDs)
			}
		}
	})

	t.Run("Verify departments via primary index", func(t *testing.T) {
		// Get Departments table
		tableIdx, exists := dbDef.TableIndex["Departments"]
		if !exists {
			t.Fatal("Departments table not found")
		}
		deptTable := dbDef.Tables[tableIdx]

		// Verify primary index exists
		if deptTable.PrimeIndex == nil {
			t.Fatal("Departments primary index not initialized")
		}

		// Look up each department by its primary key
		for i, dept := range departments {
			// Convert Department_code to []byte (CHAR[8] uses []byte keys)
			deptCode := []byte(dept.code)

			// Find in primary index
			value, err := deptTable.PrimeIndex.Find(deptCode)
			if err != nil {
				t.Errorf("Failed to find department %d (%s) in primary index: %v", i, dept.name, err)
				continue
			}

			// Convert the returned value to recordID
			recordIDFromIndex := binary.LittleEndian.Uint32(value)

			// The value should be 4 bytes containing the recordID
			if len(value) != 4 {
				t.Errorf("Department %d (%s): expected 4-byte value, got %d bytes", i, dept.name, len(value))
			} else {
				t.Logf("Found department %d (%s) in primary index with code=%s, recordID=%d", i, dept.name, dept.code, recordIDFromIndex)
			}
		}
	})

	t.Run("Attempt to add duplicate department code - should fail", func(t *testing.T) {
		// Try to add a department with duplicate code "COMPSCI "
		recordFields := map[string]interface{}{
			"Name":            "Duplicate CS Dept",
			"Department_code": "COMPSCI ",
			"Building_name":   "Another Building",
			"Building_code":   "XYZ0001H",
		}

		_, err := AddNewRecord("Departments", recordFields, dbDef)
		if err == nil {
			t.Error("Expected duplicate primary key error, but AddNewRecord succeeded")
		} else {
			t.Logf("Correctly rejected duplicate department code: %v", err)
		}
	})
}

// TestAddNewRecord_ForeignKeys tests foreign key constraint validation when adding records.
// Uses the College database schema with Departments -> Teachers -> Students relationships.
func TestAddNewRecord_ForeignKeys(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Create and open the database
	t.Log("Creating database from College.ddl schema...")
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "college")
	t.Log("Opening database...")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	dbDef := db.Definition()

	// Step 1: Add a department (required for teacher foreign key)
	t.Run("Add Computer Science department", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Name":            "Computer Science",
			"Department_code": "COMPSCI ",
			"Building_name":   "Engineering Building",
			"Building_code":   "ENG0001H",
		}

		recordID, err := AddNewRecord("Departments", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add department: %v", err)
		}
		t.Logf("Added department with recordID=%d", recordID)
	})

	// Step 2: Try to add teacher with invalid foreign key (should fail)
	t.Run("Add teacher with invalid foreign key - should fail", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Employee_id":   "T0000001",
			"First_name":    "John",
			"Last_name":     "Smith",
			"Building_code": "ENG0001H",
			"Office":        "Room 301  ",
			"Works_for":     "INVALID ",
		}

		_, err := AddNewRecord("Teachers", recordFields, dbDef)
		if err == nil {
			t.Error("Expected foreign key constraint error, but AddNewRecord succeeded")
		} else {
			t.Logf("Correctly rejected invalid foreign key: %v", err)
		}
	})

	// Step 3: Add teacher with valid foreign key (should succeed)
	t.Run("Add teacher with valid foreign key - should succeed", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Employee_id":   "T0000001",
			"First_name":    "John",
			"Last_name":     "Smith",
			"Building_code": "ENG0001H",
			"Office":        "Room 301  ",
			"Works_for":     "COMPSCI ",
		}

		recordID, err := AddNewRecord("Teachers", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add teacher with valid foreign key: %v", err)
		}
		t.Logf("Successfully added teacher with recordID=%d", recordID)
	})

	// Step 4: Try to add student with invalid advisor foreign key (should fail)
	t.Run("Add student with invalid advisor FK - should fail", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"First_name":  "Jane",
			"Last_name":   "Doe",
			"Gender":      "F",
			"Birth_date":  "2004-05-12",
			"State_or_Country": "Idaho",
			"Start_date":  "2023-09-01",
			"Student_id":  "S000000001",
			"Major":       "Computer Science         ",
			"Advisor":     "T9999999",
			"Year":        float64(2),
			"Credits":     float64(45),
		}

		_, err := AddNewRecord("Students", recordFields, dbDef)
		if err == nil {
			t.Error("Expected foreign key constraint error for invalid advisor, but AddNewRecord succeeded")
		} else {
			t.Logf("Correctly rejected invalid advisor FK: %v", err)
		}
	})

	// Step 5: Add student with valid advisor foreign key (should succeed)
	t.Run("Add student with valid advisor FK - should succeed", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"First_name":  "Jane",
			"Last_name":   "Doe",
			"Gender":      "F",
			"Birth_date":  "2004-05-12",
			"State_or_Country": "Idaho",
			"Start_date":  "2023-09-01",
			"Student_id":  "S000000001",
			"Major":       "Computer Science         ",
			"Advisor":     "T0000001",
			"Year":        float64(2),
			"Credits":     float64(45),
		}

		recordID, err := AddNewRecord("Students", recordFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add student with valid advisor FK: %v", err)
		}
		t.Logf("Successfully added student with recordID=%d", recordID)
	})

	// Step 6: Add another department and teacher to test multiple relationships
	t.Run("Add Mathematics department and teacher", func(t *testing.T) {
		// Add department
		deptFields := map[string]interface{}{
			"Name":            "Mathematics",
			"Department_code": "MATH    ",
			"Building_name":   "Science Hall",
			"Building_code":   "SCI0001H",
		}

		deptID, err := AddNewRecord("Departments", deptFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add Math department: %v", err)
		}
		t.Logf("Added Math department with recordID=%d", deptID)

		// Add teacher in Math department
		teacherFields := map[string]interface{}{
			"Employee_id":   "T0000002",
			"First_name":    "Alice",
			"Last_name":     "Johnson",
			"Building_code": "SCI0001H",
			"Office":        "Room 205  ",
			"Works_for":     "MATH    ",
		}

		teacherID, err := AddNewRecord("Teachers", teacherFields, dbDef)
		if err != nil {
			t.Fatalf("Failed to add Math teacher: %v", err)
		}
		t.Logf("Successfully added Math teacher with recordID=%d", teacherID)
	})

	t.Log("All foreign key constraint tests passed!")
}
