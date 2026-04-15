package record

import (
	"encoding/binary"
	"path/filepath"
	"testing"

	"github.com/kozwoj/step2/db"
)

// TestAddSetMember_GetSetMembers tests adding teachers to a department's Faculty set
// and retrieving them.
func TestAddSetMember_GetSetMembers(t *testing.T) {
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
	dbDir := filepath.Join(tempDir, "College")
	t.Log("Opening database...")
	err = db.OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()

	// Get the database definition
	dbDef := db.Definition()

	// Step 1: Add a Department record
	t.Run("Add Department", func(t *testing.T) {
		recordFields := map[string]interface{}{
			"Name":            "Computer Science",
			"Department_code": "COMPSCI ",
			"Building_name":   "Engineering Building",
			"Building_code":   "ENG0001H",
		}

		recordID, err := AddNewRecord("Departments", recordFields, dbDef)
		if err != nil {
			t.Fatalf("AddNewRecord for Department failed: %v", err)
		}
		t.Logf("Added Department with recordID: %d", recordID)

		if recordID != 1 {
			t.Errorf("Expected Department recordID to be 1, got %d", recordID)
		}
	})

	// Step 2: Add two Teacher records
	var teacher1ID, teacher2ID uint32
	t.Run("Add Teachers", func(t *testing.T) {
		teacher1Fields := map[string]interface{}{
			"Employee_id":   "T0000001",
			"First_name":    "Alice",
			"Last_name":     "Smith",
			"Building_code": "ENG0001H",
			"Office":        "ENG-201   ",
			"Works_for":     "COMPSCI ",
		}

		var err error
		teacher1ID, err = AddNewRecord("Teachers", teacher1Fields, dbDef)
		if err != nil {
			t.Fatalf("AddNewRecord for Teacher 1 failed: %v", err)
		}
		t.Logf("Added Teacher 1 with recordID: %d", teacher1ID)

		teacher2Fields := map[string]interface{}{
			"Employee_id":   "T0000002",
			"First_name":    "Bob",
			"Last_name":     "Johnson",
			"Building_code": "ENG0001H",
			"Office":        "ENG-202   ",
			"Works_for":     "COMPSCI ",
		}

		teacher2ID, err = AddNewRecord("Teachers", teacher2Fields, dbDef)
		if err != nil {
			t.Fatalf("AddNewRecord for Teacher 2 failed: %v", err)
		}
		t.Logf("Added Teacher 2 with recordID: %d", teacher2ID)
	})

	// Step 3: Verify Faculty set is initially empty
	t.Run("Get empty Faculty set", func(t *testing.T) {
		members, err := GetSetMembers("Departments", 1, "Faculty", dbDef)
		if err != nil {
			t.Fatalf("GetSetMembers failed: %v", err)
		}
		t.Logf("GetSetMembers result: %v", members)

		// Verify members is empty
		if len(members) != 0 {
			t.Errorf("Expected empty members array, got %d members", len(members))
		}
	})

	// Step 4: Add first teacher to Faculty set
	t.Run("Add first teacher to Faculty set", func(t *testing.T) {
		err := AddSetMember("Departments", 1, "Faculty", teacher1ID, dbDef)
		if err != nil {
			t.Fatalf("AddSetMember for Teacher 1 failed: %v", err)
		}
		t.Logf("Added Teacher %d to Faculty set", teacher1ID)
	})

	// Step 5: Verify Faculty set contains first teacher
	t.Run("Get Faculty set with one member", func(t *testing.T) {
		members, err := GetSetMembers("Departments", 1, "Faculty", dbDef)
		if err != nil {
			t.Fatalf("GetSetMembers failed: %v", err)
		}
		t.Logf("GetSetMembers result: %v", members)

		// Verify members contains teacher1ID
		if len(members) != 1 {
			t.Errorf("Expected 1 member, got %d", len(members))
		} else {
			if members[0] != teacher1ID {
				t.Errorf("Expected member %d, got %d", teacher1ID, members[0])
			}
		}
	})

	// Step 6: Add second teacher to Faculty set
	t.Run("Add second teacher to Faculty set", func(t *testing.T) {
		err := AddSetMember("Departments", 1, "Faculty", teacher2ID, dbDef)
		if err != nil {
			t.Fatalf("AddSetMember for Teacher 2 failed: %v", err)
		}
		t.Logf("Added Teacher %d to Faculty set", teacher2ID)
	})

	// Step 7: Verify Faculty set contains both teachers
	t.Run("Get Faculty set with two members", func(t *testing.T) {
		members, err := GetSetMembers("Departments", 1, "Faculty", dbDef)
		if err != nil {
			t.Fatalf("GetSetMembers failed: %v", err)
		}
		t.Logf("GetSetMembers result: %v", members)

		// Verify members contains both teachers
		if len(members) != 2 {
			t.Errorf("Expected 2 members, got %d", len(members))
		} else {
			// Check that both teacher IDs are present (order doesn't matter)
			foundTeacher1 := (members[0] == teacher1ID || members[1] == teacher1ID)
			foundTeacher2 := (members[0] == teacher2ID || members[1] == teacher2ID)

			if !foundTeacher1 {
				t.Errorf("Teacher %d not found in set", teacher1ID)
			}
			if !foundTeacher2 {
				t.Errorf("Teacher %d not found in set", teacher2ID)
			}
		}
	})

	// Step 8: Remove first teacher from Faculty set
	t.Run("Remove first teacher from Faculty set", func(t *testing.T) {
		err := RemoveSetMember("Departments", 1, "Faculty", teacher1ID, dbDef)
		if err != nil {
			t.Fatalf("RemoveSetMember for Teacher 1 failed: %v", err)
		}
		t.Logf("Removed Teacher %d from Faculty set", teacher1ID)
	})

	// Step 9: Verify Faculty set contains only second teacher
	t.Run("Get Faculty set with one member after removal", func(t *testing.T) {
		members, err := GetSetMembers("Departments", 1, "Faculty", dbDef)
		if err != nil {
			t.Fatalf("GetSetMembers failed: %v", err)
		}
		t.Logf("GetSetMembers result: %v", members)

		// Verify members contains only teacher2ID
		if len(members) != 1 {
			t.Errorf("Expected 1 member after removing teacher1, got %d", len(members))
		} else {
			if members[0] != teacher2ID {
				t.Errorf("Expected member %d, got %d", teacher2ID, members[0])
			}
		}
	})

	// Step 10: Remove last teacher from Faculty set
	t.Run("Remove last teacher from Faculty set", func(t *testing.T) {
		err := RemoveSetMember("Departments", 1, "Faculty", teacher2ID, dbDef)
		if err != nil {
			t.Fatalf("RemoveSetMember for Teacher 2 failed: %v", err)
		}
		t.Logf("Removed Teacher %d from Faculty set (last member)", teacher2ID)
	})

	// Step 11: Verify Faculty set is empty and header is updated to NoSet
	t.Run("Get Faculty set after removing all members", func(t *testing.T) {
		members, err := GetSetMembers("Departments", 1, "Faculty", dbDef)
		if err != nil {
			t.Fatalf("GetSetMembers failed: %v", err)
		}
		t.Logf("GetSetMembers result: %v", members)

		// Verify members is empty
		if len(members) != 0 {
			t.Errorf("Expected empty members array after removing all, got %d members", len(members))
		}

		// Verify record header has NoSet for Faculty set
		deptTable := dbDef.Tables[dbDef.TableIndex["Departments"]]
		recordData, err := GetRecordData(deptTable.RecordFile, 1)
		if err != nil {
			t.Fatalf("Failed to read department record: %v", err)
		}

		// Faculty is the first set (index 0), so it's at offset 5
		setBlockOffset := 5
		blockNumber := binary.LittleEndian.Uint32(recordData[setBlockOffset : setBlockOffset+4])
		if blockNumber != uint32(NoSet) {
			t.Errorf("Expected set block number to be NoSet (0x%X), got 0x%X", NoSet, blockNumber)
		} else {
			t.Logf("Verified: Set block number is NoSet (0x%X) after removing all members", NoSet)
		}
	})
}
