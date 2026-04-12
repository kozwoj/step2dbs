package record

import (
	"path/filepath"
	"github.com/kozwoj/step2/db"
	"testing"
)

// TestGetRecordID tests mapping primary key to recordID
func TestGetRecordID(t *testing.T) {
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

	// Add test records with known primary keys
	t.Run("Add test records", func(t *testing.T) {
		records := []struct {
			pkValue int
			name    string
		}{
			{100, "Test100"},
			{200, "Test200"},
			{300, "Test300"},
		}

		for _, rec := range records {
			recordFields := map[string]interface{}{
			"Integer_value":        float64(rec.pkValue),
			"Small_int_value":      float64(1),
			"Big_int_value":        float64(1000),
				"Decimal_value":        "10.50",
				"Float_value":          1.5,
				"String_size_value":    rec.name,
				"String_no_size_value": "Test",
				"Char_array_value":     "TESTCHAR0000000",
				"Boolean_value":        true,
				"Date_value":           "2024-03-01",
				"Time_value":           "10:00:00",
			}

			_, err := AddNewRecord("AllTypes", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add record with pk=%d: %v", rec.pkValue, err)
			}
		}
		t.Log("Added 3 test records")
	})

	// Test GetRecordID with INT primary key
	t.Run("Lookup INT primary key", func(t *testing.T) {
		recordID, err := GetRecordID("AllTypes", int32(200), dbDef)
		if err != nil {
			t.Fatalf("GetRecordID failed: %v", err)
		}

		if recordID == 0 {
			t.Error("Expected non-zero recordID")
		}
		t.Logf("Found recordID=%d for primary key 200", recordID)

		// Verify we can read the record using this recordID
		record, err := GetRecordByID("AllTypes", recordID, dbDef)
		if err != nil {
			t.Errorf("Failed to read record with found recordID: %v", err)
		} else {
			if record["Integer_value"].(int) != 200 {
				t.Errorf("Expected Integer_value=200, got %v", record["Integer_value"])
			}
			t.Log("Successfully verified record using returned recordID")
		}
	})

	// Test non-existent primary key
	t.Run("Non-existent primary key", func(t *testing.T) {
		_, err := GetRecordID("AllTypes", int32(999), dbDef)
		if err == nil {
			t.Error("Expected error for non-existent primary key")
		} else {
			t.Logf("Correctly rejected non-existent key: %v", err)
		}
	})

	// Test invalid table name
	t.Run("Invalid table name", func(t *testing.T) {
		_, err := GetRecordID("NonExistentTable", int32(100), dbDef)
		if err == nil {
			t.Error("Expected error for invalid table name")
		} else {
			t.Logf("Correctly rejected invalid table: %v", err)
		}
	})
}

// TestGetRecordID_CharPrimaryKey tests GetRecordID with CHAR primary key
func TestGetRecordID_CharPrimaryKey(t *testing.T) {
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

	// Add test departments
	t.Run("Add departments", func(t *testing.T) {
		departments := []struct {
			code string
			name string
		}{
			{"COMPSCI ", "Computer Science"},
			{"MATH    ", "Mathematics"},
			{"PHYSICS ", "Physics"},
		}

		for _, dept := range departments {
			recordFields := map[string]interface{}{
				"Name":            dept.name,
				"Department_code": dept.code,
				"Building_name":   "Science Hall",
				"Building_code":   "SCI0001H",
			}

			_, err := AddNewRecord("Departments", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add department: %v", err)
			}
		}
		t.Log("Added 3 departments")
	})

	// Test GetRecordID with CHAR primary key
	t.Run("Lookup CHAR primary key", func(t *testing.T) {
		recordID, err := GetRecordID("Departments", "MATH    ", dbDef)
		if err != nil {
			t.Fatalf("GetRecordID failed: %v", err)
		}

		if recordID == 0 {
			t.Error("Expected non-zero recordID")
		}
		t.Logf("Found recordID=%d for primary key 'MATH    '", recordID)

		// Verify we can read the record
		record, err := GetRecordByID("Departments", recordID, dbDef)
		if err != nil {
			t.Errorf("Failed to read record: %v", err)
		} else {
			if record["Name"].(string) != "Mathematics" {
				t.Errorf("Expected Name='Mathematics', got %v", record["Name"])
			}
			t.Log("Successfully verified department record")
		}
	})

	// Test wrong length CHAR key
	t.Run("Wrong length CHAR key", func(t *testing.T) {
		_, err := GetRecordID("Departments", "MATH", dbDef)
		if err == nil {
			t.Error("Expected error for wrong length CHAR key")
		} else {
			t.Logf("Correctly rejected wrong length key: %v", err)
		}
	})
}

// TestGetNextRecord tests sequential record traversal
func TestGetNextRecord(t *testing.T) {
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

	// Add test records
	var recordIDs []uint32
	t.Run("Add test records", func(t *testing.T) {
		for i := 1; i <= 5; i++ {
			recordFields := map[string]interface{}{
				"Integer_value":        float64(100 * i),
				"Small_int_value":      float64(i),
				"Big_int_value":        float64(1000 * i),
				"Decimal_value":        "10.50",
				"Float_value":          float64(i) * 1.5,
				"String_size_value":    "Test" + string(rune('0'+i)),
				"String_no_size_value": "Test",
				"Char_array_value":     "TESTCHAR0000000",
				"Boolean_value":        true,
				"Date_value":           "2024-03-01",
				"Time_value":           "10:00:00",
			}

			recordID, err := AddNewRecord("AllTypes", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add record %d: %v", i, err)
			}
			recordIDs = append(recordIDs, recordID)
		}
		t.Logf("Added 5 test records: %v", recordIDs)
	})

	// Test sequential traversal
	t.Run("Sequential traversal", func(t *testing.T) {
		currentID := uint32(0)

		for i := 0; i < 5; i++ {
			record, nextID, err := GetNextRecord("AllTypes", currentID, dbDef)
			if err != nil {
				t.Fatalf("GetNextRecord failed at iteration %d: %v", i, err)
			}

			if nextID != recordIDs[i] {
				t.Errorf("Expected recordID=%d, got %d", recordIDs[i], nextID)
			}

			expectedInt := 100 * (i + 1)
			if record["Integer_value"].(int) != expectedInt {
				t.Errorf("Expected Integer_value=%d, got %v", expectedInt, record["Integer_value"])
			}

			t.Logf("Record %d: Integer_value=%d", nextID, record["Integer_value"].(int))
			currentID = nextID
		}
	})

	// Test with deleted record in middle
	t.Run("Skip deleted records", func(t *testing.T) {
		// Delete record at index 2 (middle record)
		err := DeleteRecord("AllTypes", recordIDs[2], dbDef)
		if err != nil {
			t.Fatalf("Failed to delete record: %v", err)
		}
		t.Logf("Deleted record %d", recordIDs[2])

		// Get next after record 1 - should skip deleted record 2 and return record 3
		record, nextID, err := GetNextRecord("AllTypes", recordIDs[1], dbDef)
		if err != nil {
			t.Fatalf("GetNextRecord failed: %v", err)
		}

		if nextID != recordIDs[3] {
			t.Errorf("Expected to skip deleted record and get %d, got %d", recordIDs[3], nextID)
		}

		expectedInt := 400 // 100 * 4 (4th record, index 3)
		if record["Integer_value"].(int) != expectedInt {
			t.Errorf("Expected Integer_value=%d, got %v", expectedInt, record["Integer_value"])
		}

		t.Logf("Successfully skipped deleted record, got record %d", nextID)
	})

	// Test at end of table
	t.Run("No more records", func(t *testing.T) {
		_, _, err := GetNextRecord("AllTypes", recordIDs[4], dbDef)
		if err == nil {
			t.Error("Expected error when no more records available")
		} else {
			t.Logf("Correctly returned error at end: %v", err)
		}
	})

	// Test invalid table name
	t.Run("Invalid table name", func(t *testing.T) {
		_, _, err := GetNextRecord("NonExistentTable", uint32(0), dbDef)
		if err == nil {
			t.Error("Expected error for invalid table name")
		} else {
			t.Logf("Correctly rejected invalid table: %v", err)
		}
	})

	// Test starting from high record ID
	t.Run("Start from beyond last record", func(t *testing.T) {
		_, _, err := GetNextRecord("AllTypes", uint32(9999), dbDef)
		if err == nil {
			t.Error("Expected error when starting beyond last record")
		} else {
			t.Logf("Correctly returned error: %v", err)
		}
	})
}

// TestGetRecordByKey tests retrieving records by primary key value
func TestGetRecordByKey(t *testing.T) {
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

	// Add test records with known primary keys
	t.Run("Add test records", func(t *testing.T) {
		records := []struct {
			pkValue int
			name    string
		}{
			{100, "Record100"},
			{200, "Record200"},
			{300, "Record300"},
		}

		for _, rec := range records {
			recordFields := map[string]interface{}{
				"Integer_value":        float64(rec.pkValue),
				"Small_int_value":      float64(1),
				"Big_int_value":        float64(1000),
				"Decimal_value":        "10.50",
				"Float_value":          1.5,
				"String_size_value":    rec.name,
				"String_no_size_value": "Test",
				"Char_array_value":     "TESTCHAR0000000",
				"Boolean_value":        true,
				"Date_value":           "2024-03-01",
				"Time_value":           "10:00:00",
			}

			_, err := AddNewRecord("AllTypes", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add record with pk=%d: %v", rec.pkValue, err)
			}
		}
		t.Log("Added 3 test records")
	})

	// Test successful retrieval with INT primary key
	t.Run("Get record by INT primary key", func(t *testing.T) {
		record, recordID, err := GetRecordByKey("AllTypes", int32(200), dbDef)
		if err != nil {
			t.Fatalf("GetRecordByKey failed: %v", err)
		}

		if recordID == 0 {
			t.Error("Expected non-zero recordID")
		}

		if record["Integer_value"].(int) != 200 {
			t.Errorf("Expected Integer_value=200, got %v", record["Integer_value"])
		}

		if record["String_size_value"].(string) != "Record200" {
			t.Errorf("Expected String_size_value='Record200', got %v", record["String_size_value"])
		}

		t.Logf("Successfully retrieved record %d with primary key 200", recordID)
	})

	// Test non-existent primary key
	t.Run("Non-existent primary key", func(t *testing.T) {
		_, _, err := GetRecordByKey("AllTypes", int32(999), dbDef)
		if err == nil {
			t.Error("Expected error for non-existent primary key")
		} else {
			t.Logf("Correctly rejected non-existent key: %v", err)
		}
	})

	// Test invalid table name
	t.Run("Invalid table name", func(t *testing.T) {
		_, _, err := GetRecordByKey("NonExistentTable", int32(100), dbDef)
		if err == nil {
			t.Error("Expected error for invalid table name")
		} else {
			t.Logf("Correctly rejected invalid table: %v", err)
		}
	})
}

// TestGetRecordByKey_CharPrimaryKey tests GetRecordByKey with CHAR primary key
func TestGetRecordByKey_CharPrimaryKey(t *testing.T) {
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

	// Add test departments
	t.Run("Add departments", func(t *testing.T) {
		departments := []struct {
			code string
			name string
		}{
			{"COMPSCI ", "Computer Science"},
			{"MATH    ", "Mathematics"},
			{"PHYSICS ", "Physics"},
		}

		for _, dept := range departments {
			recordFields := map[string]interface{}{
				"Name":            dept.name,
				"Department_code": dept.code,
				"Building_name":   "Science Hall",
				"Building_code":   "SCI0001H",
			}

			_, err := AddNewRecord("Departments", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add department: %v", err)
			}
		}
		t.Log("Added 3 departments")
	})

	// Test GetRecordByKey with CHAR primary key
	t.Run("Get record by CHAR primary key", func(t *testing.T) {
		record, recordID, err := GetRecordByKey("Departments", "MATH    ", dbDef)
		if err != nil {
			t.Fatalf("GetRecordByKey failed: %v", err)
		}

		if recordID == 0 {
			t.Error("Expected non-zero recordID")
		}

		if record["Name"].(string) != "Mathematics" {
			t.Errorf("Expected Name='Mathematics', got %v", record["Name"])
		}

		if record["Department_code"].(string) != "MATH    " {
			t.Errorf("Expected Department_code='MATH    ', got %v", record["Department_code"])
		}

		t.Logf("Successfully retrieved department record %d with code 'MATH    '", recordID)
	})

	// Test wrong length CHAR key
	t.Run("Wrong length CHAR key", func(t *testing.T) {
		_, _, err := GetRecordByKey("Departments", "MATH", dbDef)
		if err == nil {
			t.Error("Expected error for wrong length CHAR key")
		} else {
			t.Logf("Correctly rejected wrong length key: %v", err)
		}
	})
}

// TestGetRecordsByString tests retrieving recordIDs by exact STRING field match
func TestGetRecordsByString(t *testing.T) {
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

	dbDef := db.Definition()

	// Add test customers with specific cities
	t.Run("Add test customers", func(t *testing.T) {
		customers := []struct {
			id      string
			company string
			city    string
			country string
		}{
			{"CUST000001", "TechCorp", "Seattle", "USA"},
			{"CUST000002", "DataInc", "Portland", "USA"},
			{"CUST000003", "CloudSys", "Seattle", "USA"},
			{"CUST000004", "NetWorks", "Boston", "USA"},
			{"CUST000005", "CodeLab", "Seattle", "USA"},
		}

		for _, cust := range customers {
			recordFields := map[string]interface{}{
				"Customer_id":   cust.id,
				"Company_name":  cust.company,
				"Contact_name":  "John Doe",
				"Contact_title": "Manager",
				"Address":       "123 Main St",
				"City":          cust.city,
				"Region":        "West",
				"Postal_code":   "12345",
				"Country":       cust.country,
				"Phone":         "555-1234",
				"Fax":           "555-5678",
			}

			_, err := AddNewRecord("Customers", recordFields, dbDef)
			if err != nil {
				t.Fatalf("Failed to add customer %s: %v", cust.id, err)
			}
		}
		t.Log("Added 5 test customers")
	})

	// Test finding records with specific city value
	t.Run("Find records by City=Seattle", func(t *testing.T) {
		recordIDs, err := GetRecordsByString("Customers", "City", "Seattle", dbDef)
		if err != nil {
			t.Fatalf("GetRecordsByString failed: %v", err)
		}

		if len(recordIDs) != 3 {
			t.Errorf("Expected 3 records with City=Seattle, got %d", len(recordIDs))
		} else {
			t.Logf("Found %d customers in Seattle: %v", len(recordIDs), recordIDs)
		}

		// Verify the records actually have City="Seattle"
		for _, recID := range recordIDs {
			rec, err := GetRecordByID("Customers", recID, dbDef)
			if err != nil {
				t.Errorf("Failed to read record %d: %v", recID, err)
				continue
			}
			if rec["City"].(string) != "Seattle" {
				t.Errorf("Record %d has City=%v, expected Seattle", recID, rec["City"])
			}
		}
	})

	// Test finding records with different city value
	t.Run("Find records by City=Portland", func(t *testing.T) {
		recordIDs, err := GetRecordsByString("Customers", "City", "Portland", dbDef)
		if err != nil {
			t.Fatalf("GetRecordsByString failed: %v", err)
		}

		if len(recordIDs) != 1 {
			t.Errorf("Expected 1 record with City=Portland, got %d", len(recordIDs))
		} else {
			t.Logf("Found %d customer in Portland", len(recordIDs))
		}
	})

	// Test string not found in dictionary (should return empty slice)
	t.Run("Search for non-existent city", func(t *testing.T) {
		recordIDs, err := GetRecordsByString("Customers", "City", "Tokyo", dbDef)
		if err != nil {
			t.Fatalf("GetRecordsByString failed: %v", err)
		}

		if len(recordIDs) != 0 {
			t.Errorf("Expected 0 records for non-existent city, got %d", len(recordIDs))
		} else {
			t.Log("Correctly returned empty slice for non-existent city")
		}
	})

	// Test invalid table
	t.Run("Invalid table name", func(t *testing.T) {
		_, err := GetRecordsByString("InvalidTable", "City", "Seattle", dbDef)
		if err == nil {
			t.Error("Expected error for invalid table name")
		} else {
			t.Logf("Correctly rejected invalid table: %v", err)
		}
	})

	// Test invalid field
	t.Run("Invalid field name", func(t *testing.T) {
		_, err := GetRecordsByString("Customers", "InvalidField", "Seattle", dbDef)
		if err == nil {
			t.Error("Expected error for invalid field name")
		} else {
			t.Logf("Correctly rejected invalid field: %v", err)
		}
	})

	// Test non-STRING field (CHAR field should not have dictionary)
	t.Run("CHAR field (no dictionary)", func(t *testing.T) {
		_, err := GetRecordsByString("Customers", "Customer_id", "CUST000001", dbDef)
		if err == nil {
			t.Error("Expected error for CHAR field (no dictionary)")
		} else {
			t.Logf("Correctly rejected CHAR field: %v", err)
		}
	})
}


// TestGetRecordsBySubstring tests retrieving recordIDs by STRING field prefix search
func TestGetRecordsBySubstring(t *testing.T) {
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

dbDef := db.Definition()

// Add test customers
t.Run("Add test customers", func(t *testing.T) {
customers := []struct {
id      string
contact string
}{
{"CUST000001", "Smith John"},
{"CUST000002", "Smithson Alice"},
{"CUST000003", "Johnson Mary"},
{"CUST000004", "Smithers Bob"},
{"CUST000005", "Kowalski Tom"},
{"CUST000006", "Kozaczynski Wojtek"},
{"CUST000007", "Anderson Jane"},
}

for _, cust := range customers {
recordFields := map[string]interface{}{
"Customer_id":   cust.id,
"Company_name":  "ACME Corp",
"Contact_name":  cust.contact,
"Contact_title": "Manager",
"Address":       "123 Main St",
"City":          "Seattle",
"Region":        "WA",
"Postal_code":   "98101",
"Country":       "USA",
"Phone":         "555-1234",
"Fax":           "555-5678",
}

_, err := AddNewRecord("Customers", recordFields, dbDef)
if err != nil {
t.Fatalf("Failed to add customer %s: %v", cust.id, err)
}
}
t.Log("Added 7 test customers")
})

// Search with "Smith" prefix
	t.Run("Search Contact_name starting with 'Smith'", func(t *testing.T) {
		recordIDs, err := GetRecordsBySubstring("Customers", "Contact_name", "Smith", dbDef)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring failed: %v", err)
		}

		if len(recordIDs) != 3 {
			t.Errorf("Expected 3 records, got %d", len(recordIDs))
		} else {
			t.Logf("Found %d contacts with last name starting with 'Smith'", len(recordIDs))
}
})

// Search with "Koz" prefix
	t.Run("Search Contact_name starting with 'Koz'", func(t *testing.T) {
		recordIDs, err := GetRecordsBySubstring("Customers", "Contact_name", "Koz", dbDef)
		if err != nil {
			t.Fatalf("GetRecordsBySubstring failed: %v", err)
		}

		if len(recordIDs) != 1 {
			t.Errorf("Expected 1 record, got %d", len(recordIDs))
}
})

// Search with "J" prefix
	t.Run("Search Contact_name starting with 'J'", func(t *testing.T) {
		recordIDs, err := GetRecordsBySubstring("Customers", "Contact_name", "J", dbDef)
if err != nil {
t.Fatalf("GetRecordsBySubstring failed: %v", err)
}

if len(recordIDs) != 1 {
t.Errorf("Expected 1 record, got %d", len(recordIDs))
}
})

// Search with no matches
t.Run("Search with no matches", func(t *testing.T) {
recordIDs, err := GetRecordsBySubstring("Customers", "Contact_name", "Xyz", dbDef)
if err != nil {
t.Fatalf("GetRecordsBySubstring failed: %v", err)
}

if len(recordIDs) != 0 {
t.Errorf("Expected 0 records, got %d", len(recordIDs))
}
})

// Test 8-character substring (boundary)
t.Run("8-character substring", func(t *testing.T) {
recordIDs, err := GetRecordsBySubstring("Customers", "Contact_name", "Smithson", dbDef)
if err != nil {
t.Fatalf("GetRecordsBySubstring failed: %v", err)
}

if len(recordIDs) != 1 {
t.Errorf("Expected 1 record, got %d", len(recordIDs))
}
})

// Test substring exceeds 8 characters
t.Run("Substring exceeds 8 characters", func(t *testing.T) {
_, err := GetRecordsBySubstring("Customers", "Contact_name", "VeryLongName", dbDef)
if err == nil {
t.Error("Expected error for substring > 8 characters")
} else {
t.Logf("Correctly rejected long substring: %v", err)
}
})

// Test empty substring
t.Run("Empty substring", func(t *testing.T) {
_, err := GetRecordsBySubstring("Customers", "Contact_name", "", dbDef)
if err == nil {
t.Error("Expected error for empty substring")
}
})

// Test invalid table name
t.Run("Invalid table name", func(t *testing.T) {
_, err := GetRecordsBySubstring("InvalidTable", "Contact_name", "Smith", dbDef)
if err == nil {
t.Error("Expected error for invalid table name")
}
})

// Test invalid field name
t.Run("Invalid field name", func(t *testing.T) {
_, err := GetRecordsBySubstring("Customers", "InvalidField", "Smith", dbDef)
if err == nil {
t.Error("Expected error for invalid field name")
}
})

// Test non-STRING field
t.Run("Non-STRING field", func(t *testing.T) {
_, err := GetRecordsBySubstring("Customers", "Customer_id", "CUST", dbDef)
if err == nil {
t.Error("Expected error for CHAR field")
} else {
t.Logf("Correctly rejected CHAR field: %v", err)
}
})
}
