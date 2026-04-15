package db_test

import (
	"testing"

	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/testdb"
)

// TestLoadNIPDatabase creates and populates the NIP database with test data
// and verifies there are no integrity issues.
func TestLoadNIPDatabase(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()
	// tempDir := "C:\\temp\\nip_test_db"

	t.Log("Creating and populating NIP database...")
	dbPath, stats, err := testdb.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("Failed to create and populate NIP database: %v", err)
	}
	defer db.CloseDB()

	t.Logf("Successfully created NIP database at: %s", dbPath)

	// Verify expected record counts
	expectedCounts := map[string]int{
		"Departments": 5,
		"Courses":     45,
		"Teachers":    85,
		"Students":    337,
		"Classes":     46,
		"Grades":      317,
		"Majors":      14,
	}

	t.Log("\nRecord counts:")
	for tableName, expectedCount := range expectedCounts {
		actualCount := stats[tableName]
		t.Logf("  %-15s: %3d records", tableName, actualCount)
		if actualCount != expectedCount {
			t.Errorf("Table %s: expected %d records, got %d", tableName, expectedCount, actualCount)
		}
	}

	// Verify total records
	totalRecords := 0
	for _, count := range stats {
		totalRecords += count
	}
	expectedTotal := 865 // 5 + 45 + 85 + 337 + 46 + 317 + 14 + 16 (Enrollment)
	t.Logf("  %-15s: %3d records", "TOTAL", totalRecords)
	if totalRecords != expectedTotal {
		t.Errorf("Expected total of %d records, got %d", expectedTotal, totalRecords)
	}

	// Test GetSchema on the populated database
	t.Run("GetSchema on populated database", func(t *testing.T) {
		schema, err := db.GetSchema()
		if err != nil {
			t.Fatalf("GetSchema failed: %v", err)
		}

		// Verify schema structure
		if schema["name"] != "College" {
			t.Errorf("Expected schema name 'College', got '%v'", schema["name"])
		}

		tables, ok := schema["tables"].([]map[string]interface{})
		if !ok {
			t.Fatalf("Schema tables field is not []map[string]interface{}")
		}

		if len(tables) != 7 {
			t.Errorf("Expected 7 tables in schema, got %d", len(tables))
		}

		t.Logf("GetSchema returned schema with %d tables", len(tables))
	})

	// Test GetTableStats on the populated database
	t.Run("GetTableStats on populated database", func(t *testing.T) {
		// Request stats for all tables
		tableNames := []string{"Departments", "Courses", "Teachers", "Students", "Classes", "Grades", "Majors"}

		tableStats, errors, err := db.GetTableStats(tableNames)
		if err != nil {
			t.Fatalf("GetTableStats failed: %v", err)
		}

		if len(errors) > 0 {
			t.Errorf("GetTableStats returned errors: %v", errors)
		}

		if len(tableStats) != len(tableNames) {
			t.Errorf("Expected stats for %d tables, got %d", len(tableNames), len(tableStats))
		}

		// Verify each table's stats
		t.Log("\nTable statistics:")
		for _, stat := range tableStats {
			tableName := stat["name"].(string)
			allocatedRecords := stat["allocated_records"].(int)
			deletedListLength := stat["deleted_list_length"].(int)
			dictionaries := stat["dictionaries"].([]map[string]interface{})

			activeRecords := allocatedRecords - deletedListLength
			expectedCount := expectedCounts[tableName]

			t.Logf("  %-15s: allocated_records=%3d, deleted=%d, active=%3d",
				tableName, allocatedRecords, deletedListLength, activeRecords)

			if activeRecords != expectedCount {
				t.Errorf("Table %s: expected %d active records, got %d", tableName, expectedCount, activeRecords)
			}

			// Log dictionary info if present
			if len(dictionaries) > 0 {
				for _, dict := range dictionaries {
					fieldName := dict["field_name"].(string)
					numStrings := dict["number_of_strings"].(int)
					t.Logf("    Dictionary %-15s: %d strings", fieldName, numStrings)
				}
			}
		}
	})

	// Test some specific FK relationships
	t.Run("Verify foreign key relationships", func(t *testing.T) {
		dbDef := db.Definition()

		// Verify Grades.Course_code FK to Courses works correctly
		// This was the schema fix we made
		gradesTableIdx := dbDef.TableIndex["Grades"]
		gradesTable := dbDef.Tables[gradesTableIdx]

		// Find the Course_code field
		courseCodeFieldIdx := gradesTable.RecordLayout.FieldIndex["Course_code"]
		courseCodeField := gradesTable.RecordLayout.Fields[courseCodeFieldIdx]

		if !courseCodeField.IsForeignKey {
			t.Error("Grades.Course_code should be a foreign key")
		}

		if courseCodeField.ForeignKeyTable != "Courses" {
			t.Errorf("Grades.Course_code FK should reference Courses, got %s", courseCodeField.ForeignKeyTable)
		}

		t.Logf("Verified Grades.Course_code -> Courses FK relationship")
	})

	t.Log("\n✓ NIP database successfully created and validated")
}
