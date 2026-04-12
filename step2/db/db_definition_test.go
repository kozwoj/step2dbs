package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestCreateDBDefinition_CollegeSchema(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Test: Create DB definition from College.ddl
	dbDef, err := CreateDBDefinition(schemaFile, tempDir)
	if err != nil {
		t.Fatalf("CreateDBDefinition failed: %v", err)
	}

	// Verify DB definition was created
	if dbDef == nil {
		t.Fatal("Expected non-nil DBDefinition")
	}

	// Verify schema name
	if dbDef.Name != "College" {
		t.Errorf("Expected schema name 'College', got '%s'", dbDef.Name)
	}

	// Verify DB directory path
	expectedPath := filepath.Join(tempDir, "College")
	if dbDef.DirPath != expectedPath {
		t.Errorf("Expected DirPath '%s', got '%s'", expectedPath, dbDef.DirPath)
	}

	// Verify DB directory was created
	if _, err := os.Stat(expectedPath); os.IsNotExist(err) {
		t.Errorf("Expected DB directory to be created at '%s'", expectedPath)
	}

	// Verify number of tables (should be 7: Departments, Teachers, Students, Courses, Classes, Grades, Majors)
	expectedTables := 7
	if len(dbDef.Tables) != expectedTables {
		t.Errorf("Expected %d tables, got %d", expectedTables, len(dbDef.Tables))
	}

	// Verify TableIndex map
	if len(dbDef.TableIndex) != expectedTables {
		t.Errorf("Expected TableIndex to have %d entries, got %d", expectedTables, len(dbDef.TableIndex))
	}

	// Test specific table: Departments
	idx, exists := dbDef.TableIndex["Departments"]
	if !exists {
		t.Fatal("Expected 'Departments' table to exist in TableIndex")
	}
	deptTable := dbDef.Tables[idx]
	if deptTable.Name != "Departments" {
		t.Errorf("Expected table name 'Departments', got '%s'", deptTable.Name)
	}

	// Verify Departments has 4 fields
	if len(deptTable.RecordLayout.Fields) != 4 {
		t.Errorf("Expected Departments to have 4 fields, got %d", len(deptTable.RecordLayout.Fields))
	}

	// Verify Departments primary key is Department_code (field index 1)
	if deptTable.RecordLayout.PrimaryKey != 1 {
		t.Errorf("Expected Departments primary key to be field 1, got %d", deptTable.RecordLayout.PrimaryKey)
	}

	// Verify Department_code field
	deptCodeField := deptTable.RecordLayout.Fields[1]
	if deptCodeField.Name != "Department_code" {
		t.Errorf("Expected field name 'Department_code', got '%s'", deptCodeField.Name)
	}
	if deptCodeField.Type != CHAR {
		t.Errorf("Expected field type CHAR, got %d", deptCodeField.Type)
	}
	if deptCodeField.Size != 8 {
		t.Errorf("Expected field size 8, got %d", deptCodeField.Size)
	}

	// Verify Name field (STRING with size limit)
	nameField := deptTable.RecordLayout.Fields[0]
	if nameField.Name != "Name" {
		t.Errorf("Expected field name 'Name', got '%s'", nameField.Name)
	}
	if nameField.Type != STRING {
		t.Errorf("Expected field type STRING, got %d", nameField.Type)
	}
	if nameField.Size != 4 {
		t.Errorf("Expected Name field Size 4 (serialization), got %d", nameField.Size)
	}
	if nameField.StringSizeLimit != 50 {
		t.Errorf("Expected Name field StringSizeLimit 50, got %d", nameField.StringSizeLimit)
	}

	// Verify Building_name field (unlimited STRING)
	buildingNameField := deptTable.RecordLayout.Fields[2]
	if buildingNameField.Name != "Building_name" {
		t.Errorf("Expected field name 'Building_name', got '%s'", buildingNameField.Name)
	}
	if buildingNameField.Type != STRING {
		t.Errorf("Expected field type STRING, got %d", buildingNameField.Type)
	}
	if buildingNameField.Size != 4 {
		t.Errorf("Expected Building_name field Size 4 (serialization), got %d", buildingNameField.Size)
	}
	if buildingNameField.StringSizeLimit != 0 {
		t.Errorf("Expected Building_name field StringSizeLimit 0 (unlimited), got %d", buildingNameField.StringSizeLimit)
	}

	// Verify Departments has 1 set (Faculty Teachers)
	if len(deptTable.Sets) != 1 {
		t.Errorf("Expected Departments to have 1 set, got %d", len(deptTable.Sets))
	}
	if deptTable.Sets[0].Name != "Faculty" {
		t.Errorf("Expected set name 'Faculty', got '%s'", deptTable.Sets[0].Name)
	}
	if deptTable.Sets[0].MemberTableName != "Teachers" {
		t.Errorf("Expected set member table 'Teachers', got '%s'", deptTable.Sets[0].MemberTableName)
	}

	// Verify Departments HeaderSize and DataSize are computed correctly
	// HeaderSize = 1 (deleted flag) + 4 (next deleted pointer) + 4*1 (one set) = 9 bytes
	expectedHeaderSize := 1 + 4 + 4*len(deptTable.Sets)
	if deptTable.RecordLayout.HeaderSize != expectedHeaderSize {
		t.Errorf("Departments HeaderSize: expected %d, got %d", expectedHeaderSize, deptTable.RecordLayout.HeaderSize)
	}

	// DataSize = sum of (field size + 1 byte HasValue flag) for all fields
	// Fields: Name (STRING=4) + Department_code (CHAR[8]=8) + Building_name (STRING=4) + Building_code (CHAR[8]=8)
	// DataSize = (4+1) + (8+1) + (4+1) + (8+1) = 28 bytes
	expectedDataSize := 0
	for _, field := range deptTable.RecordLayout.Fields {
		expectedDataSize += field.Size + 1 // field size + HasValue flag
	}
	if deptTable.RecordLayout.DataSize != expectedDataSize {
		t.Errorf("Departments DataSize: expected %d, got %d", expectedDataSize, deptTable.RecordLayout.DataSize)
	}

	// Verify total record size
	expectedTotalSize := expectedHeaderSize + expectedDataSize
	actualTotalSize := deptTable.RecordLayout.HeaderSize + deptTable.RecordLayout.DataSize
	if actualTotalSize != expectedTotalSize {
		t.Errorf("Departments total record size: expected %d, got %d", expectedTotalSize, actualTotalSize)
	}
	t.Logf("Departments record: HeaderSize=%d, DataSize=%d, TotalSize=%d",
		deptTable.RecordLayout.HeaderSize, deptTable.RecordLayout.DataSize, actualTotalSize)

	// Test specific table: Teachers
	idx, exists = dbDef.TableIndex["Teachers"]
	if !exists {
		t.Fatal("Expected 'Teachers' table to exist in TableIndex")
	}
	teachersTable := dbDef.Tables[idx]

	// Verify Teachers has 6 fields
	if len(teachersTable.RecordLayout.Fields) != 6 {
		t.Errorf("Expected Teachers to have 6 fields, got %d", len(teachersTable.RecordLayout.Fields))
	}

	// Verify Teachers primary key is Employee_id (field index 0)
	if teachersTable.RecordLayout.PrimaryKey != 0 {
		t.Errorf("Expected Teachers primary key to be field 0, got %d", teachersTable.RecordLayout.PrimaryKey)
	}

	// Verify Works_for foreign key field
	worksForField := teachersTable.RecordLayout.Fields[5]
	if worksForField.Name != "Works_for" {
		t.Errorf("Expected field name 'Works_for', got '%s'", worksForField.Name)
	}
	if !worksForField.IsForeignKey {
		t.Error("Expected Works_for to be a foreign key")
	}
	if worksForField.ForeignKeyTable != "Departments" {
		t.Errorf("Expected foreign key table 'Departments', got '%s'", worksForField.ForeignKeyTable)
	}

	// Verify Teachers has 2 sets
	if len(teachersTable.Sets) != 2 {
		t.Errorf("Expected Teachers to have 2 sets, got %d", len(teachersTable.Sets))
	}

	// Test specific table: Students
	idx, exists = dbDef.TableIndex["Students"]
	if !exists {
		t.Fatal("Expected 'Students' table to exist in TableIndex")
	}
	studentsTable := dbDef.Tables[idx]

	// Verify Students has 12 fields
	if len(studentsTable.RecordLayout.Fields) != 12 {
		t.Errorf("Expected Students to have 12 fields, got %d", len(studentsTable.RecordLayout.Fields))
	}

	// Verify Preferred_name is optional
	prefNameField := studentsTable.RecordLayout.Fields[2]
	if prefNameField.Name != "Preferred_name" {
		t.Errorf("Expected field name 'Preferred_name', got '%s'", prefNameField.Name)
	}
	if !prefNameField.IsOptional {
		t.Error("Expected Preferred_name to be optional")
	}

	// Verify Advisor foreign key
	advisorField := studentsTable.RecordLayout.Fields[9]
	if advisorField.Name != "Advisor" {
		t.Errorf("Expected field name 'Advisor', got '%s'", advisorField.Name)
	}
	if !advisorField.IsForeignKey {
		t.Error("Expected Advisor to be a foreign key")
	}
	if advisorField.ForeignKeyTable != "Teachers" {
		t.Errorf("Expected foreign key table 'Teachers', got '%s'", advisorField.ForeignKeyTable)
	}

	// Test specific table: Grades (no primary key)
	idx, exists = dbDef.TableIndex["Grades"]
	if !exists {
		t.Fatal("Expected 'Grades' table to exist in TableIndex")
	}
	gradesTable := dbDef.Tables[idx]

	// Verify Grades has no primary key
	if gradesTable.RecordLayout.PrimaryKey != -1 {
		t.Errorf("Expected Grades to have no primary key (PrimaryKey = -1), got %d", gradesTable.RecordLayout.PrimaryKey)
	}

	// Verify Grades has 4 fields
	if len(gradesTable.RecordLayout.Fields) != 4 {
		t.Errorf("Expected Grades to have 4 fields, got %d", len(gradesTable.RecordLayout.Fields))
	}

	// Verify Grades has 2 foreign keys
	fkCount := 0
	for _, field := range gradesTable.RecordLayout.Fields {
		if field.IsForeignKey {
			fkCount++
		}
	}
	if fkCount != 2 {
		t.Errorf("Expected Grades to have 2 foreign keys, got %d", fkCount)
	}

	// Verify HeaderSize and DataSize are calculated
	for _, table := range dbDef.Tables {
		if table.RecordLayout.HeaderSize <= 0 {
			t.Errorf("Table %s has invalid HeaderSize: %d", table.Name, table.RecordLayout.HeaderSize)
		}
		if table.RecordLayout.DataSize < 0 {
			t.Errorf("Table %s has invalid DataSize: %d", table.Name, table.RecordLayout.DataSize)
		}
	}

	t.Logf("Successfully created DB definition for College schema with %d tables", len(dbDef.Tables))
}

func TestCreateDBDefinition_NonEmptyDirectory(t *testing.T) {
	// Setup: Create a temporary directory with a subdirectory that has files
	tempDir := t.TempDir()
	// Create the College directory (which will be the schema name) with a file in it
	dbDir := filepath.Join(tempDir, "College")
	err := os.Mkdir(dbDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create test directory: %v", err)
	}

	// Create a file in the directory
	testFile := filepath.Join(dbDir, "test.txt")
	err = os.WriteFile(testFile, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Test: Attempt to create DB in non-empty directory
	// Pass tempDir, function will append "College" from schema
	_, err = CreateDBDefinition(schemaFile, tempDir)
	if err != ErrDBDirNotEmpty {
		t.Errorf("Expected ErrDBDirNotEmpty, got %v", err)
	}
}

func TestCreateDBDefinition_InvalidSchemaFile(t *testing.T) {
	// Setup
	tempDir := t.TempDir()

	// Test: Non-existent schema file
	_, err := CreateDBDefinition("nonexistent.ddl", tempDir)
	if err != ErrDBFailedToReadSchema {
		t.Errorf("Expected ErrDBFailedToReadSchema, got %v", err)
	}
}

func TestSaveAndLoadDBDefinition_CollegeSchema(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Step 1: Create DB definition from College.ddl
	originalDBDef, err := CreateDBDefinition(schemaFile, tempDir)
	if err != nil {
		t.Fatalf("CreateDBDefinition failed: %v", err)
	}

	// Step 2: Save DB definition to JSON file
	jsonFilePath := filepath.Join(tempDir, "schema.json")
	err = SaveDefinitionAsJson(originalDBDef, jsonFilePath)
	if err != nil {
		t.Fatalf("SaveDefinitionAsJson failed: %v", err)
	}

	// Verify JSON file was created
	if _, err := os.Stat(jsonFilePath); os.IsNotExist(err) {
		t.Fatalf("JSON file was not created at '%s'", jsonFilePath)
	}

	// Step 3: Load DB definition from JSON file
	loadedDBDef := &DBDefinition{}
	err = LoadDefinitionFromJson(jsonFilePath, loadedDBDef)
	if err != nil {
		t.Fatalf("LoadDefinitionFromJson failed: %v", err)
	}

	// Step 4: Verify loaded definition matches original

	// Verify basic properties
	if loadedDBDef.Name != originalDBDef.Name {
		t.Errorf("Name mismatch: expected '%s', got '%s'", originalDBDef.Name, loadedDBDef.Name)
	}
	if loadedDBDef.DirPath != originalDBDef.DirPath {
		t.Errorf("DirPath mismatch: expected '%s', got '%s'", originalDBDef.DirPath, loadedDBDef.DirPath)
	}
	if !loadedDBDef.CreatedOn.Equal(originalDBDef.CreatedOn) {
		t.Errorf("CreatedOn mismatch: expected '%v', got '%v'", originalDBDef.CreatedOn, loadedDBDef.CreatedOn)
	}

	// Verify number of tables
	if len(loadedDBDef.Tables) != len(originalDBDef.Tables) {
		t.Fatalf("Tables count mismatch: expected %d, got %d", len(originalDBDef.Tables), len(loadedDBDef.Tables))
	}

	// Verify TableIndex map
	if len(loadedDBDef.TableIndex) != len(originalDBDef.TableIndex) {
		t.Errorf("TableIndex count mismatch: expected %d, got %d", len(originalDBDef.TableIndex), len(loadedDBDef.TableIndex))
	}

	// Verify each table matches
	for tableName, originalIdx := range originalDBDef.TableIndex {
		loadedIdx, exists := loadedDBDef.TableIndex[tableName]
		if !exists {
			t.Errorf("Table '%s' not found in loaded TableIndex", tableName)
			continue
		}

		originalTable := originalDBDef.Tables[originalIdx]
		loadedTable := loadedDBDef.Tables[loadedIdx]

		// Verify table properties
		if loadedTable.Name != originalTable.Name {
			t.Errorf("Table name mismatch for '%s': got '%s'", originalTable.Name, loadedTable.Name)
		}
		if loadedTable.Key != originalTable.Key {
			t.Errorf("Table '%s' Key mismatch: expected %d, got %d", tableName, originalTable.Key, loadedTable.Key)
		}

		// Verify RecordLayout
		if loadedTable.RecordLayout.HeaderSize != originalTable.RecordLayout.HeaderSize {
			t.Errorf("Table '%s' HeaderSize mismatch: expected %d, got %d", tableName, originalTable.RecordLayout.HeaderSize, loadedTable.RecordLayout.HeaderSize)
		}
		if loadedTable.RecordLayout.DataSize != originalTable.RecordLayout.DataSize {
			t.Errorf("Table '%s' DataSize mismatch: expected %d, got %d", tableName, originalTable.RecordLayout.DataSize, loadedTable.RecordLayout.DataSize)
		}
		if loadedTable.RecordLayout.NoFields != originalTable.RecordLayout.NoFields {
			t.Errorf("Table '%s' NoFields mismatch: expected %d, got %d", tableName, originalTable.RecordLayout.NoFields, loadedTable.RecordLayout.NoFields)
		}
		if loadedTable.RecordLayout.PrimaryKey != originalTable.RecordLayout.PrimaryKey {
			t.Errorf("Table '%s' PrimaryKey mismatch: expected %d, got %d", tableName, originalTable.RecordLayout.PrimaryKey, loadedTable.RecordLayout.PrimaryKey)
		}

		// Verify number of fields
		if len(loadedTable.RecordLayout.Fields) != len(originalTable.RecordLayout.Fields) {
			t.Errorf("Table '%s' field count mismatch: expected %d, got %d", tableName, len(originalTable.RecordLayout.Fields), len(loadedTable.RecordLayout.Fields))
			continue
		}

		// Verify each field
		for i, originalField := range originalTable.RecordLayout.Fields {
			loadedField := loadedTable.RecordLayout.Fields[i]

			if loadedField.Name != originalField.Name {
				t.Errorf("Table '%s' field %d name mismatch: expected '%s', got '%s'", tableName, i, originalField.Name, loadedField.Name)
			}
			if loadedField.Type != originalField.Type {
				t.Errorf("Table '%s' field '%s' type mismatch: expected %d, got %d", tableName, originalField.Name, originalField.Type, loadedField.Type)
			}
			if loadedField.IsForeignKey != originalField.IsForeignKey {
				t.Errorf("Table '%s' field '%s' IsForeignKey mismatch: expected %v, got %v", tableName, originalField.Name, originalField.IsForeignKey, loadedField.IsForeignKey)
			}
			if loadedField.ForeignKeyTable != originalField.ForeignKeyTable {
				t.Errorf("Table '%s' field '%s' ForeignKeyTable mismatch: expected '%s', got '%s'", tableName, originalField.Name, originalField.ForeignKeyTable, loadedField.ForeignKeyTable)
			}
			if loadedField.IsOptional != originalField.IsOptional {
				t.Errorf("Table '%s' field '%s' IsOptional mismatch: expected %v, got %v", tableName, originalField.Name, originalField.IsOptional, loadedField.IsOptional)
			}
			if loadedField.Offset != originalField.Offset {
				t.Errorf("Table '%s' field '%s' Offset mismatch: expected %d, got %d", tableName, originalField.Name, originalField.Offset, loadedField.Offset)
			}
			if loadedField.Size != originalField.Size {
				t.Errorf("Table '%s' field '%s' Size mismatch: expected %d, got %d", tableName, originalField.Name, originalField.Size, loadedField.Size)
			}
		}

		// Verify field index map
		if len(loadedTable.RecordLayout.FieldIndex) != len(originalTable.RecordLayout.FieldIndex) {
			t.Errorf("Table '%s' FieldIndex count mismatch: expected %d, got %d", tableName, len(originalTable.RecordLayout.FieldIndex), len(loadedTable.RecordLayout.FieldIndex))
		}

		// Verify sets
		if len(loadedTable.Sets) != len(originalTable.Sets) {
			t.Errorf("Table '%s' Sets count mismatch: expected %d, got %d", tableName, len(originalTable.Sets), len(loadedTable.Sets))
		}

		for i, originalSet := range originalTable.Sets {
			if i >= len(loadedTable.Sets) {
				break
			}
			loadedSet := loadedTable.Sets[i]

			if loadedSet.Name != originalSet.Name {
				t.Errorf("Table '%s' set %d name mismatch: expected '%s', got '%s'", tableName, i, originalSet.Name, loadedSet.Name)
			}
			if loadedSet.MemberTableName != originalSet.MemberTableName {
				t.Errorf("Table '%s' set '%s' MemberTableName mismatch: expected '%s', got '%s'", tableName, originalSet.Name, originalSet.MemberTableName, loadedSet.MemberTableName)
			}
		}

		// Verify set index map
		if len(loadedTable.SetIndex) != len(originalTable.SetIndex) {
			t.Errorf("Table '%s' SetIndex count mismatch: expected %d, got %d", tableName, len(originalTable.SetIndex), len(loadedTable.SetIndex))
		}
	}

	t.Logf("Successfully saved and loaded DB definition for College schema with %d tables", len(loadedDBDef.Tables))
}
