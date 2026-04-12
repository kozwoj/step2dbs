package db

import (
	"os"
	"path/filepath"
	"testing"
	// "time" // Uncomment if you want to use the sleep for debugging
)

func TestCreateDirectoryTree_CollegeSchema(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Step 1: Create DB definition from College.ddl
	dbDef, err := CreateDBDefinition(schemaFile, tempDir)
	if err != nil {
		t.Fatalf("CreateDBDefinition failed: %v", err)
	}

	// Verify the DB root directory was created
	dbRootDir := filepath.Join(tempDir, "College")
	if _, err := os.Stat(dbRootDir); os.IsNotExist(err) {
		t.Fatalf("Expected DB root directory to exist at '%s'", dbRootDir)
	}

	// Step 2: Call CreateDirectoryTree
	err = CreateDirectoryTree(dbDef, dbRootDir)
	if err != nil {
		t.Fatalf("CreateDirectoryTree failed: %v", err)
	}

	// Step 3: Verify all table directories were created
	expectedTables := []string{
		"Departments",
		"Teachers",
		"Students",
		"Courses",
		"Classes",
		"Grades",
		"Majors",
	}

	for _, tableName := range expectedTables {
		tableDirPath := filepath.Join(dbRootDir, tableName)
		info, err := os.Stat(tableDirPath)
		if os.IsNotExist(err) {
			t.Errorf("Expected table directory '%s' to exist at '%s'", tableName, tableDirPath)
			continue
		}
		if err != nil {
			t.Errorf("Error checking table directory '%s': %v", tableName, err)
			continue
		}
		if !info.IsDir() {
			t.Errorf("Expected '%s' to be a directory", tableDirPath)
		}
	}

	// Step 4: Verify the number of directories created matches the number of tables
	entries, err := os.ReadDir(dbRootDir)
	if err != nil {
		t.Fatalf("Failed to read DB root directory: %v", err)
	}

	dirCount := 0
	for _, entry := range entries {
		if entry.IsDir() {
			dirCount++
		}
	}

	if dirCount != len(expectedTables) {
		t.Errorf("Expected %d table directories, found %d", len(expectedTables), dirCount)
	}

	t.Logf("Successfully created directory tree with %d table directories", dirCount)

	// Log the directory structure for verification
	t.Logf("Directory structure:")
	t.Logf("  %s/", dbRootDir)
	for _, entry := range entries {
		if entry.IsDir() {
			t.Logf("    %s/", entry.Name())
		}
	}
}

func TestCreateDirectoryTree_EmptyDBDefinition(t *testing.T) {
	// Setup: Create a temporary directory
	tempDir := t.TempDir()
	dbRootDir := filepath.Join(tempDir, "EmptyDB")

	// Create the root directory
	err := os.Mkdir(dbRootDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create root directory: %v", err)
	}

	// Create an empty DBDefinition with no tables
	dbDef := &DBDefinition{
		Name:       "EmptyDB",
		DirPath:    dbRootDir,
		Tables:     []*TableDescription{},
		TableIndex: make(map[string]int),
	}

	// Call CreateDirectoryTree - should succeed with no directories created
	err = CreateDirectoryTree(dbDef, dbRootDir)
	if err != nil {
		t.Fatalf("CreateDirectoryTree failed: %v", err)
	}

	// Verify no directories were created
	entries, err := os.ReadDir(dbRootDir)
	if err != nil {
		t.Fatalf("Failed to read DB root directory: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Expected no subdirectories, found %d entries", len(entries))
	}

	t.Logf("Successfully handled empty DBDefinition with no table directories created")
}

func TestCreateDirectoryTree_NonExistentRootDir(t *testing.T) {
	// Setup: Use a non-existent directory path
	tempDir := t.TempDir()
	nonExistentDir := filepath.Join(tempDir, "NonExistentDB")

	// Create a minimal DBDefinition
	dbDef := &DBDefinition{
		Name:    "TestDB",
		DirPath: nonExistentDir,
		Tables: []*TableDescription{
			{Name: "TestTable"},
		},
		TableIndex: map[string]int{"TestTable": 0},
	}

	// Call CreateDirectoryTree with non-existent root directory
	// This should fail because the root directory doesn't exist
	err := CreateDirectoryTree(dbDef, nonExistentDir)
	if err == nil {
		t.Fatal("Expected CreateDirectoryTree to fail with non-existent root directory, but it succeeded")
	}

	t.Logf("CreateDirectoryTree correctly failed with error: %v", err)
}

func TestCreateFilesAndDictionaries_CollegeSchema(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Step 1: Create DB definition from College.ddl
	dbDef, err := CreateDBDefinition(schemaFile, tempDir)
	if err != nil {
		t.Fatalf("CreateDBDefinition failed: %v", err)
	}

	dbRootDir := filepath.Join(tempDir, "College")

	// Step 2: Create directory tree
	err = CreateDirectoryTree(dbDef, dbRootDir)
	if err != nil {
		t.Fatalf("CreateDirectoryTree failed: %v", err)
	}

	// Step 3: Call CreateFilesAndDictionaries
	err = CreateFilesAndDictionaries(dbDef, dbRootDir)
	if err != nil {
		t.Fatalf("CreateFilesAndDictionaries failed: %v", err)
	}

	// Step 4: Verify files and directories were created for each table

	// Test table-by-table file creation
	testCases := []struct {
		tableName     string
		hasPrimaryKey bool
		numSets       int
		stringFields  []string
	}{
		{"Departments", true, 1, []string{"Name", "Building_name"}},
		{"Teachers", true, 2, []string{"First_name", "Last_name"}},
			{"Students", true, 2, []string{"First_name", "Last_name", "Preferred_name", "State_or_Country"}},
		{"Courses", true, 0, []string{"Name", "Description"}},
			{"Classes", true, 1, []string{"Place_times"}},
		{"Grades", false, 0, []string{}},
		{"Majors", true, 0, []string{"Description"}},
	}

	for _, tc := range testCases {
		t.Run(tc.tableName, func(t *testing.T) {
			tableDirPath := filepath.Join(dbRootDir, tc.tableName)

			// Verify records.dat exists
			recordsPath := filepath.Join(tableDirPath, "records.dat")
			if _, err := os.Stat(recordsPath); os.IsNotExist(err) {
				t.Errorf("Expected records.dat to exist at '%s'", recordsPath)
			} else {
				t.Logf("✓ records.dat created for %s", tc.tableName)
			}

			// Verify primindex.dat exists if table has primary key
			primindexPath := filepath.Join(tableDirPath, "primindex.dat")
			if tc.hasPrimaryKey {
				if _, err := os.Stat(primindexPath); os.IsNotExist(err) {
					t.Errorf("Expected primindex.dat to exist at '%s'", primindexPath)
				} else {
					t.Logf("✓ primindex.dat created for %s", tc.tableName)
				}
			} else {
				if _, err := os.Stat(primindexPath); !os.IsNotExist(err) {
					t.Errorf("primindex.dat should not exist for table without primary key at '%s'", primindexPath)
				} else {
					t.Logf("✓ No primindex.dat for %s (no primary key)", tc.tableName)
				}
			}

			// Verify set files exist
			tableIdx := dbDef.TableIndex[tc.tableName]
			table := dbDef.Tables[tableIdx]
			if len(table.Sets) != tc.numSets {
				t.Errorf("Expected %d sets for %s, got %d", tc.numSets, tc.tableName, len(table.Sets))
			}
			for _, set := range table.Sets {
				setPath := filepath.Join(tableDirPath, set.Name+".dat")
				if _, err := os.Stat(setPath); os.IsNotExist(err) {
					t.Errorf("Expected set file '%s' to exist at '%s'", set.Name, setPath)
				} else {
					t.Logf("✓ Set file %s.dat created for %s", set.Name, tc.tableName)
				}
			}

			// Verify dictionary directories and files exist for STRING fields
			for _, fieldName := range tc.stringFields {
				dictDirPath := filepath.Join(tableDirPath, fieldName)

				// Check if dictionary directory exists
				if info, err := os.Stat(dictDirPath); os.IsNotExist(err) {
					t.Errorf("Expected dictionary directory '%s' to exist at '%s'", fieldName, dictDirPath)
					continue
				} else if !info.IsDir() {
					t.Errorf("Expected '%s' to be a directory", dictDirPath)
					continue
				}

				// Verify all 5 dictionary files exist
				dictFiles := []string{"strings.dat", "offsets.dat", "postings.dat", "index.dat", "prefix.dat"}
				for _, fileName := range dictFiles {
					filePath := filepath.Join(dictDirPath, fileName)
					if _, err := os.Stat(filePath); os.IsNotExist(err) {
						t.Errorf("Expected dictionary file '%s' to exist at '%s'", fileName, filePath)
					}
				}
				t.Logf("✓ Dictionary for field %s created in %s", fieldName, tc.tableName)
			}
		})
	}

	// Step 5: Count total files created
	totalRecordsFiles := 0
	totalPrimaryIndexFiles := 0
	totalSetFiles := 0
	totalDictionaryDirs := 0

	for _, table := range dbDef.Tables {
		tableDirPath := filepath.Join(dbRootDir, table.Name)

		// Count records.dat
		if _, err := os.Stat(filepath.Join(tableDirPath, "records.dat")); err == nil {
			totalRecordsFiles++
		}

		// Count primindex.dat
		if _, err := os.Stat(filepath.Join(tableDirPath, "primindex.dat")); err == nil {
			totalPrimaryIndexFiles++
		}

		// Count set files
		totalSetFiles += len(table.Sets)

		// Count dictionary directories (STRING fields)
		for _, field := range table.RecordLayout.Fields {
			if field.Type == STRING {
				totalDictionaryDirs++
			}
		}
	}

	t.Logf("\nSummary:")
	t.Logf("  Total tables: %d", len(dbDef.Tables))
	t.Logf("  Records files (records.dat): %d", totalRecordsFiles)
	t.Logf("  Primary index files (primindex.dat): %d", totalPrimaryIndexFiles)
	t.Logf("  Set files: %d", totalSetFiles)
	t.Logf("  Dictionary directories: %d", totalDictionaryDirs)

	// Expected values for College schema
	if totalRecordsFiles != 7 {
		t.Errorf("Expected 7 records files, got %d", totalRecordsFiles)
	}
	if totalPrimaryIndexFiles != 6 {
		t.Errorf("Expected 6 primary index files (Grades has no primary key), got %d", totalPrimaryIndexFiles)
	}
	if totalSetFiles != 6 {
		t.Errorf("Expected 6 set files (Departments:1, Teachers:2, Students:2, Classes:1), got %d", totalSetFiles)
	}
	if totalDictionaryDirs != 12 {
		t.Errorf("Expected 12 dictionary directories, got %d", totalDictionaryDirs)
	}
}

func TestCreateDB_CollegeSchema(t *testing.T) {
	// Setup: Create a temporary directory
	tempDir := t.TempDir()
	t.Logf("Test database location: %s", tempDir)

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Call CreateDB - this should create everything
	err := CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Verify the DB root directory was created with schema name
	dbRootDir := filepath.Join(tempDir, "College")
	t.Logf("DB root directory: %s", dbRootDir)
	if _, err := os.Stat(dbRootDir); os.IsNotExist(err) {
		t.Fatalf("Expected DB root directory to exist at '%s'", dbRootDir)
	}

	// List what's in the DB root directory
	entries, err := os.ReadDir(dbRootDir)
	if err != nil {
		t.Fatalf("Failed to read DB root directory: %v", err)
	}
	t.Logf("Contents of DB root directory (%d items):", len(entries))
	for _, entry := range entries {
		if entry.IsDir() {
			t.Logf("  [DIR]  %s", entry.Name())
		} else {
			t.Logf("  [FILE] %s", entry.Name())
		}
	}

	// Verify schema.json was created
	schemaJSONPath := filepath.Join(dbRootDir, "schema.json")
	if _, err := os.Stat(schemaJSONPath); os.IsNotExist(err) {
		t.Errorf("Expected schema.json to exist at '%s'", schemaJSONPath)
	} else {
		t.Logf("✓ schema.json created")
	}

	// Verify all table directories exist
	expectedTables := []string{"Departments", "Teachers", "Students", "Courses", "Classes", "Grades", "Majors"}
	for _, tableName := range expectedTables {
		tableDirPath := filepath.Join(dbRootDir, tableName)
		if info, err := os.Stat(tableDirPath); os.IsNotExist(err) {
			t.Errorf("Expected table directory '%s' to exist", tableName)
		} else if !info.IsDir() {
			t.Errorf("Expected '%s' to be a directory", tableName)
		}
	}
	t.Logf("✓ All %d table directories created", len(expectedTables))

	// Verify key files exist for each table
	filesVerified := 0
	for _, tableName := range expectedTables {
		tableDirPath := filepath.Join(dbRootDir, tableName)

		// Check records.dat
		if _, err := os.Stat(filepath.Join(tableDirPath, "records.dat")); err == nil {
			filesVerified++
		}

		// Check primindex.dat (if not Grades table)
		if tableName != "Grades" {
			if _, err := os.Stat(filepath.Join(tableDirPath, "primindex.dat")); err == nil {
				filesVerified++
			}
		}
	}
	t.Logf("✓ Verified %d essential files created", filesVerified)

	// Verify sample dictionaries exist
	deptNameDict := filepath.Join(dbRootDir, "Departments", "Name")
	if info, err := os.Stat(deptNameDict); os.IsNotExist(err) {
		t.Errorf("Expected dictionary directory for Departments.Name at '%s'", deptNameDict)
	} else if !info.IsDir() {
		t.Errorf("Expected '%s' to be a directory", deptNameDict)
	} else {
		// Check dictionary files
		dictFiles := []string{"strings.dat", "offsets.dat", "postings.dat", "index.dat", "prefix.dat"}
		allExist := true
		for _, fileName := range dictFiles {
			if _, err := os.Stat(filepath.Join(deptNameDict, fileName)); os.IsNotExist(err) {
				allExist = false
				break
			}
		}
		if allExist {
			t.Logf("✓ Dictionary files verified for Departments.Name")
		}
	}

	// Verify sample set file exists
	facultySetPath := filepath.Join(dbRootDir, "Departments", "Faculty.dat")
	if _, err := os.Stat(facultySetPath); os.IsNotExist(err) {
		t.Errorf("Expected set file Faculty.dat at '%s'", facultySetPath)
	} else {
		t.Logf("✓ Set file Faculty.dat verified")
	}

	// Load and verify schema.json can be read back
	loadedDef := &DBDefinition{}
	err = LoadDefinitionFromJson(schemaJSONPath, loadedDef)
	if err != nil {
		t.Errorf("Failed to load schema.json: %v", err)
	} else {
		if loadedDef.Name != "College" {
			t.Errorf("Expected loaded schema name 'College', got '%s'", loadedDef.Name)
		}
		if len(loadedDef.Tables) != 7 {
			t.Errorf("Expected 7 tables in loaded schema, got %d", len(loadedDef.Tables))
		} else {
			t.Logf("✓ schema.json successfully loaded with %d tables", len(loadedDef.Tables))
		}
	}

	t.Logf("\n✅ CreateDB successfully created complete College database")

	// Uncomment the line below to pause and inspect the directory before cleanup:
	// time.Sleep(60 * time.Second)
}
