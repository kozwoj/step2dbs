package db

import (
	"os"
	"path/filepath"
	"testing"
)

func TestOpenDB_CollegeSchema(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Step 1: Create the database first
	t.Log("Creating database from College.ddl schema...")
	err := CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Verify database directory exists
	dbDir := filepath.Join(tempDir, "College")
	if _, err := os.Stat(dbDir); os.IsNotExist(err) {
		t.Fatalf("Expected database directory to exist at '%s'", dbDir)
	}

	// Step 2: Open the database
	t.Log("Opening database...")
	err = OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer CloseDB()

	// Step 3: Verify the database definition was loaded
	dbDef := Definition()
	if dbDef == nil {
		t.Fatal("Expected non-nil DBDefinition")
	}

	if dbDef.Name != "College" {
		t.Errorf("Expected DB name 'College', got '%s'", dbDef.Name)
	}

	if dbDef.DirPath != dbDir {
		t.Errorf("Expected DB dir path '%s', got '%s'", dbDir, dbDef.DirPath)
	}

	t.Logf("Database loaded: %s at %s", dbDef.Name, dbDef.DirPath)
	t.Logf("Created on: %s", dbDef.CreatedOn)
	t.Logf("Number of tables: %d", len(dbDef.Tables))

	// Step 4: Verify schema.json file was opened
	if dbDef.SchemaFile == nil {
		t.Error("Expected schema.json file to be opened")
	}

	// Step 5: Verify all tables are loaded
	expectedTables := []string{
		"Departments",
		"Teachers",
		"Students",
		"Courses",
		"Classes",
		"Grades",
		"Majors",
	}

	if len(dbDef.Tables) != len(expectedTables) {
		t.Errorf("Expected %d tables, found %d", len(expectedTables), len(dbDef.Tables))
	}

	// Step 6: Verify each table has its files opened
	for _, tableName := range expectedTables {
		tableIdx, exists := dbDef.TableIndex[tableName]
		if !exists {
			t.Errorf("Table '%s' not found in TableIndex", tableName)
			continue
		}

		table := dbDef.Tables[tableIdx]
		t.Logf("Checking table: %s", table.Name)

		// Verify record file is opened
		if table.RecordFile == nil {
			t.Errorf("Table '%s': RecordFile is nil", tableName)
		} else {
			t.Logf("  - RecordFile opened: YES")
		}

		// Verify primary index is opened if table has a primary key
		if table.Key != -1 {
			if table.PrimeIndex == nil {
				t.Errorf("Table '%s': Has primary key (field %d) but PrimeIndex is nil", tableName, table.Key)
			} else {
				pkField := table.RecordLayout.Fields[table.Key]
				t.Logf("  - PrimeIndex opened: YES (key: %s)", pkField.Name)
			}
		} else {
			t.Logf("  - PrimeIndex: N/A (no primary key)")
		}

		// Verify set member files are opened
		if len(table.Sets) > 0 {
			for _, set := range table.Sets {
				if set.MembersFile == nil {
					t.Errorf("Table '%s': Set '%s' MembersFile is nil", tableName, set.Name)
				} else {
					t.Logf("  - Set '%s' opened: YES (member table: %s)", set.Name, set.MemberTableName)
				}
			}
		} else {
			t.Logf("  - Sets: N/A (no sets)")
		}

		// Verify dictionaries are opened for STRING fields
		stringFieldCount := 0
		for _, field := range table.RecordLayout.Fields {
			if field.Type == STRING {
				stringFieldCount++
				if field.Dictionary == nil {
					t.Errorf("Table '%s': STRING field '%s' Dictionary is nil", tableName, field.Name)
				} else {
					t.Logf("  - Dictionary for field '%s' opened: YES", field.Name)
				}
			}
		}
		if stringFieldCount == 0 {
			t.Logf("  - Dictionaries: N/A (no STRING fields)")
		}
	}

	// Step 7: Verify global definition was initialized
	globalDef := Definition()
	if globalDef == nil {
		t.Error("Global DBDefinition was not initialized")
	} else if globalDef.Name != "College" {
		t.Errorf("Global DBDefinition has wrong name: expected 'College', got '%s'", globalDef.Name)
	} else {
		t.Logf("Global DBDefinition initialized correctly")
	}

	// Step 8: Test CloseDB
	t.Log("Closing database...")
	CloseDB()

	// Verify files are closed (should be safe to call twice)
	CloseDB()

	// Verify global definition was reset
	if DefinitionInitialized() {
		t.Error("Global DBDefinition should be reset after CloseDB")
	}
	t.Log("Database closed successfully")
}

func TestOpenDB_NonExistentDirectory(t *testing.T) {
	// Try to open a database that doesn't exist
	dbDir := filepath.Join(t.TempDir(), "NonExistent")

	err := OpenDB(dbDir)
	if err == nil {
		defer CloseDB()
		t.Fatal("Expected error when opening non-existent database, got nil")
	}

	t.Logf("Expected error received: %v", err)
}

func TestOpenDB_NotADirectory(t *testing.T) {
	// Create a file instead of a directory
	tempDir := t.TempDir()
	filePath := filepath.Join(tempDir, "notadir.txt")
	err := os.WriteFile(filePath, []byte("test"), 0644)
	if err != nil {
		t.Fatalf("Failed to create test file: %v", err)
	}

	err = OpenDB(filePath)
	if err == nil {
		defer CloseDB()
		t.Fatal("Expected error when opening a file as database directory, got nil")
	}

	t.Logf("Expected error received: %v", err)
}

func TestOpenDB_MissingSchemaFile(t *testing.T) {
	// Create directory without schema.json
	dbDir := t.TempDir()
	emptyDBDir := filepath.Join(dbDir, "EmptyDB")
	err := os.Mkdir(emptyDBDir, 0755)
	if err != nil {
		t.Fatalf("Failed to create empty directory: %v", err)
	}

	err = OpenDB(emptyDBDir)
	if err == nil {
		defer CloseDB()
		t.Fatal("Expected error when schema.json is missing, got nil")
	}

	t.Logf("Expected error received: %v", err)
}

func TestOpenDB_CustomerEmployeeSchema(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the Customer_Employee.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl")

	// Step 1: Create the database
	t.Log("Creating database from Customer_Employee.ddl schema...")
	err := CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Step 2: Open the database
	dbDir := filepath.Join(tempDir, "TestSchema")
	t.Log("Opening database...")
	err = OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer CloseDB()

	// Step 3: Verify basic properties
	dbDef := Definition()
	if dbDef.Name != "TestSchema" {
		t.Errorf("Expected DB name 'TestSchema', got '%s'", dbDef.Name)
	}

	t.Logf("Database loaded: %s", dbDef.Name)
	t.Logf("Number of tables: %d", len(dbDef.Tables))

	// Step 4: Verify all tables are loaded with their resources
	expectedTables := []string{"Customers", "Employees"}

	if len(dbDef.Tables) != len(expectedTables) {
		t.Errorf("Expected %d tables, found %d", len(expectedTables), len(dbDef.Tables))
	}

	for _, tableName := range expectedTables {
		tableIdx, exists := dbDef.TableIndex[tableName]
		if !exists {
			t.Errorf("Table '%s' not found", tableName)
			continue
		}

		table := dbDef.Tables[tableIdx]
		t.Logf("Table '%s': RecordFile=%v, PrimeIndex=%v, Sets=%d",
			table.Name,
			table.RecordFile != nil,
			table.PrimeIndex != nil,
			len(table.Sets))
	}

	// Step 5: Verify STRING field StringSizeLimit persists through JSON serialization
	customersIdx, exists := dbDef.TableIndex["Customers"]
	if !exists {
		t.Fatal("Expected 'Customers' table to exist")
	}
	customersTable := dbDef.Tables[customersIdx]

	// Verify Company_name STRING(40)
	companyNameField := customersTable.RecordLayout.Fields[1]
	if companyNameField.Name != "Company_name" {
		t.Errorf("Expected field 1 to be 'Company_name', got '%s'", companyNameField.Name)
	}
	if companyNameField.Type != STRING {
		t.Errorf("Expected Company_name type STRING, got %d", companyNameField.Type)
	}
	if companyNameField.Size != 4 {
		t.Errorf("Expected Company_name Size 4 (serialization), got %d", companyNameField.Size)
	}
	if companyNameField.StringSizeLimit != 40 {
		t.Errorf("Expected Company_name StringSizeLimit 40, got %d", companyNameField.StringSizeLimit)
	}

	// Verify Contact_name STRING(30)
	contactNameField := customersTable.RecordLayout.Fields[2]
	if contactNameField.Name != "Contact_name" {
		t.Errorf("Expected field 2 to be 'Contact_name', got '%s'", contactNameField.Name)
	}
	if contactNameField.StringSizeLimit != 30 {
		t.Errorf("Expected Contact_name StringSizeLimit 30, got %d", contactNameField.StringSizeLimit)
	}

	t.Log("StringSizeLimit field correctly persisted through JSON serialization/deserialization")

	t.Log("Database opened and verified successfully")
}

func TestOpenDB_AlreadyOpen(t *testing.T) {
	// Setup: Create a temporary directory for the test DB
	tempDir := t.TempDir()

	// Path to the College.ddl schema file
	schemaFile := filepath.Join("..", "docs", "testdata", "College.ddl")

	// Step 1: Create the database
	t.Log("Creating database from College.ddl schema...")
	err := CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	// Step 2: Open the database
	dbDir := filepath.Join(tempDir, "College")
	t.Log("Opening database...")
	err = OpenDB(dbDir)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer CloseDB()

	// Step 3: Try to open again without closing - should fail
	t.Log("Attempting to reopen database...")
	err = OpenDB(dbDir)
	if err == nil {
		t.Fatal("Expected error when trying to reopen database, got nil")
	}

	t.Logf("Expected error received: %v", err)

	// Step 4: Close and reopen should work
	t.Log("Closing database...")
	CloseDB()

	t.Log("Reopening database after close...")
	err = OpenDB(dbDir)
	if err != nil {
		t.Fatalf("Failed to reopen database after close: %v", err)
	}
	defer CloseDB()

	t.Log("Database successfully reopened after close")
}
