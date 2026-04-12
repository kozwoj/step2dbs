package step2DDLparser

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestParser_SimpleSchema(t *testing.T) {
	input := `
SCHEMA TestSchema

TABLE Users (
    id INT PRIMARY KEY,
    name STRING(50)
);
`
	schema, err := ParseSchema(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if schema.Name != "TestSchema" {
		t.Errorf("expected schema name 'TestSchema', got '%s'", schema.Name)
	}

	if len(schema.Tables) != 1 {
		t.Fatalf("expected 1 table, got %d", len(schema.Tables))
	}

	table := schema.Tables[0]
	if table.Name != "Users" {
		t.Errorf("expected table name 'Users', got '%s'", table.Name)
	}

	if len(table.Columns) != 2 {
		t.Fatalf("expected 2 columns, got %d", len(table.Columns))
	}

	// Check first column
	col1 := table.Columns[0]
	if col1.Name != "id" {
		t.Errorf("expected column name 'id', got '%s'", col1.Name)
	}
	if col1.Type != 2 { // INT
		t.Errorf("expected type 2 (INT), got %d", col1.Type)
	}
	if len(col1.Constraints) != 1 || !col1.Constraints[0].IsPrimaryKey {
		t.Errorf("expected PRIMARY KEY constraint")
	}

	// Check second column
	col2 := table.Columns[1]
	if col2.Name != "name" {
		t.Errorf("expected column name 'name', got '%s'", col2.Name)
	}
	if col2.Type != 6 { // STRING
		t.Errorf("expected type 6 (STRING), got %d", col2.Type)
	}
	if col2.SizeLimit != 50 {
		t.Errorf("expected size limit 50, got %d", col2.SizeLimit)
	}
}

func TestParser_DataTypes(t *testing.T) {
	tests := []struct {
		name          string
		columnDef     string
		expectedType  uint8
		expectedLimit int
	}{
		{"SMALLINT", "col SMALLINT", 1, 0},
		{"INT", "col INT", 2, 0},
		{"BIGINT", "col BIGINT", 3, 0},
		{"DECIMAL", "col DECIMAL", 4, 0},
		{"FLOAT", "col FLOAT", 5, 0},
		{"STRING no limit", "col STRING", 6, 0},
		{"STRING with limit", "col STRING(100)", 6, 100},
		{"CHAR array", "col CHAR[20]", 7, 20},
		{"BOOLEAN", "col BOOLEAN", 8, 0},
		{"DATE", "col DATE", 9, 0},
		{"TIME", "col TIME", 10, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			input := "SCHEMA Test\nTABLE T (" + tt.columnDef + ");"
			schema, err := ParseSchema(input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}

			col := schema.Tables[0].Columns[0]
			if col.Type != tt.expectedType {
				t.Errorf("expected type %d, got %d", tt.expectedType, col.Type)
			}
			if col.SizeLimit != tt.expectedLimit {
				t.Errorf("expected size limit %d, got %d", tt.expectedLimit, col.SizeLimit)
			}
		})
	}
}

func TestParser_Constraints(t *testing.T) {
	t.Run("PRIMARY KEY", func(t *testing.T) {
		input := `SCHEMA Test
TABLE T (id INT PRIMARY KEY);`

		schema, err := ParseSchema(input)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		col := schema.Tables[0].Columns[0]
		if len(col.Constraints) != 1 {
			t.Fatalf("expected 1 constraint, got %d", len(col.Constraints))
		}
		if !col.Constraints[0].IsPrimaryKey {
			t.Error("expected IsPrimaryKey to be true")
		}
		if col.Constraints[0].IsForeignKey {
			t.Error("expected IsForeignKey to be false")
		}
	})

	t.Run("FOREIGN KEY", func(t *testing.T) {
		input := `SCHEMA Test
TABLE T1 (id INT PRIMARY KEY);
TABLE T2 (ref INT FOREIGN KEY T1);`

		schema, err := ParseSchema(input)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		col := schema.Tables[1].Columns[0]
		if len(col.Constraints) != 1 {
			t.Fatalf("expected 1 constraint, got %d", len(col.Constraints))
		}
		if col.Constraints[0].IsPrimaryKey {
			t.Error("expected IsPrimaryKey to be false")
		}
		if !col.Constraints[0].IsForeignKey {
			t.Error("expected IsForeignKey to be true")
		}
		if col.Constraints[0].TableName != "T1" {
			t.Errorf("expected foreign key table 'T1', got '%s'", col.Constraints[0].TableName)
		}
	})

	t.Run("No constraint", func(t *testing.T) {
		input := `SCHEMA Test
TABLE T (name STRING);`

		schema, err := ParseSchema(input)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		col := schema.Tables[0].Columns[0]
		if len(col.Constraints) != 0 {
			t.Errorf("expected no constraints, got %d", len(col.Constraints))
		}
	})

	t.Run("OPTIONAL", func(t *testing.T) {
		input := `SCHEMA Test
TABLE T (id INT PRIMARY KEY, nickname STRING OPTIONAL);`

		schema, err := ParseSchema(input)
		if err != nil {
			t.Fatalf("parse error: %v", err)
		}

		col := schema.Tables[0].Columns[1]
		if len(col.Constraints) != 1 {
			t.Fatalf("expected 1 constraint, got %d", len(col.Constraints))
		}
		if col.Constraints[0].IsPrimaryKey {
			t.Error("expected IsPrimaryKey to be false")
		}
		if col.Constraints[0].IsForeignKey {
			t.Error("expected IsForeignKey to be false")
		}
		if !col.Constraints[0].IsOptional {
			t.Error("expected IsOptional to be true")
		}
	})
}

func TestParser_Sets(t *testing.T) {
	input := `SCHEMA Test
TABLE T1 (id INT PRIMARY KEY);
TABLE T2 (id INT) SETS (S1 T1, S2 T1);`

	schema, err := ParseSchema(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	table := schema.Tables[1]
	if len(table.Sets) != 2 {
		t.Fatalf("expected 2 sets, got %d", len(table.Sets))
	}

	if table.Sets[0].Name != "S1" {
		t.Errorf("expected set name 'S1', got '%s'", table.Sets[0].Name)
	}
	if table.Sets[0].TableName != "T1" {
		t.Errorf("expected table name 'T1', got '%s'", table.Sets[0].TableName)
	}

	if table.Sets[1].Name != "S2" {
		t.Errorf("expected set name 'S2', got '%s'", table.Sets[1].Name)
	}
	if table.Sets[1].TableName != "T1" {
		t.Errorf("expected table name 'T1', got '%s'", table.Sets[1].TableName)
	}
}

func TestParser_MultipleTables(t *testing.T) {
	input := `SCHEMA Test
TABLE T1 (id INT);
TABLE T2 (name STRING);
TABLE T3 (value FLOAT);`

	schema, err := ParseSchema(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	if len(schema.Tables) != 3 {
		t.Fatalf("expected 3 tables, got %d", len(schema.Tables))
	}

	expectedNames := []string{"T1", "T2", "T3"}
	for i, expected := range expectedNames {
		if schema.Tables[i].Name != expected {
			t.Errorf("table %d: expected name '%s', got '%s'", i, expected, schema.Tables[i].Name)
		}
	}
}

func TestParser_MultipleColumns(t *testing.T) {
	input := `SCHEMA Test
TABLE T (
    col1 INT,
    col2 STRING(20),
    col3 CHAR[10],
    col4 DATE
);`

	schema, err := ParseSchema(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	table := schema.Tables[0]
	if len(table.Columns) != 4 {
		t.Fatalf("expected 4 columns, got %d", len(table.Columns))
	}

	expectedNames := []string{"col1", "col2", "col3", "col4"}
	for i, expected := range expectedNames {
		if table.Columns[i].Name != expected {
			t.Errorf("column %d: expected name '%s', got '%s'", i, expected, table.Columns[i].Name)
		}
	}
}

func TestParser_TestDataFile(t *testing.T) {
	data, err := os.ReadFile("test_data.ddl")
	if err != nil {
		t.Skipf("test_data.ddl not found: %v", err)
	}

	input := string(data)
	schema, err := ParseSchema(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Verify schema name
	if schema.Name != "TestSchema" {
		t.Errorf("expected schema name 'TestSchema', got '%s'", schema.Name)
	}

	// Verify we have 2 tables
	if len(schema.Tables) != 2 {
		t.Fatalf("expected 2 tables, got %d", len(schema.Tables))
	}

	// Check Customers table
	customers := schema.Tables[0]
	if customers.Name != "Customers" {
		t.Errorf("expected first table 'Customers', got '%s'", customers.Name)
	}
	if len(customers.Columns) != 11 {
		t.Errorf("expected 11 columns in Customers, got %d", len(customers.Columns))
	}
	if len(customers.Sets) != 1 {
		t.Errorf("expected 1 set in Customers, got %d", len(customers.Sets))
	}
	if customers.Sets[0].Name != "Reps" {
		t.Errorf("expected set name 'Reps', got '%s'", customers.Sets[0].Name)
	}

	// Check Employees table
	employees := schema.Tables[1]
	if employees.Name != "Employees" {
		t.Errorf("expected second table 'Employees', got '%s'", employees.Name)
	}
	if len(employees.Columns) != 10 {
		t.Errorf("expected 10 columns in Employees, got %d", len(employees.Columns))
		return
	}

	// Verify FOREIGN KEY constraint
	reportsToCol := employees.Columns[9]
	if reportsToCol.Name != "Reports_to" {
		t.Errorf("expected column 'Reports_to', got '%s'", reportsToCol.Name)
	}
	if len(reportsToCol.Constraints) != 1 || !reportsToCol.Constraints[0].IsForeignKey {
		t.Error("expected FOREIGN KEY constraint on Reports_to")
	}
	if reportsToCol.Constraints[0].TableName != "Employees" {
		t.Errorf("expected foreign key to 'Employees', got '%s'", reportsToCol.Constraints[0].TableName)
	}
}

func TestParser_ErrorCases(t *testing.T) {
	tests := []struct {
		name  string
		input string
	}{
		{"Missing SCHEMA keyword", "TABLE T (id INT);"},
		{"Missing schema name", "SCHEMA TABLE T (id INT);"},
		{"Missing TABLE keyword", "SCHEMA Test T (id INT);"},
		{"Missing table name", "SCHEMA Test TABLE (id INT);"},
		{"Missing left paren", "SCHEMA Test TABLE T id INT);"},
		{"Missing right paren", "SCHEMA Test TABLE T (id INT;"},
		{"Missing semicolon", "SCHEMA Test TABLE T (id INT)"},
		{"Missing column type", "SCHEMA Test TABLE T (id);"},
		{"Invalid PRIMARY KEY", "SCHEMA Test TABLE T (id INT PRIMARY);"},
		{"Invalid FOREIGN KEY", "SCHEMA Test TABLE T (id INT FOREIGN);"},
		{"Missing FOREIGN KEY table", "SCHEMA Test TABLE T (id INT FOREIGN KEY);"},
		{"Empty table", "SCHEMA Test TABLE T ();"},
		{"Missing comma between columns", "SCHEMA Test TABLE T (id INT name STRING);"},
		{"Missing table definition", "SCHEMA Test"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSchema(tt.input)
			if err == nil {
				t.Error("expected parse error, got nil")
			}
		})
	}
}

func TestParser_UnknownTokenError(t *testing.T) {
	tests := []struct {
		name  string
		input string
		char  string
	}{
		{"Dollar sign in table name", "SCHEMA Test TABLE Test$ (id INT);", "$"},
		{"At sign in column name", "SCHEMA Test TABLE T (@id INT);", "@"},
		{"Hash in type", "SCHEMA Test TABLE T (id INT#);", "#"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSchema(tt.input)
			if err == nil {
				t.Fatal("expected error for UNKNOWN token, got nil")
			}

			// Verify error mentions the invalid character
			if !strings.Contains(err.Error(), tt.char) {
				t.Errorf("expected error to mention '%s', got: %v", tt.char, err)
			}
		})
	}
}

func TestParser_BetterErrorForInvalidSets(t *testing.T) {
	// Test that typo in SETS keyword gives helpful error message
	input := `SCHEMA Test
TABLE T (id INT)
SETSss (s1 T2);`

	_, err := ParseSchema(input)
	if err == nil {
		t.Fatal("expected error for 'SETSss', got nil")
	}

	errMsg := err.Error()
	// Should mention both SETS and ; as options
	if !strings.Contains(errMsg, "SETS") || !strings.Contains(errMsg, ";") {
		t.Errorf("expected error to mention 'SETS' or ';', got: %v", errMsg)
	}
	// Should mention the found token
	if !strings.Contains(errMsg, "SETSss") {
		t.Errorf("expected error to mention 'SETSss', got: %v", errMsg)
	}
}

func TestParser_BetterErrorForInvalidConstraint(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		typo        string
		shouldCatch bool
	}{
		{"Typo in PRIMARY", "SCHEMA Test TABLE T (id INT PRIMARYKEY);", "PRIMARYKEY", true},
		{"Typo in PRIMARY 2", "SCHEMA Test TABLE T (id INT PRIMARYY KEY);", "PRIMARYY", true},
		{"Typo in FOREIGN", "SCHEMA Test TABLE T (id INT FOREIGNKEY T2);", "FOREIGNKEY", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ParseSchema(tt.input)
			if err == nil {
				t.Fatalf("expected error for '%s', got nil", tt.typo)
			}

			errMsg := err.Error()
			if tt.shouldCatch {
				// Should mention constraint options
				if !strings.Contains(errMsg, "PRIMARY") && !strings.Contains(errMsg, "FOREIGN") {
					t.Errorf("expected error to mention constraint keywords, got: %v", errMsg)
				}
			}
		})
	}
}

func TestParser_CaseInsensitive(t *testing.T) {
	tests := []string{
		"SCHEMA Test TABLE T (id INT);",
		"schema Test table T (id int);",
		"Schema Test Table T (id Int);",
		"SCHEMA Test TABLE T (id INT PRIMARY KEY);",
		"schema Test table T (id int primary key);",
	}

	for i, input := range tests {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			schema, err := ParseSchema(input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if schema.Name != "Test" {
				t.Errorf("expected schema name 'Test', got '%s'", schema.Name)
			}
		})
	}
}

func TestParser_WhitespaceHandling(t *testing.T) {
	tests := []string{
		"SCHEMA Test\nTABLE T(id INT);",
		"SCHEMA Test\n\nTABLE T (id INT);",
		"SCHEMA  Test  TABLE  T  (  id  INT  )  ;",
		"SCHEMA\tTest\nTABLE\tT\n(\n\tid\tINT\n)\n;",
	}

	for i, input := range tests {
		t.Run(string(rune('A'+i)), func(t *testing.T) {
			schema, err := ParseSchema(input)
			if err != nil {
				t.Fatalf("parse error: %v", err)
			}
			if schema.Name != "Test" {
				t.Errorf("expected schema name 'Test', got '%s'", schema.Name)
			}
			if len(schema.Tables) != 1 {
				t.Errorf("expected 1 table, got %d", len(schema.Tables))
			}
		})
	}
}

func TestParser_ParseTestData(t *testing.T) {
	// Read the test data file
	data, err := os.ReadFile("test_data.ddl")
	if err != nil {
		fmt.Printf("Error reading test_data.ddl: %v\n", err)
		return
	}

	fmt.Println("=== Parsing step2DDLparser/test_data.ddl ===")

	schema, err := ParseSchema(string(data))
	if err != nil {
		fmt.Printf("Parse error: %v\n", err)
		return
	}

	// Print schema information
	fmt.Printf("Schema: %s\n", schema.Name)
	fmt.Printf("Tables: %d\n\n", len(schema.Tables))

	// Print each table
	for i, table := range schema.Tables {
		fmt.Printf("Table #%d: %s\n", i+1, table.Name)
		fmt.Printf("  Columns: %d\n", len(table.Columns))
		for j, col := range table.Columns {
			typeStr := getTypeString(col.Type)
			if col.SizeLimit > 0 {
				if col.Type == 7 { // CHAR array
					typeStr = fmt.Sprintf("%s[%d]", typeStr, col.SizeLimit)
				} else { // STRING with size
					typeStr = fmt.Sprintf("%s(%d)", typeStr, col.SizeLimit)
				}
			}

			constraintStr := ""
			if len(col.Constraints) > 0 {
				constraint := col.Constraints[0]
				if constraint.IsPrimaryKey {
					constraintStr = " PRIMARY KEY"
				} else if constraint.IsForeignKey {
					constraintStr = fmt.Sprintf(" FOREIGN KEY %s", constraint.TableName)
				}
			}

			fmt.Printf("    %d. %-20s %s%s\n", j+1, col.Name, typeStr, constraintStr)
		}

		if len(table.Sets) > 0 {
			fmt.Printf("  Sets: %d\n", len(table.Sets))
			for _, set := range table.Sets {
				fmt.Printf("    - %s -> %s\n", set.Name, set.TableName)
			}
		}
		fmt.Println()
	}
}

func TestParser_CollegeSchema(t *testing.T) {
	data, err := os.ReadFile("../docs/testdata/College.ddl")
	if err != nil {
		t.Skipf("College.ddl not found: %v", err)
	}

	input := string(data)
	schema, err := ParseSchema(input)
	if err != nil {
		t.Fatalf("parse error: %v", err)
	}

	// Verify schema name
	if schema.Name != "College" {
		t.Errorf("expected schema name 'College', got '%s'", schema.Name)
	}

	// Verify we have 7 tables
	expectedTables := []string{"Departments", "Teachers", "Students", "Courses", "Classes", "Grades", "Majors"}
	if len(schema.Tables) != len(expectedTables) {
		t.Fatalf("expected %d tables, got %d", len(expectedTables), len(schema.Tables))
	}

	for i, expectedName := range expectedTables {
		if schema.Tables[i].Name != expectedName {
			t.Errorf("table %d: expected name '%s', got '%s'", i, expectedName, schema.Tables[i].Name)
		}
	}

	// Check Departments table has SETS
	departments := schema.Tables[0]
	if len(departments.Sets) != 1 {
		t.Errorf("expected 1 set in Departments, got %d", len(departments.Sets))
	} else if departments.Sets[0].Name != "Faculty" {
		t.Errorf("expected set name 'Faculty', got '%s'", departments.Sets[0].Name)
	}

	// Check Teachers table has multiple SETS
	teachers := schema.Tables[1]
	if len(teachers.Sets) != 2 {
		t.Errorf("expected 2 sets in Teachers, got %d", len(teachers.Sets))
	}

	// Check Students table has FOREIGN KEY to Teachers
	students := schema.Tables[2]
	var advisorCol *Column
	for i := range students.Columns {
		if students.Columns[i].Name == "Advisor" {
			advisorCol = &students.Columns[i]
			break
		}
	}
	if advisorCol == nil {
		t.Error("expected Advisor column in Students table")
	} else if len(advisorCol.Constraints) != 1 || !advisorCol.Constraints[0].IsForeignKey {
		t.Error("expected FOREIGN KEY constraint on Advisor")
	} else if advisorCol.Constraints[0].TableName != "Teachers" {
		t.Errorf("expected foreign key to 'Teachers', got '%s'", advisorCol.Constraints[0].TableName)
	}
}

func getTypeString(typeCode uint8) string {
	switch typeCode {
	case 1:
		return "SMALLINT"
	case 2:
		return "INT"
	case 3:
		return "BIGINT"
	case 4:
		return "DECIMAL"
	case 5:
		return "FLOAT"
	case 6:
		return "STRING"
	case 7:
		return "CHAR"
	case 8:
		return "BOOLEAN"
	case 9:
		return "DATE"
	case 10:
		return "TIME"
	default:
		return "UNKNOWN"
	}
}
