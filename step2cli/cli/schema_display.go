package cli

import (
	"bufio"
	"fmt"
	"os"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/step2DDLparser"
	"strings"
)

// FormatParseErrorWithContext formats a parse error with surrounding file context
func FormatParseErrorWithContext(err error, filePath string) string {
	var sb strings.Builder

	sb.WriteString(Red("ERROR: Schema parsing failed") + "\n\n")

	// Check if it's a ParseError with location info
	if parseErr, ok := err.(*step2DDLparser.ParseError); ok {
		sb.WriteString(Red("Parse Error:") + "\n")
		sb.WriteString(fmt.Sprintf("  File: %s\n", filePath))
		sb.WriteString(fmt.Sprintf("  %s\n\n", parseErr.Error()))

		// Show context from file
		if parseErr.Line > 0 {
			context := getFileContext(filePath, parseErr.Line, parseErr.Col)
			if context != "" {
				sb.WriteString(Yellow("Context:") + "\n")
				sb.WriteString(context)
			}
		}
	} else {
		// Generic error without location
		sb.WriteString(Red("Parse Error:") + "\n")
		sb.WriteString(fmt.Sprintf("  %s\n", err.Error()))
	}

	return sb.String()
}

// getFileContext reads the file and returns 2-3 lines of context around the error line
func getFileContext(filePath string, errorLine, errorCol int) string {
	file, err := os.Open(filePath)
	if err != nil {
		return ""
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 1
	var lines []string

	// Read lines around the error
	for scanner.Scan() {
		if lineNum >= errorLine-1 && lineNum <= errorLine+1 {
			lines = append(lines, scanner.Text())
		}
		if lineNum > errorLine+1 {
			break
		}
		lineNum++
	}

	if len(lines) == 0 {
		return ""
	}

	var sb strings.Builder
	startLine := errorLine - 1
	if startLine < 1 {
		startLine = 1
	}

	for i, line := range lines {
		currentLine := startLine + i
		if currentLine == errorLine {
			// Error line - highlight it
			sb.WriteString(fmt.Sprintf("  %3d | %s\n", currentLine, line))

			// Add error indicator
			if errorCol > 0 && errorCol <= len(line) {
				indicator := strings.Repeat(" ", errorCol+6) + Red("^^^^^^")
				sb.WriteString(indicator + "\n")
			}
		} else {
			// Context line
			sb.WriteString(fmt.Sprintf("  %3d | %s\n", currentLine, line))
		}
	}

	return sb.String()
}

// FormatValidationErrors formats all validation errors grouped by category
func FormatValidationErrors(result *ValidationResult) string {
	var sb strings.Builder

	sb.WriteString(Red("ERROR: Schema validation failed") + "\n\n")
	sb.WriteString(Red(fmt.Sprintf("Found %d validation error(s):", len(result.ValidationErrs))) + "\n\n")

	// Group errors by category
	errorsByCategory := make(map[string][]ValidationError)
	for _, err := range result.ValidationErrs {
		errorsByCategory[err.Category] = append(errorsByCategory[err.Category], err)
	}

	// Display errors by category
	categories := []string{"schema", "table", "field", "foreign_key", "set", "duplicate"}
	categoryTitles := map[string]string{
		"schema":      "Schema-Level Errors:",
		"table":       "Table-Level Errors:",
		"field":       "Field-Level Errors:",
		"foreign_key": "Foreign Key Errors:",
		"set":         "Set-Level Errors:",
		"duplicate":   "Duplicate Name Errors:",
	}

	for _, category := range categories {
		if errs, exists := errorsByCategory[category]; exists && len(errs) > 0 {
			sb.WriteString(Yellow(categoryTitles[category]) + "\n")
			for _, err := range errs {
				sb.WriteString(fmt.Sprintf("  • %s\n", err.Message))
			}
			sb.WriteString("\n")
		}
	}

	// Show what was successfully parsed
	if result.Schema != nil {
		fieldCount := 0
		for _, table := range result.Schema.Tables {
			fieldCount += len(table.Columns)
		}
		sb.WriteString(Green(fmt.Sprintf("Successfully parsed: %d table(s), %d field(s)", len(result.Schema.Tables), fieldCount)) + "\n")
	}

	return sb.String()
}

// DisplaySchemaInfo displays schema information for a valid schema
func DisplaySchemaInfo(result *ValidationResult, showStorage bool) {
	schema := result.Schema

	fmt.Println(Green("✓ Schema validated successfully") + "\n")
	fmt.Printf("Schema: %s\n\n", Cyan(schema.Name))

	if showStorage {
		// Show directory tree structure
		fmt.Println(Yellow("Database Directory Structure:") + "\n")
		tree := BuildStorageTree(schema)
		fmt.Println(DrawTree(tree))
		fmt.Println()

		// Show summary
		dictCount := countDictionaries(schema)
		fileCount := countFiles(schema)
		fmt.Println(Green(fmt.Sprintf("Summary: %d table(s), %d dictionar%s, %d file(s)",
			len(schema.Tables),
			dictCount,
			pluralSuffix(dictCount, "y", "ies"),
			fileCount)) + "\n")
	} else {
		// Show basic schema info
		for _, table := range schema.Tables {
			displayTableInfo(table)
		}

		// Show summary
		fieldCount := 0
		dictCount := 0
		for _, table := range schema.Tables {
			fieldCount += len(table.Columns)
			for _, col := range table.Columns {
				if db.FieldType(col.Type) == db.STRING {
					dictCount++
				}
			}
		}
		fmt.Println(Green(fmt.Sprintf("Summary: %d table(s), %d field(s), %d dictionar%s",
			len(schema.Tables),
			fieldCount,
			dictCount,
			pluralSuffix(dictCount, "y", "ies"))) + "\n")
	}
}

// displayTableInfo displays information about a single table
func displayTableInfo(table step2DDLparser.Table) {
	// Table header
	setCount := len(table.Sets)
	if setCount > 0 {
		fmt.Printf("%s (%d field(s), %d set(s))\n", Yellow("Table: "+table.Name), len(table.Columns), setCount)
	} else {
		fmt.Printf("%s (%d field(s))\n", Yellow("Table: "+table.Name), len(table.Columns))
	}

	// Find primary key
	var primaryKeyName string
	var primaryKeyType string
	for _, col := range table.Columns {
		for _, constraint := range col.Constraints {
			if constraint.IsPrimaryKey {
				primaryKeyName = col.Name
				primaryKeyType = formatFieldType(col)
				break
			}
		}
		if primaryKeyName != "" {
			break
		}
	}

	if primaryKeyName != "" {
		fmt.Printf("  Primary Key: %s (%s)\n", primaryKeyName, primaryKeyType)
	}

	// Fields
	fmt.Println("  Fields:")
	for _, col := range table.Columns {
		displayFieldInfo(col)
	}

	// Sets
	if len(table.Sets) > 0 {
		fmt.Println("  Sets:")
		for _, set := range table.Sets {
			fmt.Printf("    • %s → %s\n", set.Name, set.TableName)
		}
	}

	fmt.Println()
}

// displayFieldInfo displays information about a single field
func displayFieldInfo(col step2DDLparser.Column) {
	typeStr := formatFieldType(col)

	var constraints []string
	for _, constraint := range col.Constraints {
		if constraint.IsPrimaryKey {
			constraints = append(constraints, "PRIMARY KEY")
		}
		if constraint.IsForeignKey {
			constraints = append(constraints, fmt.Sprintf("FK→%s", constraint.TableName))
		}
		if constraint.IsOptional {
			constraints = append(constraints, "OPTIONAL")
		}
	}

	if len(constraints) > 0 {
		fmt.Printf("    • %-20s %-15s %s\n", col.Name, typeStr, strings.Join(constraints, ", "))
	} else {
		fmt.Printf("    • %-20s %s\n", col.Name, typeStr)
	}
}

// formatFieldType formats a field type for display
func formatFieldType(col step2DDLparser.Column) string {
	fieldType := db.FieldType(col.Type)
	typeName := getFieldTypeName(fieldType)

	switch fieldType {
	case db.STRING:
		if col.SizeLimit > 0 {
			return fmt.Sprintf("STRING[%d]", col.SizeLimit)
		}
		return "STRING"
	case db.CHAR:
		return fmt.Sprintf("CHAR[%d]", col.SizeLimit)
	default:
		return typeName
	}
}

// BuildStorageTree builds a directory tree structure from a parsed schema
func BuildStorageTree(schema *step2DDLparser.Schema) *Node {
	root := &Node{Name: schema.Name + "/"}

	// schema.json
	root.Children = append(root.Children, &Node{Name: "schema.json"})

	// For each table
	for _, table := range schema.Tables {
		tableNode := &Node{Name: table.Name + "/"}

		// records.dat
		tableNode.Children = append(tableNode.Children, &Node{Name: "records.dat"})

		// primindex.dat (if has primary key)
		hasPrimaryKey := false
		for _, col := range table.Columns {
			for _, constraint := range col.Constraints {
				if constraint.IsPrimaryKey {
					hasPrimaryKey = true
					break
				}
			}
			if hasPrimaryKey {
				break
			}
		}
		if hasPrimaryKey {
			tableNode.Children = append(tableNode.Children, &Node{Name: "primindex.dat"})
		}

		// Dictionary subdirectories (for STRING fields)
		for _, col := range table.Columns {
			if db.FieldType(col.Type) == db.STRING {
				dictNode := &Node{Name: col.Name + "/"}
				dictNode.Children = []*Node{
					{Name: "strings.dat"},
					{Name: "offsets.dat"},
					{Name: "postings.dat"},
					{Name: "index.dat"},
					{Name: "prefix.dat"},
				}
				tableNode.Children = append(tableNode.Children, dictNode)
			}
		}

		// Set files
		for _, set := range table.Sets {
			tableNode.Children = append(tableNode.Children, &Node{Name: set.Name + ".dat"})
		}

		root.Children = append(root.Children, tableNode)
	}

	return root
}

// Helper: Count dictionaries in schema
func countDictionaries(schema *step2DDLparser.Schema) int {
	count := 0
	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			if db.FieldType(col.Type) == db.STRING {
				count++
			}
		}
	}
	return count
}

// Helper: Count total files in schema
func countFiles(schema *step2DDLparser.Schema) int {
	count := 1 // schema.json

	for _, table := range schema.Tables {
		count++ // records.dat

		// primindex.dat
		for _, col := range table.Columns {
			for _, constraint := range col.Constraints {
				if constraint.IsPrimaryKey {
					count++
					break
				}
			}
		}

		// Dictionary files (5 per STRING field)
		for _, col := range table.Columns {
			if db.FieldType(col.Type) == db.STRING {
				count += 5
			}
		}

		// Set files
		count += len(table.Sets)
	}

	return count
}

// Helper: Pluralize words
func pluralSuffix(count int, singular, plural string) string {
	if count == 1 {
		return singular
	}
	return plural
}
