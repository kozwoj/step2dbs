package cli

import (
	"fmt"
	"os"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/step2DDLparser"
)

// ValidationResult holds the result of schema validation
type ValidationResult struct {
	Schema         *step2DDLparser.Schema // Parsed schema (may be partial if errors)
	ParseError     error                  // nil if parse succeeded
	ValidationErrs []ValidationError      // All validation errors found
}

// ValidationError represents a single validation error
type ValidationError struct {
	Category string // "table", "field", "set", "foreign_key", "duplicate", etc.
	Table    string // Table name where error occurred
	Field    string // Field name (if field-level error)
	Set      string // Set name (if set-level error)
	Message  string // Human-readable error message
}

// ValidateSchemaForDisplay parses and validates a schema, accumulating all errors
// Unlike CreateDBDefinition (which stops on first error), this continues to find
// all validation issues for better user feedback
func ValidateSchemaForDisplay(schemaPath string) (*ValidationResult, error) {
	// Read schema file
	data, err := os.ReadFile(schemaPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read schema file: %w", err)
	}

	// Parse schema
	input := string(data)
	schema, err := step2DDLparser.ParseSchema(input)
	if err != nil {
		// Parse error - return it in the result
		return &ValidationResult{
			Schema:         nil,
			ParseError:     err,
			ValidationErrs: []ValidationError{},
		}, nil
	}

	// Schema parsed successfully - now validate it
	result := &ValidationResult{
		Schema:         &schema,
		ParseError:     nil,
		ValidationErrs: []ValidationError{},
	}

	// Run all validation checks and accumulate errors
	result.ValidationErrs = append(result.ValidationErrs, validateSchemaLevel(&schema)...)
	result.ValidationErrs = append(result.ValidationErrs, validateTableLevel(&schema)...)
	result.ValidationErrs = append(result.ValidationErrs, validateFieldLevel(&schema)...)
	result.ValidationErrs = append(result.ValidationErrs, validateSetLevel(&schema)...)

	return result, nil
}

// validateSchemaLevel performs schema-level validation checks
func validateSchemaLevel(schema *step2DDLparser.Schema) []ValidationError {
	var errors []ValidationError

	// Check 1: Schema name must not be empty
	if schema.Name == "" {
		errors = append(errors, ValidationError{
			Category: "schema",
			Message:  "Schema name is empty",
		})
	}

	// Check 2: Schema must contain at least one table
	if len(schema.Tables) == 0 {
		errors = append(errors, ValidationError{
			Category: "schema",
			Message:  "Schema must contain at least one table",
		})
	}

	// Check 3: All table names must be unique
	tableNames := make(map[string]bool)
	for _, table := range schema.Tables {
		if tableNames[table.Name] {
			errors = append(errors, ValidationError{
				Category: "duplicate",
				Table:    table.Name,
				Message:  fmt.Sprintf("Duplicate table name '%s'", table.Name),
			})
		}
		tableNames[table.Name] = true
	}

	return errors
}

// validateTableLevel performs table-level validation checks
func validateTableLevel(schema *step2DDLparser.Schema) []ValidationError {
	var errors []ValidationError

	for _, table := range schema.Tables {
		// Check 4: Each table must contain at least one field
		if len(table.Columns) == 0 {
			errors = append(errors, ValidationError{
				Category: "table",
				Table:    table.Name,
				Message:  fmt.Sprintf("Table '%s' has no fields defined", table.Name),
			})
			continue // Skip further checks for this table
		}

		// Check 5: All field names within a table must be unique
		fieldNames := make(map[string]bool)
		for _, col := range table.Columns {
			if fieldNames[col.Name] {
				errors = append(errors, ValidationError{
					Category: "duplicate",
					Table:    table.Name,
					Field:    col.Name,
					Message:  fmt.Sprintf("Table '%s': Duplicate field name '%s'", table.Name, col.Name),
				})
			}
			fieldNames[col.Name] = true
		}

		// Check 6: All set names within a table must be unique
		setNames := make(map[string]bool)
		for _, set := range table.Sets {
			if setNames[set.Name] {
				errors = append(errors, ValidationError{
					Category: "duplicate",
					Table:    table.Name,
					Set:      set.Name,
					Message:  fmt.Sprintf("Table '%s': Duplicate set name '%s'", table.Name, set.Name),
				})
			}
			setNames[set.Name] = true
		}

		// Check 7: Each table must have at most one field marked as primary key
		primaryKeyCount := 0
		var primaryKeyFields []string
		for _, col := range table.Columns {
			for _, constraint := range col.Constraints {
				if constraint.IsPrimaryKey {
					primaryKeyCount++
					primaryKeyFields = append(primaryKeyFields, col.Name)
				}
			}
		}
		if primaryKeyCount > 1 {
			errors = append(errors, ValidationError{
				Category: "table",
				Table:    table.Name,
				Message:  fmt.Sprintf("Table '%s': Multiple primary key fields found (%v)", table.Name, primaryKeyFields),
			})
		}
	}

	return errors
}

// validateFieldLevel performs field-level validation checks
func validateFieldLevel(schema *step2DDLparser.Schema) []ValidationError {
	var errors []ValidationError

	for _, table := range schema.Tables {
		for _, col := range table.Columns {
			// Check 8: If field has primary key constraint, type must be allowed
			for _, constraint := range col.Constraints {
				if constraint.IsPrimaryKey {
					fieldType := db.FieldType(col.Type)
					validType := (fieldType == db.SMALLINT || fieldType == db.INT || fieldType == db.BIGINT ||
						(fieldType == db.CHAR && col.SizeLimit >= 4 && col.SizeLimit <= 32))
					if !validType {
						typeName := getFieldTypeName(fieldType)
						if fieldType == db.CHAR {
							typeName = fmt.Sprintf("CHAR[%d]", col.SizeLimit)
						}
						errors = append(errors, ValidationError{
							Category: "field",
							Table:    table.Name,
							Field:    col.Name,
							Message:  fmt.Sprintf("Table '%s', field '%s': Primary key must be SMALLINT, INT, BIGINT, or CHAR[4-32], found %s", table.Name, col.Name, typeName),
						})
					}
				}
			}

			// Check 9: If field is CHAR type, size must be specified and > 0
			if db.FieldType(col.Type) == db.CHAR && col.SizeLimit <= 0 {
				errors = append(errors, ValidationError{
					Category: "field",
					Table:    table.Name,
					Field:    col.Name,
					Message:  fmt.Sprintf("Table '%s', field '%s': CHAR type must have size specified", table.Name, col.Name),
				})
			}

			// Check 10, 11, 12: Foreign key constraints
			for _, constraint := range col.Constraints {
				if constraint.IsForeignKey {
					// Check 10: Referenced table must exist
					refTable := findTable(schema.Tables, constraint.TableName)
					if refTable == nil {
						errors = append(errors, ValidationError{
							Category: "foreign_key",
							Table:    table.Name,
							Field:    col.Name,
							Message:  fmt.Sprintf("Table '%s', field '%s': Foreign key references non-existent table '%s'", table.Name, col.Name, constraint.TableName),
						})
						continue // Skip further checks for this foreign key
					}

					// Check 11: Referenced table must have a primary key
					var refPrimaryKeyCol *step2DDLparser.Column
					for i, refCol := range refTable.Columns {
						for _, refConstraint := range refCol.Constraints {
							if refConstraint.IsPrimaryKey {
								refPrimaryKeyCol = &refTable.Columns[i]
								break
							}
						}
						if refPrimaryKeyCol != nil {
							break
						}
					}
					if refPrimaryKeyCol == nil {
						errors = append(errors, ValidationError{
							Category: "foreign_key",
							Table:    table.Name,
							Field:    col.Name,
							Message:  fmt.Sprintf("Table '%s', field '%s': References table '%s' which has no primary key", table.Name, col.Name, constraint.TableName),
						})
						continue
					}

					// Check 12: Foreign key type must match referenced primary key type
					if col.Type != refPrimaryKeyCol.Type {
						errors = append(errors, ValidationError{
							Category: "foreign_key",
							Table:    table.Name,
							Field:    col.Name,
							Message:  fmt.Sprintf("Table '%s', field '%s': Type mismatch with referenced field '%s' in table '%s'", table.Name, col.Name, refPrimaryKeyCol.Name, constraint.TableName),
						})
						continue
					}

					// For CHAR types, sizes must also match
					if db.FieldType(col.Type) == db.CHAR && col.SizeLimit != refPrimaryKeyCol.SizeLimit {
						errors = append(errors, ValidationError{
							Category: "foreign_key",
							Table:    table.Name,
							Field:    col.Name,
							Message:  fmt.Sprintf("Table '%s', field '%s': CHAR[%d] size mismatch with referenced field '%s' CHAR[%d] in table '%s'", table.Name, col.Name, col.SizeLimit, refPrimaryKeyCol.Name, refPrimaryKeyCol.SizeLimit, constraint.TableName),
						})
					}
				}
			}
		}
	}

	return errors
}

// validateSetLevel performs set-level validation checks
func validateSetLevel(schema *step2DDLparser.Schema) []ValidationError {
	var errors []ValidationError

	for _, table := range schema.Tables {
		for _, set := range table.Sets {
			// Check 13: Member table must exist in schema
			if findTable(schema.Tables, set.TableName) == nil {
				errors = append(errors, ValidationError{
					Category: "set",
					Table:    table.Name,
					Set:      set.Name,
					Message:  fmt.Sprintf("Table '%s', set '%s': Member table '%s' not found in schema", table.Name, set.Name, set.TableName),
				})
			}
		}
	}

	return errors
}

// Helper: Find table by name
func findTable(tables []step2DDLparser.Table, name string) *step2DDLparser.Table {
	for i := range tables {
		if tables[i].Name == name {
			return &tables[i]
		}
	}
	return nil
}

// Helper: Get field type name for display
func getFieldTypeName(ft db.FieldType) string {
	switch ft {
	case db.SMALLINT:
		return "SMALLINT"
	case db.INT:
		return "INT"
	case db.BIGINT:
		return "BIGINT"
	case db.DECIMAL:
		return "DECIMAL"
	case db.FLOAT:
		return "FLOAT"
	case db.STRING:
		return "STRING"
	case db.CHAR:
		return "CHAR"
	case db.BOOLEAN:
		return "BOOLEAN"
	case db.DATE:
		return "DATE"
	case db.TIME:
		return "TIME"
	default:
		return fmt.Sprintf("UNKNOWN(%d)", ft)
	}
}
