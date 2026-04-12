package cli

import (
	"fmt"
)

// AnalyzeSchema is the main entry point for the schema command
// It validates a schema file and displays the results
func AnalyzeSchema(schemaPath string, showStorage bool) error {
	// Validate the schema
	result, err := ValidateSchemaForDisplay(schemaPath)

	if err != nil {
		// File read error or similar
		fmt.Println(Red("ERROR: " + err.Error()))
		return err
	}

	// Check for parse errors
	if result.ParseError != nil {
		fmt.Println(FormatParseErrorWithContext(result.ParseError, schemaPath))
		return result.ParseError
	}

	// Check for validation errors
	if len(result.ValidationErrs) > 0 {
		fmt.Println(FormatValidationErrors(result))
		return fmt.Errorf("validation failed with %d error(s)", len(result.ValidationErrs))
	}

	// Success - display schema information
	DisplaySchemaInfo(result, showStorage)
	return nil
}
