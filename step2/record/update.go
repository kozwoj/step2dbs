package record

import (
	"fmt"
	"github.com/kozwoj/step2/db"
	"time"
)

/* gofmt:off

UpdateRecord updates an existing record in a table identified by its recordID.
Parameters:
- tableName: name of the table
- recordID: the record identifier (uint32)
- recordFields: map containing fields to update with their new values
- dbDef: DBDefinition struct with the database definition
Returns:
- error: if there is an issue updating the record

The function performs the following steps:
- Finds table definition in DBDefinition
- Reads existing record data
- Validates record exists and is not deleted
- Deserializes existing record to get current values
- Validates primary key is not being changed (if provided in update)
- For each field in the update:
  - Validate the field value
  - If STRING field: Remove recordID from old string's postings, add to new string's postings
  - Update the field value in the record body
- Validates foreign key constraints still hold after update
- Writes updated record back to file
- Returns success

gofmt:on */

func UpdateRecord(tableName string, recordID uint32, recordFields map[string]interface{}, dbDef *db.DBDefinition) error {
	// Find table definition
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return fmt.Errorf("table '%s' not found", tableName)
	}
	tableDescription := dbDef.Tables[tableIndex]

	// Read existing record data
	recordData, err := GetRecordData(tableDescription.RecordFile, recordID)
	if err != nil {
		return fmt.Errorf("failed to read record: %w", err)
	}

	// Validate record exists and is not deleted
	if len(recordData) < 1 {
		return fmt.Errorf("record data too short")
	}
	if recordData[0] != 0x00 {
		return fmt.Errorf("record is deleted")
	}

	// Extract record body (skip header)
	recordBody := recordData[tableDescription.RecordLayout.HeaderSize:]

	// Deserialize existing record to get current values
	currentValues, err := DeserializeRecord(recordBody, tableDescription)
	if err != nil {
		return fmt.Errorf("failed to deserialize current record: %w", err)
	}

	// Build map of field name to field description for quick lookup
	fieldMap := make(map[string]int)
	for i, fieldDesc := range tableDescription.RecordLayout.Fields {
		fieldMap[fieldDesc.Name] = i
	}

	// Validate primary key is not being changed
	if tableDescription.Key != -1 {
		pkFieldDesc := tableDescription.RecordLayout.Fields[tableDescription.Key]
		if newPKValue, exists := recordFields[pkFieldDesc.Name]; exists {
			currentPKValue := currentValues[pkFieldDesc.Name]
			// Compare values - they must match
			if !valuesEqual(newPKValue, currentPKValue, pkFieldDesc.Type) {
				return fmt.Errorf("primary key field '%s' cannot be changed", pkFieldDesc.Name)
			}
		}
	}

	// Process each field in the update
	for fieldName, newValue := range recordFields {
		fieldIdx, exists := fieldMap[fieldName]
		if !exists {
			return fmt.Errorf("field '%s' does not exist in table '%s'", fieldName, tableName)
		}
		fieldDesc := tableDescription.RecordLayout.Fields[fieldIdx]

		// Calculate offset within the record body
		bodyOffset := fieldDesc.Offset - tableDescription.RecordLayout.HeaderSize

		// Get current value
		currentValue, hasCurrentValue := currentValues[fieldName]

		// Handle STRING field updates specially
		if fieldDesc.Type == db.STRING {
			// STRING fields require dictionary and postings list updates
			if newValue == nil {
				// Setting STRING field to NULL
				if !fieldDesc.IsOptional {
					return fmt.Errorf("field '%s' is required and cannot be set to NULL", fieldName)
				}
				// Remove from old string's postings list if it had a value
				if hasCurrentValue {
					err = removeRecordFromStringPostings(currentValue.(string), recordID, fieldDesc)
					if err != nil {
						return fmt.Errorf("failed to remove record from old string postings: %w", err)
					}
				}
				// Set HasValue flag to 0x00
				recordBody[bodyOffset-1] = 0x00
				// Clear the field data
				for i := 0; i < fieldDesc.Size; i++ {
					recordBody[bodyOffset+i] = 0x00
				}
			} else {
				// Updating to new string value
				newStringValue, ok := newValue.(string)
				if !ok {
					return fmt.Errorf("field '%s' must be a string", fieldName)
				}

				// Validate string length
				if fieldDesc.StringSizeLimit > 0 && len(newStringValue) > fieldDesc.StringSizeLimit {
					return fmt.Errorf("field '%s' exceeds maximum length %d", fieldName, fieldDesc.StringSizeLimit)
				}

				// Remove from old string's postings list if it had a value
				if hasCurrentValue && currentValue.(string) != newStringValue {
					err = removeRecordFromStringPostings(currentValue.(string), recordID, tableDescription.RecordLayout.Fields[fieldIdx])
					if err != nil {
						return fmt.Errorf("failed to remove record from old string postings: %w", err)
					}
				}

				// Add to new string's postings list and update record body
				if !hasCurrentValue || currentValue.(string) != newStringValue {
					_, _, err = serializeString(newStringValue, fieldDesc.Dictionary, recordID, bodyOffset, recordBody)
					if err != nil {
						return fmt.Errorf("failed to serialize string field: %w", err)
					}
				}
			}
		} else {
			// Non-STRING field update
			// Convert JSON value to proper Go type based on field type
			validatedValue, err := convertJSONValueToFieldType(newValue, tableDescription.RecordLayout.Fields[fieldIdx])
			if err != nil {
				return fmt.Errorf("validation failed for field '%s': %w", fieldName, err)
			}

			// Update the field in record body
			err = updateFieldInBody(validatedValue, tableDescription.RecordLayout.Fields[fieldIdx], bodyOffset, recordBody)
			if err != nil {
				return fmt.Errorf("failed to update field '%s': %w", fieldName, err)
			}
		}
	}

	// Validate foreign key constraints after update
	// Build complete field values map for validation
	updatedValues, err := DeserializeRecord(recordBody, tableDescription)
	if err != nil {
		return fmt.Errorf("failed to deserialize updated record: %w", err)
	}

	// Convert map to slice in field order
	fieldValues := make([]interface{}, len(tableDescription.RecordLayout.Fields))
	for i, fieldDesc := range tableDescription.RecordLayout.Fields {
		if value, exists := updatedValues[fieldDesc.Name]; exists {
			fieldValues[i] = value
		} else {
			fieldValues[i] = nil
		}
	}

	// Validate foreign keys
	err = validateForeignKeyConstraints(tableDescription, fieldValues, dbDef)
	if err != nil {
		return fmt.Errorf("foreign key validation failed: %w", err)
	}

	// Write updated record back to file
	// Reconstruct full record data (header + body)
	copy(recordData[tableDescription.RecordLayout.HeaderSize:], recordBody)
	err = OverrideRecordData(tableDescription.RecordFile, recordID, recordData)
	if err != nil {
		return fmt.Errorf("failed to write updated record: %w", err)
	}

	return nil
}

// Helper function to compare values for equality based on field type
func valuesEqual(v1, v2 interface{}, fieldType db.FieldType) bool {
	switch fieldType {
	case db.SMALLINT:
		// v1 might be float64 from JSON, v2 is int from deserialization
		val1, ok1 := v1.(float64)
		val2, ok2 := v2.(int)
		if ok1 && ok2 {
			return int(val1) == val2
		}
	case db.INT:
		val1, ok1 := v1.(float64)
		val2, ok2 := v2.(int)
		if ok1 && ok2 {
			return int(val1) == val2
		}
	case db.BIGINT:
		val1, ok1 := v1.(float64)
		val2, ok2 := v2.(int64)
		if ok1 && ok2 {
			return int64(val1) == val2
		}
	case db.CHAR, db.STRING, db.DECIMAL:
		val1, ok1 := v1.(string)
		val2, ok2 := v2.(string)
		if ok1 && ok2 {
			return val1 == val2
		}
	case db.FLOAT:
		val1, ok1 := v1.(float64)
		val2, ok2 := v2.(float64)
		if ok1 && ok2 {
			return val1 == val2
		}
	case db.BOOLEAN:
		val1, ok1 := v1.(bool)
		val2, ok2 := v2.(bool)
		if ok1 && ok2 {
			return val1 == val2
		}
	}
	return false
}

// Helper function to remove a recordID from a string's postings list
func removeRecordFromStringPostings(stringValue string, recordID uint32, fieldDesc *db.FieldDescription) error {
	if fieldDesc.Dictionary == nil {
		return fmt.Errorf("dictionary not initialized for field %s", fieldDesc.Name)
	}

	// Find the string in dictionary
	dictID, postingsRef, err := fieldDesc.Dictionary.FindString(stringValue)
	if err != nil {
		// String not found - nothing to remove
		return nil
	}

	// Remove recordID from postings list
	err = fieldDesc.Dictionary.RemoveRecordID(postingsRef, recordID, dictID)
	if err != nil {
		return err
	}

	return nil
}

// Helper function to update a non-STRING field in the record body
func updateFieldInBody(value interface{}, fieldDesc *db.FieldDescription, bodyOffset int, recordBody []byte) error {
	// Handle NULL values
	if value == nil {
		if !fieldDesc.IsOptional {
			return fmt.Errorf("field '%s' is required and cannot be set to NULL", fieldDesc.Name)
		}
		// Set HasValue flag to 0x00
		recordBody[bodyOffset-1] = 0x00
		// Clear the field data
		for i := 0; i < fieldDesc.Size; i++ {
			recordBody[bodyOffset+i] = 0x00
		}
		return nil
	}

	// Set HasValue flag to 0x01
	recordBody[bodyOffset-1] = 0x01

	// Serialize based on field type
	switch fieldDesc.Type {
	case db.SMALLINT:
		serializeSmallInt(value.(int16), bodyOffset, recordBody)
	case db.INT:
		serializeInt(value.(int32), bodyOffset, recordBody)
	case db.BIGINT:
		serializeBigInt(value.(int64), bodyOffset, recordBody)
	case db.DECIMAL:
		serializeDecimal(value.(string), bodyOffset, recordBody)
	case db.FLOAT:
		serializeFloat(value.(float64), bodyOffset, recordBody)
	case db.CHAR:
		serializeChar(value.(string), fieldDesc.Size, bodyOffset, recordBody)
	case db.BOOLEAN:
		serializeBoolean(value.(bool), bodyOffset, recordBody)
	case db.DATE:
		serializeDate(value.(string), bodyOffset, recordBody)
	case db.TIME:
		serializeTime(value.(string), bodyOffset, recordBody)
	default:
		return fmt.Errorf("unsupported field type: %d", fieldDesc.Type)
	}

	return nil
}

// Helper function to convert JSON value to proper Go type based on field type
func convertJSONValueToFieldType(value interface{}, fieldDesc *db.FieldDescription) (interface{}, error) {
	// Handle NULL values
	if value == nil {
		if !fieldDesc.IsOptional {
			return nil, fmt.Errorf("field '%s' is required and cannot be NULL", fieldDesc.Name)
		}
		return nil, nil
	}

	switch fieldDesc.Type {
	case db.SMALLINT:
		// JSON numbers come as float64
		floatVal, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a number", fieldDesc.Name)
		}
		// Check if floatValue is a valid int16 without fractional part
		if floatVal != float64(int16(floatVal)) {
			return nil, fmt.Errorf("field '%s' must be a valid SMALLINT (int16)", fieldDesc.Name)
		}
		return int16(floatVal), nil

	case db.INT:
		floatVal, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a number", fieldDesc.Name)
		}
		// Check if floatValue is a valid int32 without fractional part
		if floatVal != float64(int32(floatVal)) {
			return nil, fmt.Errorf("field '%s' must be a valid INT (int32)", fieldDesc.Name)
		}
		return int32(floatVal), nil

	case db.BIGINT:
		floatVal, ok := value.(float64)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a number", fieldDesc.Name)
		}
		// Check if floatValue is a valid int64 without fractional part
		if floatVal != float64(int64(floatVal)) {
			return nil, fmt.Errorf("field '%s' must be a valid BIGINT (int64)", fieldDesc.Name)
		}
		return int64(floatVal), nil

	case db.DECIMAL:
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a string (decimal)", fieldDesc.Name)
		}
		// Validate decimal format using IsDecimalString
		if !IsDecimalString(strVal) {
			return nil, fmt.Errorf("field '%s' has invalid decimal format", fieldDesc.Name)
		}
		return strVal, nil

	case db.FLOAT:
		if floatVal, ok := value.(float64); ok {
			return floatVal, nil
		}
		return nil, fmt.Errorf("field '%s' must be a number", fieldDesc.Name)

	case db.CHAR:
		if strVal, ok := value.(string); ok {
			// Validate length
			if len(strVal) != fieldDesc.Size {
				return nil, fmt.Errorf("field '%s' must be exactly %d characters", fieldDesc.Name, fieldDesc.Size)
			}
			return strVal, nil
		}
		return nil, fmt.Errorf("field '%s' must be a string", fieldDesc.Name)

	case db.BOOLEAN:
		if boolVal, ok := value.(bool); ok {
			return boolVal, nil
		}
		return nil, fmt.Errorf("field '%s' must be a boolean", fieldDesc.Name)

	case db.DATE:
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a string (date)", fieldDesc.Name)
		}
		// Validate date format "YYYY-MM-DD"
		if _, err := time.Parse("2006-01-02", strVal); err != nil {
			return nil, fmt.Errorf("field '%s' has invalid date format (expected YYYY-MM-DD)", fieldDesc.Name)
		}
		return strVal, nil

	case db.TIME:
		strVal, ok := value.(string)
		if !ok {
			return nil, fmt.Errorf("field '%s' must be a string (time)", fieldDesc.Name)
		}
		// Validate time format (H:MM, H:MM:SS, or H:MM:SS.mmm)
		if err := ParseCompactTime(strVal); err != nil {
			return nil, fmt.Errorf("field '%s' has invalid time format: %v", fieldDesc.Name, err)
		}
		return strVal, nil

	default:
		return nil, fmt.Errorf("unsupported field type: %d", fieldDesc.Type)
	}
}
