package record

import (
	"errors"
	"math"
	"strings"
	"time"

	"github.com/kozwoj/step2/db"
)

/*
UndoOperation represents a single STRING field serialization operation that needs to be undone
if subsequent operations fail. It stores the information needed to remove a recordID from a postings list.
*/
type UndoOperation struct {
	Dictionary  *db.FieldDescription // Reference to field description (contains dictionary)
	DictID      uint32               // Dictionary ID of the string
	PostingsRef uint32               // Reference to the postings list
	RecordID    uint32               // Record ID to remove from postings
}

/* gofmt:off

SerializeRecord serializes validated field values into a byte slice representing the record body.
Parameters:
- fieldValues: slice of validated field values in the order defined by the table schema (obtained from ValidateRecord)
- tableDescription: instance of TableDescription struct containing the schema information for the table and the record
- recordID: the record ID allocated for the record being serialized (used for updating postings lists for string fields)
Returns:
- []byte: the serialized byte slice of the record BODY (not including the record header)
- error: error if serialization fails

The serialization follows these steps:
- Create record body buffer
- Serialize fields stored in the records body first (has no impact on indices)
- Serialize string fields and update the corresponding postings lists for each string field
	- Create an in‑memory undo stack for serializing string fields.
	- For each string property / dictionary
  		- Do the normal forward ops:
    	- Add string (...)
    	- Add recordID to postings list (...)
    	- Add string prefix (...)
  		- Immediately push the corresponding inverse op (removing recordID from postings list) onto the undo stack.
	- If anything fails before all strings fields are serialized - the current string serialization fails
  		- Pop the undo stack and execute each inverse op in reverse order.
  		- Add the allocated record space to the deleted records linked-list.
	- If everything succeeds
  		- Drop the undo stack
- Return the serialized record bytes.

gofmt:on */

func SerializeRecord(fieldValues []interface{}, tableDescription *db.TableDescription, recordID uint32) ([]byte, error) {

	// Step 1: Create record body buffer
	recordBody := make([]byte, tableDescription.RecordLayout.DataSize)

	// Step 2: Initialize undo stack for STRING field operations
	undoStack := []UndoOperation{}

	// Step 3: Serialize all fields in their definition sequence
	for i, fieldDesc := range tableDescription.RecordLayout.Fields {
		fieldValue := fieldValues[i]

		// Calculate offset within the record body
		bodyOffset := fieldDesc.Offset - tableDescription.RecordLayout.HeaderSize

		// Handle nil/NULL values for optional fields
		if fieldValue == nil {
			if !fieldDesc.IsOptional {
				// This should have been caught by validation, but check anyway
				return nil, errors.New("missing required field: " + fieldDesc.Name)
			}
			// For NULL optional fields, write HasValue flag = 0x00
			// Field data is already zeros from make()
			recordBody[bodyOffset-1] = 0x00
			continue
		}

		// Serialize based on field type
		switch fieldDesc.Type {
		case db.SMALLINT:
			serializeSmallInt(fieldValue.(int16), bodyOffset, recordBody)

		case db.INT:
			serializeInt(fieldValue.(int32), bodyOffset, recordBody)

		case db.BIGINT:
			serializeBigInt(fieldValue.(int64), bodyOffset, recordBody)

		case db.DECIMAL:
			serializeDecimal(fieldValue.(string), bodyOffset, recordBody)

		case db.FLOAT:
			serializeFloat(fieldValue.(float64), bodyOffset, recordBody)

		case db.STRING:
			// STRING fields require dictionary operations with undo support
			value := fieldValue.(string)

			// Check if dictionary is initialized
			if fieldDesc.Dictionary == nil {
				// Rollback all previous STRING operations
				rollbackStringOperations(undoStack)
				return nil, errors.New("dictionary not initialized for field: " + fieldDesc.Name)
			}

			// Call serializeString which handles dictionary operations and returns dictID and postingsRef
			dictID, postingsRef, err := serializeString(value, fieldDesc.Dictionary, recordID, bodyOffset, recordBody)
			if err != nil {
				// Rollback all previous STRING operations
				rollbackStringOperations(undoStack)
				return nil, errors.New("failed to serialize STRING field " + fieldDesc.Name + ": " + err.Error())
			}

			// Push undo operation onto stack
			undoStack = append(undoStack, UndoOperation{
				Dictionary:  fieldDesc,
				DictID:      dictID,
				PostingsRef: postingsRef,
				RecordID:    recordID,
			})

		case db.CHAR:
			serializeChar(fieldValue.(string), fieldDesc.Size, bodyOffset, recordBody)

		case db.BOOLEAN:
			serializeBoolean(fieldValue.(bool), bodyOffset, recordBody)

		case db.DATE:
			serializeDate(fieldValue.(string), bodyOffset, recordBody)

		case db.TIME:
			serializeTime(fieldValue.(string), bodyOffset, recordBody)

		default:
			return nil, errors.New("unsupported field type for field: " + fieldDesc.Name)
		}
	}

	// Step 4: All fields serialized successfully - drop the undo stack
	// (Go's garbage collector will handle cleanup)

	// Step 5: Return serialized record body
	return recordBody, nil
}

/*
rollbackStringOperations executes the undo stack in reverse order.
It removes recordID from each postings list that was modified during STRING field serialization.
*/
func rollbackStringOperations(undoStack []UndoOperation) {
	// Execute undo operations in reverse order
	for i := len(undoStack) - 1; i >= 0; i-- {
		undo := undoStack[i]
		// Best effort - ignore errors during rollback
		// If RemoveRecordID fails, the recordID may remain in the postings list,
		// but this is acceptable as the record itself won't be created
		_ = undo.Dictionary.Dictionary.RemoveRecordID(undo.PostingsRef, undo.RecordID, undo.DictID)
	}
}

/*
ValidateRecord validates a record map against the table schema and collects the field values.
Parameters:
- recordMap: the record as map[string]interface{} with field names as keys
- tableDescription: instance of TableDescription struct containing the schema information for the table and the record
Returns:
- []interface{}: a slice of field values extracted from the record map
- error: error if validation fails

note:
the returned slice of field values corresponds to the field descriptions in TableDescription.RecordLayout.Fields.
so the first element in the returned slice corresponds to the first field description, and so on.
*/
func ValidateRecord(recordMap map[string]interface{}, tableDescription *db.TableDescription) ([]interface{}, error) {
	errorStrings := []string{}
	fieldValues := make([]interface{}, len(tableDescription.RecordLayout.Fields))
	// validate each field in the JSON record against the corresponding field description in the table schema
	for i, fieldDesc := range tableDescription.RecordLayout.Fields {
		value, exists := recordMap[fieldDesc.Name]
		if !exists {
			// check if the filed is optional - if not optional, add an error string
			if !fieldDesc.IsOptional {
				errorStrings = append(errorStrings, "missing field: "+fieldDesc.Name)
			} else {
				fieldValues[i] = nil
			}
		} else {
			fieldValue, err := ValidateField(value, fieldDesc)
			if err != nil {
				errorStrings = append(errorStrings, "invalid value for field: "+fieldDesc.Name)
			} else {
				fieldValues[i] = fieldValue
			}
		}
	}

	// Additional validation: if table has a primary key, verify the field type matches the primary index codec
	if tableDescription.Key != -1 {
		if tableDescription.PrimeIndex == nil {
			return nil, errors.New("table has primary key but primary index is not initialized")
		}

		pkFieldDesc := tableDescription.RecordLayout.Fields[tableDescription.Key]
		pkValue := fieldValues[tableDescription.Key]

		// Skip validation if primary key field is nil (optional primary keys shouldn't exist, but be defensive)
		if pkValue != nil {
			err := validatePrimaryKeyType(pkFieldDesc, pkValue, tableDescription.PrimeIndex.Header.KeyType)
			if err != nil {
				errorStrings = append(errorStrings, err.Error())
			}
		}
	}

	if len(errorStrings) > 0 {
		return nil, errors.New("validation errors: " + strings.Join(errorStrings, ", "))
	}
	return fieldValues, nil
}

/*
validatePrimaryKeyType validates that a primary key field value type matches the primary index codec type.
Parameters:
- fieldDesc: the field description for the primary key field
- fieldValue: the validated field value (already converted to correct Go type by ValidateField)
- indexKeyType: the KeyType from the primary index header
Returns:
- error: if the field type is inconsistent with the index codec type, nil otherwise
*/
func validatePrimaryKeyType(fieldDesc *db.FieldDescription, fieldValue interface{}, indexKeyType uint8) error {
	// Map database field types to expected primary index KeyTypes
	var expectedKeyType uint8

	switch fieldDesc.Type {
	case db.SMALLINT:
		expectedKeyType = 5 // primindex.KeyTypeSMALLINT
		// Verify the value is int16
		if _, ok := fieldValue.(int16); !ok {
			return errors.New("primary key field type mismatch: expected int16 for SMALLINT")
		}
	case db.INT:
		expectedKeyType = 6 // primindex.KeyTypeINT
		// Verify the value is int32
		if _, ok := fieldValue.(int32); !ok {
			return errors.New("primary key field type mismatch: expected int32 for INT")
		}
	case db.BIGINT:
		expectedKeyType = 7 // primindex.KeyTypeBIGINT
		// Verify the value is int64
		if _, ok := fieldValue.(int64); !ok {
			return errors.New("primary key field type mismatch: expected int64 for BIGINT")
		}
	case db.CHAR:
		// For CHAR[N], the KeyType should be KeyTypeBytesN where N is the size
		// KeyTypeBytes4 = 8, KeyTypeBytes5 = 9, ..., KeyTypeBytes32 = 36
		expectedKeyType = uint8(8 + fieldDesc.Size - 4) // KeyTypeBytesN = 8 + (N - 4)
		// Verify the value is a string of correct length
		if strValue, ok := fieldValue.(string); !ok {
			return errors.New("primary key field type mismatch: expected string for CHAR")
		} else if len(strValue) != fieldDesc.Size {
			return errors.New("primary key field size mismatch: CHAR array length does not match")
		}
	default:
		return errors.New("primary key has unsupported field type")
	}

	// Verify the index KeyType matches the expected type
	if indexKeyType != expectedKeyType {
		return errors.New("primary key field type does not match primary index codec type")
	}

	return nil
}

/*
ValidateField validates a single JSON property value against the field description.
Parameters:
- value: the JSON property value to be validated as an interface{}
- fieldDesc: instance of FieldDescription struct containing the schema information for the field
Returns:
- field value as an interface{} if validation succeeds
- error: error if validation fails
*/
func ValidateField(value interface{}, fieldDesc *db.FieldDescription) (interface{}, error) {

	switch fieldDesc.Type {
	case 1: // SMALLINT type
		floatValue, ok := value.(float64)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if floatValue is an small integer (int16) with out fractional part
		if floatValue != float64(int16(floatValue)) {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		return int16(floatValue), nil
	case 2: // INT type
		floatValue, ok := value.(float64)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if floatValue is an integer (int32) without fractional part
		if floatValue != float64(int32(floatValue)) {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		return int32(floatValue), nil
	case 3: // BIGINT type
		floatValue, ok := value.(float64)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if floatValue is an integer (int64) without fractional part
		if floatValue != math.Trunc(floatValue) {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		if floatValue < math.MinInt64 || floatValue > math.MaxInt64 {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		return int64(floatValue), nil
	case 4: // DECIMAL type
		decimal, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		if !IsDecimalString(decimal) {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		return decimal, nil
	case 5: // FLOAT type
		floatValue, ok := value.(float64)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		return floatValue, nil
	case 6: // STRING type
		strValue, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if string value exceeds size limit from DDL (StringSizeLimit)
		// StringSizeLimit == 0 means unlimited string
		if fieldDesc.StringSizeLimit > 0 && len(strValue) > fieldDesc.StringSizeLimit {
			return nil, errors.New("string value longer than max size: " + fieldDesc.Name)
		}
		return strValue, nil
	case 7: // CHAR[] type
		charArrayValue, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if string value is exactly the size specified by fieldDesc.Size and if not, return an error
		if len(charArrayValue) != fieldDesc.Size {
			return nil, errors.New("char array length does not match required size: " + fieldDesc.Name)
		}
		return charArrayValue, nil
	case 8: // BOOLEAN type
		boolValue, ok := value.(bool)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		return boolValue, nil
	case 9: // DATE type
		dateValue, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if string value is a valid date in the format "YYYY-MM-DD"
		if _, err := time.Parse("2006-01-02", dateValue); err != nil {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		return dateValue, nil
	case 10: // TIME type
		timeValue, ok := value.(string)
		if !ok {
			return nil, errors.New("invalid type for field: " + fieldDesc.Name)
		}
		// check if string value is a valid time in flexible format (H:MM, H:MM:SS, or H:MM:SS.mmm)
		if err := ParseCompactTime(timeValue); err != nil {
			return nil, errors.New("invalid value for field: " + fieldDesc.Name)
		}
		return timeValue, nil
	default:
		return nil, errors.New("unsupported field type for field: " + fieldDesc.Name)
	}
}
