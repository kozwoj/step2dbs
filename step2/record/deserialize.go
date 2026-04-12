package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"strings"
)

/* gofmt:off

DeserializeRecord deserializes a record body byte slice into a map of field name to field value.
Parameters:
- recordBody: byte slice representing the record BODY only (header already stripped)
- tableDescription: instance of TableDescription struct containing the schema information for the table
Returns:
- map[string]interface{}: map with field names as keys and their deserialized values
- error: error if deserialization fails

The deserialization follows these steps:
- Initialize result map
- For each field in the table schema:
  - Calculate bodyOffset = field.Offset - RecordLayout.HeaderSize
  - Read HasValue flag at recordBody[bodyOffset - 1]
  - If HasValue == 0x00, skip this field (do not add to map)
  - Otherwise, deserialize the field based on its type and add to map
- Return the populated map

This is the reverse operation of SerializeRecord.

gofmt:on */

func DeserializeRecord(recordBody []byte, tableDescription *db.TableDescription) (map[string]interface{}, error) {
	// Initialize result map
	fieldValues := make(map[string]interface{})

	// Deserialize each field
	for _, fieldDesc := range tableDescription.RecordLayout.Fields {
		// Calculate offset within the record body
		bodyOffset := fieldDesc.Offset - tableDescription.RecordLayout.HeaderSize

		// Check HasValue flag (at bodyOffset - 1)
		hasValue := recordBody[bodyOffset-1]
		if hasValue == 0x00 {
			// Field has no value (NULL) - skip it
			continue
		}

		// Field has a value - deserialize based on type
		var value interface{}
		var err error

		switch fieldDesc.Type {
		case db.SMALLINT:
			value = deserializeSmallInt(recordBody, bodyOffset)

		case db.INT:
			value = deserializeInt(recordBody, bodyOffset)

		case db.BIGINT:
			value = deserializeBigInt(recordBody, bodyOffset)

		case db.DECIMAL:
			value = deserializeDecimal(recordBody, bodyOffset)

		case db.FLOAT:
			value = deserializeFloat(recordBody, bodyOffset)

		case db.STRING:
			value, err = deserializeString(recordBody, bodyOffset, fieldDesc.Dictionary)
			if err != nil {
				return nil, fmt.Errorf("failed to deserialize STRING field %s: %w", fieldDesc.Name, err)
			}

		case db.CHAR:
			value = deserializeChar(recordBody, bodyOffset, fieldDesc.Size)

		case db.BOOLEAN:
			value = deserializeBoolean(recordBody, bodyOffset)

		case db.DATE:
			value = deserializeDate(recordBody, bodyOffset)

		case db.TIME:
			value = deserializeTime(recordBody, bodyOffset)

		default:
			return nil, fmt.Errorf("unsupported field type for field: %s", fieldDesc.Name)
		}

		// Add field to result map
		fieldValues[fieldDesc.Name] = value
	}

	return fieldValues, nil
}

// deserializeSmallInt deserializes a SMALLINT (int16) field from the record body.
func deserializeSmallInt(recordBody []byte, offset int) int {
	value := int16(binary.LittleEndian.Uint16(recordBody[offset : offset+2]))
	return int(value)
}

// deserializeInt deserializes an INT (int32) field from the record body.
func deserializeInt(recordBody []byte, offset int) int {
	value := int32(binary.LittleEndian.Uint32(recordBody[offset : offset+4]))
	return int(value)
}

// deserializeBigInt deserializes a BIGINT (int64) field from the record body.
func deserializeBigInt(recordBody []byte, offset int) int64 {
	value := int64(binary.LittleEndian.Uint64(recordBody[offset : offset+8]))
	return value
}

// deserializeDecimal deserializes a DECIMAL field from the record body.
// The decimal is stored as 19 bytes: IntPart (8), FracPart (8), Scale (1), Neg (1)
func deserializeDecimal(recordBody []byte, offset int) string {
	var decimal Decimal

	// Read IntPart (8 bytes)
	decimal.IntPart = binary.LittleEndian.Uint64(recordBody[offset : offset+8])

	// Read FracPart (8 bytes)
	decimal.FracPart = binary.LittleEndian.Uint64(recordBody[offset+8 : offset+16])

	// Read Scale (1 byte)
	decimal.Scale = recordBody[offset+16]

	// Read Neg (1 byte)
	decimal.Neg = recordBody[offset+17] != 0x00

	// Convert to string using the Decimal.String() method
	return decimal.String()
}

// deserializeFloat deserializes a FLOAT (float64) field from the record body.
func deserializeFloat(recordBody []byte, offset int) float64 {
	bits := binary.LittleEndian.Uint64(recordBody[offset : offset+8])
	return math.Float64frombits(bits)
}

// deserializeString deserializes a STRING field from the record body.
// The string is stored as a 4-byte dictionary ID.
func deserializeString(recordBody []byte, offset int, dict *dictionary.Dictionary) (string, error) {
	// Read dictionary ID (4 bytes)
	dictID := binary.LittleEndian.Uint32(recordBody[offset : offset+4])

	// Lookup string in dictionary
	if dict == nil {
		return "", errors.New("dictionary not initialized")
	}

	str, err := dict.GetStringByID(dictID)
	if err != nil {
		return "", fmt.Errorf("failed to lookup string with dictID %d: %w", dictID, err)
	}

	return str, nil
}

// deserializeChar deserializes a CHAR[N] (fixed-size character array) field from the record body.
// The string is stored as exactly 'size' bytes.
func deserializeChar(recordBody []byte, offset int, size int) string {
	// Read the string bytes
	strBytes := recordBody[offset : offset+size]

	// Convert to string and trim any null bytes from the end
	str := string(strBytes)
	str = strings.TrimRight(str, "\x00")

	return str
}

// deserializeBoolean deserializes a BOOLEAN field from the record body.
func deserializeBoolean(recordBody []byte, offset int) bool {
	return recordBody[offset] != 0x00
}

// deserializeDate deserializes a DATE field from the record body.
// The date is stored as 8 bytes (uint64) representing days since 2000-01-01.
func deserializeDate(recordBody []byte, offset int) string {
	days := binary.LittleEndian.Uint64(recordBody[offset : offset+8])
	return FormatDate(days)
}

// deserializeTime deserializes a TIME field from the record body.
// The time is stored as 8 bytes (uint64) representing milliseconds since midnight.
func deserializeTime(recordBody []byte, offset int) string {
	millis := binary.LittleEndian.Uint64(recordBody[offset : offset+8])
	return formatTime(millis)
}

// formatTime converts milliseconds since midnight to time string format "HH:MM:SS" or "HH:MM:SS.mmm"
func formatTime(millis uint64) string {
	hours := millis / 3_600_000
	remaining := millis % 3_600_000
	minutes := remaining / 60_000
	remaining = remaining % 60_000
	seconds := remaining / 1_000
	ms := remaining % 1_000

	if ms == 0 {
		return fmt.Sprintf("%d:%02d:%02d", hours, minutes, seconds)
	}
	return fmt.Sprintf("%d:%02d:%02d.%03d", hours, minutes, seconds, ms)
}
