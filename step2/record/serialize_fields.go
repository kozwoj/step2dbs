package record

import (
	"encoding/binary"
	"math"
	"github.com/kozwoj/indexing/dictionary/dictionary"
)

/*
serializeString serializes a string field value by adding it to the dictionary, updating
the postings list with the recordID, and writing the dictionary ID to the record body.

The function performs the following operations:
1. Adds the string to the dictionary (if not already present) using dictionary.AddString
  - This is idempotent: if the string exists, returns existing dictID and postingsRef
  - Automatically adds the string to the prefix index (handled internally by AddString)

2. Adds the recordID to the string's postings list using dictionary.AddRecordID
  - This is also idempotent: if recordID already exists in the postings list, no error occurs

3. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
4. Writes the dictID (4 bytes, little-endian) at the field data offset

Note: The prefix index update is automatically handled by dictionary.AddString() - no separate
call is needed. The prefix index allows efficient prefix-based searches on string values.

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

The caller is responsible for:
  - Creating the record body with appropriate size before calling this function
  - Identifying which dictionary to use for the field
  - Calculating the correct offset within the record body:
    NOTE: FieldDescription.Offset is relative to the entire record (header + body),
    but this function requires the offset within just the body.
    Calculate as: bodyOffset = fieldDescription.Offset - tableDescription.RecordLayout.HeaderSize
  - Managing the undo stack to rollback postings updates if subsequent operations fail
  - The caller must be able to remove the recordID from the postings list if rollback is needed

Parameters:
  - value: the string value to serialize
  - dict: pointer to the Dictionary object for this field
  - recordID: the record ID to add to the postings list for this string
  - offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
    NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
  - recordBody: byte slice representing the record body only (not including header)

Returns:
- dictID: the dictionary ID assigned to or already associated with this string (used for undo operations)
- postingsRef: reference to the postings list for this string (used for undo operations)
- error: if dictionary operations fail or if parameters are invalid

Example usage:

	dictID, postingsRef, err := serializeString("John", employeeNameDict, recordID, 10, recordBody)
	if err != nil {
		// handle error and rollback previous string operations
	}
	// Push undo operation onto stack:
	// undoStack = append(undoStack, UndoOperation{dict: employeeNameDict, dictID: dictID, postingsRef: postingsRef, recordID: recordID})
*/
func serializeString(value string, dict *dictionary.Dictionary, recordID uint32, offset int, recordBody []byte) (uint32, uint32, error) {
	// Step 1: Add string to dictionary and get dictID and postingsRef
	// The AddString method is idempotent - if string exists, it returns existing IDs
	// It also automatically adds the string to the prefix index
	dictID, postingsRef, err := dict.AddString(value)
	if err != nil {
		return 0, 0, err
	}

	// Step 2: Add recordID to the postings list for this string
	// The AddRecordID method is idempotent - if recordID exists, it returns nil without error
	err = dict.AddRecordID(postingsRef, recordID, dictID)
	if err != nil {
		return 0, 0, err
	}

	// Step 3: Write HasValue flag (0x01) one byte BEFORE the field data
	// Note: FieldDescription.Offset points to the field data, not the HasValue flag
	recordBody[offset-1] = 0x01

	// Step 4: Write dictID (4 bytes, little-endian) at the field data offset
	binary.LittleEndian.PutUint32(recordBody[offset:offset+4], dictID)

	// Return dictID and postingsRef so caller can use them for undo operations if needed
	return dictID, postingsRef, nil
}

/*
serializeSmallInt serializes a SMALLINT (int16) field value to the record body.

The function performs the following operations:
1. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
2. Writes the int16 value (2 bytes, little-endian) at the field data offset

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the int16 value to serialize
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeSmallInt(int16(100), bodyOffset, recordBody)
*/
func serializeSmallInt(value int16, offset int, recordBody []byte) {
	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write int16 value (2 bytes, little-endian) at the field data offset
	binary.LittleEndian.PutUint16(recordBody[offset:offset+2], uint16(value))
}

/*
serializeInt serializes an INT (int32) field value to the record body.

The function performs the following operations:
1. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
2. Writes the int32 value (4 bytes, little-endian) at the field data offset

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the int32 value to serialize
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeInt(int32(50000), bodyOffset, recordBody)
*/
func serializeInt(value int32, offset int, recordBody []byte) {
	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write int32 value (4 bytes, little-endian) at the field data offset
	binary.LittleEndian.PutUint32(recordBody[offset:offset+4], uint32(value))
}

/*
serializeBigInt serializes a BIGINT (int64) field value to the record body.

The function performs the following operations:
1. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
2. Writes the int64 value (8 bytes, little-endian) at the field data offset

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the int64 value to serialize
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeBigInt(int64(9223372036854775807), bodyOffset, recordBody)
*/
func serializeBigInt(value int64, offset int, recordBody []byte) {
	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write int64 value (8 bytes, little-endian) at the field data offset
	binary.LittleEndian.PutUint64(recordBody[offset:offset+8], uint64(value))
}

/*
serializeDecimal serializes a DECIMAL field value to the record body.

The function performs the following operations:
1. Parses the decimal string into a Decimal struct (IntPart, FracPart, Scale, Neg)
2. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
3. Writes the Decimal components (19 bytes total, little-endian) at the field data offset:
   - IntPart: 8 bytes (uint64)
   - FracPart: 8 bytes (uint64)
   - Scale: 1 byte (uint8)
   - Neg: 1 byte (0x00 for false, 0x01 for true)

Note: This function is called after validation, so the decimal string is assumed to be valid.
No error checking is performed.

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the decimal string value to serialize (e.g., "123.45" or "-999.123")
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeDecimal("123.45", bodyOffset, recordBody)
*/
func serializeDecimal(value string, offset int, recordBody []byte) {
	// Parse decimal string into Decimal struct (no error check - already validated)
	decimal, _ := DecimalFromString(value)

	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write Decimal components (19 bytes total) at the field data offset
	// IntPart: 8 bytes (uint64)
	binary.LittleEndian.PutUint64(recordBody[offset:offset+8], decimal.IntPart)

	// FracPart: 8 bytes (uint64)
	binary.LittleEndian.PutUint64(recordBody[offset+8:offset+16], decimal.FracPart)

	// Scale: 1 byte (uint8)
	recordBody[offset+16] = decimal.Scale

	// Neg: 1 byte (0x00 for false, 0x01 for true)
	if decimal.Neg {
		recordBody[offset+17] = 0x01
	} else {
		recordBody[offset+17] = 0x00
	}
}

/*
serializeFloat serializes a FLOAT (float64) field value to the record body.

The function performs the following operations:
1. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
2. Writes the float64 value (8 bytes, little-endian) at the field data offset

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the float64 value to serialize
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeFloat(3.14159, bodyOffset, recordBody)
*/
func serializeFloat(value float64, offset int, recordBody []byte) {
	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write float64 value (8 bytes, little-endian) at the field data offset
	// Use math.Float64bits to convert float64 to uint64 binary representation
	binary.LittleEndian.PutUint64(recordBody[offset:offset+8], math.Float64bits(value))
}

/*
serializeChar serializes a CHAR[N] (fixed-size character array) field value to the record body.

The function performs the following operations:
1. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
2. Writes the string bytes at the field data offset (exactly size bytes)

Note: This function is called after validation, so the string is assumed to be exactly
the correct size. The size parameter must match FieldDescription.Size.

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the string value to serialize (must be exactly 'size' characters)
- size: the fixed size of the CHAR array (from FieldDescription.Size)
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeChar("Exactly15chars!", 15, bodyOffset, recordBody)
*/
func serializeChar(value string, size int, offset int, recordBody []byte) {
	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write the string bytes (exactly size bytes) at the field data offset
	copy(recordBody[offset:offset+size], []byte(value))
}

/*
serializeBoolean serializes a BOOLEAN field value to the record body.

The function performs the following operations:
1. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
2. Writes the boolean value (1 byte: 0x01 for true, 0x00 for false) at the field data offset

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the boolean value to serialize
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeBoolean(true, bodyOffset, recordBody)
*/
func serializeBoolean(value bool, offset int, recordBody []byte) {
	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write boolean value (1 byte) at the field data offset
	if value {
		recordBody[offset] = 0x01
	} else {
		recordBody[offset] = 0x00
	}
}

/*
serializeDate serializes a DATE field value to the record body.

The function performs the following operations:
1. Parses the date string ("YYYY-MM-DD" format) and converts to days since 2000-01-01
2. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
3. Writes the days value (8 bytes, uint64, little-endian) at the field data offset

Note: This function is called after validation, so the date string is assumed to be valid.
No error checking is performed.

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the date string in "YYYY-MM-DD" format
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeDate("2024-06-15", bodyOffset, recordBody)
*/
func serializeDate(value string, offset int, recordBody []byte) {
	// Parse date string and convert to days since 2000-01-01 (no error check - already validated)
	days, _ := ParseDate(value)

	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write days value (8 bytes, uint64, little-endian) at the field data offset
	binary.LittleEndian.PutUint64(recordBody[offset:offset+8], days)
}

/*
serializeTime serializes a TIME field value to the record body.

The function performs the following operations:
1. Parses the time string (H:MM, H:MM:SS, or H:MM:SS.mmm format) and converts to milliseconds since midnight
2. Writes the HasValue flag (0x01) one byte BEFORE the field data offset
3. Writes the milliseconds value (8 bytes, uint64, little-endian) at the field data offset

Note: This function is called after validation, so the time string is assumed to be valid.
No error checking is performed.

Important: FieldDescription.Offset points to the FIELD DATA, not the HasValue flag.
The HasValue flag is always stored at (offset - 1), immediately before the field data.

Parameters:
- value: the time string in H:MM, H:MM:SS, or H:MM:SS.mmm format
- offset: byte offset WITHIN THE RECORD BODY where this field's DATA starts (0-based from body start)
          NOTE: This is FieldDescription.Offset - HeaderSize. The HasValue flag is at (offset - 1)
- recordBody: byte slice representing the record body only (not including header)

Example usage:
	serializeTime("14:30:45.123", bodyOffset, recordBody)
*/
func serializeTime(value string, offset int, recordBody []byte) {
	// Parse time string and convert to milliseconds since midnight (no error check - already validated)
	millis, _ := ConvertCompactTime(value)

	// Write HasValue flag (0x01) one byte BEFORE the field data
	recordBody[offset-1] = 0x01

	// Write milliseconds value (8 bytes, uint64, little-endian) at the field data offset
	binary.LittleEndian.PutUint64(recordBody[offset:offset+8], millis)
}
