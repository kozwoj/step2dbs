package record

import (
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/kozwoj/step2/db"
)

/* gofmt:off

GetRecordByID retrieves a record by its recordID and returns it as a map.
Parameters:
- tableName: name of the table to retrieve the record from
- recordID: the record ID to retrieve
- dbDef: DBDefinition struct with the database definition
Returns:
- map[string]interface{}: map containing the record fields
- error: if there is an issue retrieving or deserializing the record

The steps performed by the function are:
- Find the table definition in DBDefinition by tableName
- Read full record data using GetRecordData (header + body)
- Check DeletedFlag - return error if record is deleted
- Extract record body (skip header)
- Deserialize record body to map[string]interface{} using DeserializeRecord

The function should be called after opening existing/created DB.

gofmt:on */

func GetRecordByID(tableName string, recordID uint32, dbDef *db.DBDefinition) (map[string]interface{}, error) {
	// Find the table definition in DBDefinition
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return nil, fmt.Errorf("table not found: %s", tableName)
	}
	tableDescription := dbDef.Tables[tableIndex]

	// Read full record data (header + body)
	recordData, err := GetRecordData(tableDescription.RecordFile, recordID)
	if err != nil {
		return nil, fmt.Errorf("failed to read record: %w", err)
	}

	// Check DeletedFlag (offset 0)
	deletedFlag := recordData[0]
	if deletedFlag == 0x01 {
		return nil, fmt.Errorf("record %d in table %s is deleted", recordID, tableName)
	}

	// Extract record body (skip header)
	recordBody := recordData[tableDescription.RecordLayout.HeaderSize:]

	// Deserialize record body to map
	recordMap, err := DeserializeRecord(recordBody, tableDescription)
	if err != nil {
		return nil, fmt.Errorf("failed to deserialize record: %w", err)
	}

	return recordMap, nil
}

/* gofmt:off

GetRecordID maps a primary key value to its recordID using the primary index.
Parameters:
- tableName: name of the table containing the primary key
- primeKey: the primary key value (int for INT/SMALLINT/BIGINT, string for CHAR)
- dbDef: DBDefinition struct with the database definition
Returns:
- uint32: the recordID corresponding to the primary key
- error: if there is an issue looking up the primary key

The steps performed by the function are:
- Find table definition in DBDefinition
- Validate table has a primary key
- Convert primeKey to appropriate type based on primary key field type
- Look up key in primary index
- Convert returned bytes to recordID

The function should be called after opening existing/created DB.

gofmt:on */

func GetRecordID(tableName string, primeKey interface{}, dbDef *db.DBDefinition) (uint32, error) {
	// Find table definition
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return 0, fmt.Errorf("table '%s' not found", tableName)
	}
	tableDescription := dbDef.Tables[tableIndex]

	// Check if table has a primary key
	if tableDescription.Key == -1 {
		return 0, fmt.Errorf("table '%s' does not have a primary key", tableName)
	}

	// Check if primary index exists
	if tableDescription.PrimeIndex == nil {
		return 0, fmt.Errorf("primary index not initialized or opened for table '%s'", tableName)
	}

	// Get primary key field description
	pkFieldDesc := tableDescription.RecordLayout.Fields[tableDescription.Key]

	// Convert primeKey to appropriate type for index lookup
	var indexKey interface{}
	switch pkFieldDesc.Type {
	case db.CHAR:
		// primeKey should be a string
		strKey, ok := primeKey.(string)
		if !ok {
			return 0, fmt.Errorf("primeKey must be a string for CHAR primary key")
		}
		if len(strKey) != pkFieldDesc.Size {
			return 0, fmt.Errorf("primeKey must be exactly %d characters for CHAR[%d] primary key", pkFieldDesc.Size, pkFieldDesc.Size)
		}
		indexKey = []byte(strKey)

	case db.SMALLINT:
		// Handle both int and int32 types
		switch v := primeKey.(type) {
		case int:
			indexKey = int16(v)
		case int32:
			indexKey = int16(v)
		case int16:
			indexKey = v
		default:
			return 0, fmt.Errorf("primeKey must be an integer type for SMALLINT primary key")
		}

	case db.INT:
		// Handle both int and int32 types
		switch v := primeKey.(type) {
		case int:
			indexKey = int32(v)
		case int32:
			indexKey = v
		default:
			return 0, fmt.Errorf("primeKey must be an integer type for INT primary key")
		}

	case db.BIGINT:
		// Handle both int and int64 types
		switch v := primeKey.(type) {
		case int:
			indexKey = int64(v)
		case int32:
			indexKey = int64(v)
		case int64:
			indexKey = v
		default:
			return 0, fmt.Errorf("primeKey must be an integer type for BIGINT primary key")
		}

	default:
		return 0, fmt.Errorf("unsupported primary key type: %d", pkFieldDesc.Type)
	}

	// Look up key in primary index
	recordIDBytes, err := tableDescription.PrimeIndex.Find(indexKey)
	if err != nil {
		return 0, fmt.Errorf("primary key not found: %w", err)
	}

	// Convert bytes to recordID (stored as 4 bytes little-endian)
	if len(recordIDBytes) != 4 {
		return 0, fmt.Errorf("invalid recordID bytes length: expected 4, got %d", len(recordIDBytes))
	}
	recordID := binary.LittleEndian.Uint32(recordIDBytes)

	return recordID, nil
}

// ErrNoMoreRecords error can be used by callers to check if the error is because there are no more records in the table when scanning.
var ErrNoMoreRecords = errors.New("no more records")

/* gofmt:off

GetNextRecord retrieves the next non-deleted record after a given currentRecordID.
Parameters:
- tableName: name of the table to retrieve the record from
- currentRecordID: the reference record ID to start scanning from
- dbDef: DBDefinition struct with the database definition
Returns:
- map[string]interface{}: map containing the record fields
- uint32: the record ID of the next record
- error: if there is an issue or no more records found (see ErrNoMoreRecords)

If the end of the table is reached without finding a non-deleted record, the function returns a sentinel
error ErrNoMoreRecords indicating no more records.

gofmt:on */

func GetNextRecord(tableName string, currentRecordID uint32, dbDef *db.DBDefinition) (map[string]interface{}, uint32, error) {
	// Find the table definition in DBDefinition
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return nil, 0, fmt.Errorf("table not found: %s", tableName)
	}
	tableDescription := dbDef.Tables[tableIndex]

	// Get the file header to know LastRecordID
	header, err := GetRecordsFileHeader(tableDescription.RecordFile)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to read file header: %w", err)
	}

	// Scan forward from currentRecordID + 1
	for nextID := currentRecordID + 1; nextID <= header.LastRecordID; nextID++ {
		// Try to get record data
		recordData, err := GetRecordData(tableDescription.RecordFile, nextID)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read record %d: %w", nextID, err)
		}

		// Check DeletedFlag (offset 0)
		deletedFlag := recordData[0]
		if deletedFlag == 0x00 {
			// Found non-deleted record
			// Extract record body (skip header)
			recordBody := recordData[tableDescription.RecordLayout.HeaderSize:]

			// Deserialize record body to map
			recordMap, err := DeserializeRecord(recordBody, tableDescription)
			if err != nil {
				return nil, 0, fmt.Errorf("failed to deserialize record: %w", err)
			}

			return recordMap, nextID, nil
		}
	}

	// No more records found
	return nil, 0, ErrNoMoreRecords
}

/* gofmt:off

GetRecordByKey retrieves a record using its primary key value.
This is a convenience function that combines GetRecordID + GetRecordByID.
Parameters:
- tableName: name of the table to retrieve the record from
- primeKey: the primary key value (int types for INT/SMALLINT/BIGINT, string for CHAR)
- dbDef: DBDefinition struct with the database definition
Returns:
- map[string]interface{}: map containing the record fields
- uint32: the record ID
- error: if there is an issue retrieving the record

The steps performed by the function are:
- Call GetRecordID to look up the recordID from the primary key
- Call GetRecordByID to retrieve and deserialize the record
- Return both the record data and recordID

The function should be called after opening existing/created DB.

gofmt:on */

func GetRecordByKey(tableName string, primeKey interface{}, dbDef *db.DBDefinition) (map[string]interface{}, uint32, error) {
	// Look up recordID using primary key
	recordID, err := GetRecordID(tableName, primeKey, dbDef)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to find record by key: %w", err)
	}

	// Retrieve record data using recordID
	recordData, err := GetRecordByID(tableName, recordID, dbDef)
	if err != nil {
		return nil, 0, fmt.Errorf("failed to retrieve record: %w", err)
	}

	return recordData, recordID, nil
}

/* gofmt:off

GetRecordsByString retrieves all recordIDs that have a specific STRING field value (exact match).

Parameters:
  - tableName: string name of the table to search
  - fieldName: string name of the STRING field to search
  - fieldValue: string exact value to search for
  - dbDef: pointer to the DBDefinition structure

Returns:
  - []uint32: slice of recordIDs that have the specified field value (empty slice if no matches)
  - error: if validation fails or I/O error occurs

Notes:
  - Only STRING type fields can be searched (they have dictionaries)
  - CHAR fields cannot be searched with this function
  - If the string is not found in the dictionary, returns empty slice (not an error)
  - The function performs exact string matching

The function should be called after opening existing/created DB.

gofmt:on */

func GetRecordsByString(tableName string, fieldName string, fieldValue string, dbDef *db.DBDefinition) ([]uint32, error) {
	// Step 1: Validate table exists
	tableIdx, found := dbDef.TableIndex[tableName]
	if !found {
		return nil, fmt.Errorf("table '%s' not found", tableName)
	}
	table := dbDef.Tables[tableIdx]

	// Step 2: Validate field exists
	fieldIdx, found := table.RecordLayout.FieldIndex[fieldName]
	if !found {
		return nil, fmt.Errorf("field '%s' not found in table '%s'", fieldName, tableName)
	}
	field := table.RecordLayout.Fields[fieldIdx]

	// Step 3: Verify field type is STRING
	if field.Type != db.STRING {
		return nil, fmt.Errorf("field '%s' is not a STRING type (cannot search with dictionary)", fieldName)
	}

	// Step 4: Access dictionary
	if field.Dictionary == nil {
		return nil, fmt.Errorf("dictionary not initialized for field '%s'", fieldName)
	}

	// Step 5: Find string in dictionary
	_, postingsRef, err := field.Dictionary.FindString(fieldValue)
	if err != nil {
		// String not found in dictionary - this is not an error, just no matches
		return []uint32{}, nil
	}

	// Step 6: Retrieve postings list
	recordIDs, err := field.Dictionary.RetrievePostings(postingsRef)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve postings: %w", err)
	}

	return recordIDs, nil
}

/* gofmt:off

GetRecordsBySubstring retrieves all recordIDs that have a STRING field value starting with the given substring.

Parameters:
  - tableName: string name of the table to search
  - fieldName: string name of the STRING field to search
  - substring: string prefix to search for (must be <= 8 characters)
  - dbDef: pointer to the DBDefinition structure

Returns:
  - []uint32: slice of recordIDs that have field values starting with the substring (empty slice if no matches)
  - error: if validation fails or I/O error occurs

Notes:
  - Only STRING type fields can be searched (they have dictionaries with prefix indexes)
  - Substring must be <= 8 characters (prefix index limitation)
  - Returns empty slice (not error) when no strings match the prefix
  - The function uses dictionary prefix search which is optimized for <= 8 byte prefixes
  - Results are deduplicated (same recordID won't appear multiple times)

The function should be called after opening existing/created DB.

gofmt:on */

func GetRecordsBySubstring(tableName string, fieldName string, substring string, dbDef *db.DBDefinition) ([]uint32, error) {
	// Step 1: Validate table exists
	tableIdx, found := dbDef.TableIndex[tableName]
	if !found {
		return nil, fmt.Errorf("table '%s' not found", tableName)
	}
	table := dbDef.Tables[tableIdx]

	// Step 2: Validate field exists
	fieldIdx, found := table.RecordLayout.FieldIndex[fieldName]
	if !found {
		return nil, fmt.Errorf("field '%s' not found in table '%s'", fieldName, tableName)
	}
	field := table.RecordLayout.Fields[fieldIdx]

	// Step 3: Verify field type is STRING
	if field.Type != db.STRING {
		return nil, fmt.Errorf("field '%s' is not a STRING type (cannot search with dictionary)", fieldName)
	}

	// Step 4: Access dictionary
	if field.Dictionary == nil {
		return nil, fmt.Errorf("dictionary not initialized for field '%s'", fieldName)
	}

	// Step 5: Validate substring
	if len(substring) == 0 {
		return nil, fmt.Errorf("substring cannot be empty")
	}
	if len(substring) > 8 {
		return nil, fmt.Errorf("substring length (%d) exceeds maximum of 8 characters", len(substring))
	}

	// Step 6: Perform prefix search
	dictIDs, err := field.Dictionary.PrefixSearch(substring)
	if err != nil {
		return nil, fmt.Errorf("prefix search failed: %w", err)
	}

	// If no matches, return empty slice
	if len(dictIDs) == 0 {
		return []uint32{}, nil
	}

	// Step 7: Collect postings for each matching dictID and deduplicate
	recordIDMap := make(map[uint32]bool)

	for _, dictID := range dictIDs {
		// Get the actual string for this dictID
		actualString, err := field.Dictionary.GetStringByID(dictID)
		if err != nil {
			return nil, fmt.Errorf("failed to get string for dictID %d: %w", dictID, err)
		}

		// Find the string to get its postingsRef
		_, postingsRef, err := field.Dictionary.FindString(actualString)
		if err != nil {
			return nil, fmt.Errorf("failed to find string '%s': %w", actualString, err)
		}

		// Retrieve postings for this string
		recordIDs, err := field.Dictionary.RetrievePostings(postingsRef)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve postings: %w", err)
		}

		// Add all recordIDs to map for deduplication
		for _, recID := range recordIDs {
			recordIDMap[recID] = true
		}
	}

	// Step 8: Convert map to slice
	result := make([]uint32, 0, len(recordIDMap))
	for recID := range recordIDMap {
		result = append(result, recID)
	}

	return result, nil
}
