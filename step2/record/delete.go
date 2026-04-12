package record

import (
	"fmt"
	"github.com/kozwoj/step2/db"
)

/* gofmt:off

DeleteRecord deletes an existing record from a table identified by its recordID.
Parameters:
- tableName: name of the table
- recordID: the record identifier (uint32)
- dbDef: DBDefinition struct with the database definition
Returns:
- error: if there is an issue deleting the record

The function performs the following steps:
- Find table definition in DBDefinition
- Read existing record data
- Validate record exists and is not deleted
- Deserialize existing record to get current field values
- Remove from primary index (if table has primary key)
- Remove from STRING field dictionaries/postings (for each STRING field with a value)
- Call DeleteRecordData (handles sets, marks deleted, updates linked list)

gofmt:on */

func DeleteRecord(tableName string, recordID uint32, dbDef *db.DBDefinition) error {
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
		return fmt.Errorf("record is already deleted")
	}

	// Extract record body (skip header)
	recordBody := recordData[tableDescription.RecordLayout.HeaderSize:]

	// Deserialize existing record to get current field values
	currentValues, err := DeserializeRecord(recordBody, tableDescription)
	if err != nil {
		return fmt.Errorf("failed to deserialize record: %w", err)
	}

	// Step 1: Remove from primary index (if table has primary key)
	if tableDescription.Key != -1 {
		pkFieldDesc := tableDescription.RecordLayout.Fields[tableDescription.Key]
		pkValue, exists := currentValues[pkFieldDesc.Name]
		if !exists {
			return fmt.Errorf("primary key field '%s' not found in record", pkFieldDesc.Name)
		}

		// Convert the primary key value to the appropriate type for the index
		var indexKey interface{} = pkValue
		switch pkFieldDesc.Type {
		case db.CHAR:
			// For CHAR types, convert string to []byte
			indexKey = []byte(pkValue.(string))
		case db.SMALLINT:
			// deserializeSmallInt returns int, but index needs int16
			indexKey = int16(pkValue.(int))
		case db.INT:
			// deserializeInt returns int, but index needs int32
			indexKey = int32(pkValue.(int))
		case db.BIGINT:
			// deserializeBigInt returns int64, which is correct
			indexKey = pkValue.(int64)
		}

		// Delete from primary index
		if tableDescription.PrimeIndex != nil {
			err = tableDescription.PrimeIndex.Delete(indexKey)
			if err != nil {
				return fmt.Errorf("failed to delete from primary index: %w", err)
			}
		}
	}

	// Step 2: Remove from STRING field dictionaries/postings
	for _, fieldDesc := range tableDescription.RecordLayout.Fields {
		if fieldDesc.Type == db.STRING {
			// Check if field has a value
			if fieldValue, exists := currentValues[fieldDesc.Name]; exists && fieldValue != nil {
				stringValue, ok := fieldValue.(string)
				if !ok {
					// Skip if value is not a string
					continue
				}

				// Find the string in the dictionary
				dictID, postingsRef, err := fieldDesc.Dictionary.FindString(stringValue)
				if err != nil {
					// If string not found, skip - might have been corrupted
					// We still want to delete the record
					continue
				}

				// Remove recordID from the postings list
				err = fieldDesc.Dictionary.RemoveRecordID(postingsRef, recordID, dictID)
				if err != nil {
					// Log error but continue - we still want to delete the record
					// The postings list might be inconsistent but the record will be marked deleted
					continue
				}
			}
		}
	}

	// Step 3: Call DeleteRecordData to handle:
	// - Deleting all sets associated with the record
	// - Marking record as deleted
	// - Adding to deleted records linked list
	// - Updating file header
	err = DeleteRecordData(tableDescription.RecordFile, recordID)
	if err != nil {
		return fmt.Errorf("failed to delete record data: %w", err)
	}

	return nil
}
