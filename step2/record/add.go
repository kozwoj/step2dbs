package record

import (
	"encoding/binary"
	"fmt"
	"github.com/kozwoj/step2/db"
)

/* gofmt:off

AddNewRecord validates and serializes the input record and adds it to the table's records file.
Parameters:
- tableName: name of the table to add the record to
- recordFields: map containing the record field names and values
- dbDef: DBDefinition struct with the database definition
Returns:
- recordID: sequential ID of the newly added record
- error: if there is an issue adding the record

The steps performed by the function are:
- record table definition is found in DBDefinition
- record fields are validated for consistency against record definition
- if record contains primary key value, the table's primary index is queried for duplicate
- if record contains foreign key(s) the related table(s) primary index is/are queried for corresponding record(s)
- space for new record is allocated to get record ID
	- recordID is required for string fields serialization
	- if serialization fails, the allocated space is added to the deleted records list
- input record fields are serialized into []Char of the record body length = RecordDescription.DataSize
- header is created and added to the body
- record is written into the table's records.dat file

The function should be called after opening existing/created DB.

gofmt:on */

func AddNewRecord(tableName string, recordFields map[string]interface{}, dbDef *db.DBDefinition) (uint32, error) {
	// Find the record table definition number in DBDefinition
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return 0, fmt.Errorf("could not find table definition for table: %s", tableName)
	}
	tableDescription := dbDef.Tables[tableIndex]

	// Validate record fields for consistency against record definition
	fieldValues, err := ValidateRecord(recordFields, tableDescription)
	if err != nil {
		return 0, err
	}

	// Validate primary key constraint - check for duplicate
	err = validatePrimaryKeyConstraint(tableName, tableDescription, fieldValues)
	if err != nil {
		return 0, err
	}

	// Validate foreign key constraints - check that FK values exist in referenced tables
	err = validateForeignKeyConstraints(tableDescription, fieldValues, dbDef)
	if err != nil {
		return 0, err
	}

	// Allocate space for the new record and get its recordID
	recordID, err := allocateRecord(tableDescription)
	if err != nil {
		return 0, fmt.Errorf("failed to allocate record: %w", err)
	}

	// Serialize record fields into []byte for the record body
	recordBody, err := SerializeRecord(fieldValues, tableDescription, recordID)
	if err != nil {
		// Rollback allocation: mark record as deleted and add to deleted list
		err2 := DeleteRecordData(tableDescription.RecordFile, recordID)
		if err2 != nil {
			return 0, fmt.Errorf("failed to serialize record: %v, also failed to rollback allocation: %v", err, err2)
		}
		return 0, fmt.Errorf("failed to serialize record: %w", err)
	}

	// Concatenate header and body to create full record data
	recordDataSize := tableDescription.RecordLayout.DataSize + tableDescription.RecordLayout.HeaderSize
	recordData := make([]byte, recordDataSize)
	copy(recordData[tableDescription.RecordLayout.HeaderSize:], recordBody)
	recordData[0] = 0x00 // DeletedFlag: 0x00 (not deleted)
	// NextDeletedID: 0xFFFFFFFF (NoNextRecord) - 4 bytes
	binary.LittleEndian.PutUint32(recordData[1:5], NoNextRecord)
	// Sets: initialize each set to 0xFFFFFFFF (NoSet) - 4 bytes per set
	numSets := len(tableDescription.Sets)
	for i := 0; i < numSets; i++ {
		offset := 5 + (i * 4)
		binary.LittleEndian.PutUint32(recordData[offset:offset+4], NoSet)
	}

	// write the record data to the file at the allocated recordID
	err = OverrideRecordData(tableDescription.RecordFile, recordID, recordData)
	if err != nil {
		return 0, fmt.Errorf("failed to write new record after serialization: %w", err)
	}

	// If the table has a primary key, update the primary index with the new key value and recordID
	if tableDescription.Key != -1 && tableDescription.PrimeIndex != nil {
		// convert recordID to 4 byte slice for primary index entity value
		recordIDBytes := make([]byte, 4)
		binary.LittleEndian.PutUint32(recordIDBytes, recordID)

		// Get primary key value from validated fields
		pkValue := fieldValues[tableDescription.Key]

		// Convert the primary key value to the appropriate type for the index
		// For CHAR types, convert string to []byte
		pkField := tableDescription.RecordLayout.Fields[tableDescription.Key]
		var indexKey interface{} = pkValue
		if pkField.Type == db.CHAR {
			indexKey = []byte(pkValue.(string))
		}

		err = tableDescription.PrimeIndex.Insert(indexKey, recordIDBytes)
		if err != nil {
			return 0, fmt.Errorf("failed to update primary key index: %w", err)
		}
	}

	return recordID, nil

}

// validatePrimaryKeyConstraint checks if a primary key value already exists in the table's primary index.
// Returns an error if the table has a primary key and the value is a duplicate, otherwise returns nil.
func validatePrimaryKeyConstraint(tableName string, tableDescription *db.TableDescription, fieldValues []interface{}) error {
	// Skip if table has no primary key
	if tableDescription.Key == -1 {
		return nil
	}

	// Verify primary index is available
	if tableDescription.PrimeIndex == nil {
		return fmt.Errorf("table %s has primary key but primary index is not initialized", tableName)
	}

	// Extract primary key value from validated fields
	pkValue := fieldValues[tableDescription.Key]

	// Convert the primary key value to the appropriate type for the index
	// For CHAR types, convert string to []byte
	pkField := tableDescription.RecordLayout.Fields[tableDescription.Key]
	var indexKey interface{} = pkValue
	if pkField.Type == db.CHAR {
		indexKey = []byte(pkValue.(string))
	}

	// Check if this primary key value already exists in the index
	_, err := tableDescription.PrimeIndex.Find(indexKey)
	if err == nil {
		// Key found - duplicate primary key
		return fmt.Errorf("duplicate primary key value: %v", pkValue)
	}

	// If err != nil, key not found, which is what we want
	return nil
}

// validateForeignKeyConstraints checks that all foreign key values exist in their referenced tables.
// Returns an error if any FK constraint is violated, otherwise returns nil.
func validateForeignKeyConstraints(tableDescription *db.TableDescription, fieldValues []interface{}, dbDef *db.DBDefinition) error {
	// Iterate through all fields to find foreign keys
	for fieldIndex, field := range tableDescription.RecordLayout.Fields {
		if !field.IsForeignKey {
			continue
		}

		// Get the FK value from validated fields
		fkValue := fieldValues[fieldIndex]

		// If field is optional and value is nil, skip FK validation
		if field.IsOptional && fkValue == nil {
			continue
		}

		// Get the referenced table
		refTableIndex, exists := dbDef.TableIndex[field.ForeignKeyTable]
		if !exists {
			// This should never happen because it's validated during DB creation
			return fmt.Errorf("internal error: referenced table %s not found", field.ForeignKeyTable)
		}
		refTable := dbDef.Tables[refTableIndex]

		// Verify referenced table has a primary index
		if refTable.PrimeIndex == nil {
			return fmt.Errorf("internal error: referenced table %s has no primary index", field.ForeignKeyTable)
		}

		// Check if the foreign key value exists in the referenced table's primary index
		// For CHAR types, convert string to []byte for index lookup
		lookupValue := fkValue
		if field.Type == db.CHAR {
			// Convert string to []byte for CHAR field lookups
			lookupValue = []byte(fkValue.(string))
		}

		_, err := refTable.PrimeIndex.Find(lookupValue)
		if err != nil {
			// FK value not found in referenced table
			return fmt.Errorf("foreign key constraint violation: value %v for field '%s' not found in table '%s'", fkValue, field.Name, field.ForeignKeyTable)
		}
	}

	return nil
}

// allocateRecord creates an empty record (header + empty body) and writes it to the table's
// records file, returning the assigned recordID. The recordID is needed for STRING field serialization.
// Parameters:
//   - tableDescription: pointer to the TableDescription containing record layout and file reference
//
// Returns:
//   - recordID: the sequential ID assigned to the allocated record
//   - error: if there is an issue allocating the record
func allocateRecord(tableDescription *db.TableDescription) (uint32, error) {
	// Verify record file is available
	if tableDescription.RecordFile == nil {
		return 0, fmt.Errorf("record file not initialized for table %s", tableDescription.Name)
	}

	// Calculate total record size: header + body
	recordLayout := tableDescription.RecordLayout
	totalSize := recordLayout.HeaderSize + recordLayout.DataSize
	recordData := make([]byte, totalSize)

	// Create record header
	// DeletedFlag: 0x00 (not deleted)
	recordData[0] = 0x00

	// NextDeletedID: 0xFFFFFFFF (NoNextRecord) - 4 bytes
	binary.LittleEndian.PutUint32(recordData[1:5], NoNextRecord)

	// Sets: initialize each set to 0xFFFFFFFF (NoSet) - 4 bytes per set
	numSets := len(tableDescription.Sets)
	for i := 0; i < numSets; i++ {
		offset := 5 + (i * 4)
		binary.LittleEndian.PutUint32(recordData[offset:offset+4], NoSet)
	}

	// Body is already zero-initialized by make()

	// Write the empty record to the file and get assigned recordID
	recordID, err := AddRecordData(tableDescription.RecordFile, recordData)
	if err != nil {
		return 0, err
	}

	return recordID, nil
}
