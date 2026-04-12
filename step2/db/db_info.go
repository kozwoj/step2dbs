package db

import (
	"encoding/binary"
	"errors"
	"fmt"
)

// Custom error variables for DB info operations
var (
	ErrDBNotOpen = errors.New("database is not open")
)

/* gofmt:off
GetSchema retrieves the schema of the currently opened database as a structured object.

This function converts the internal DBDefinition structure into a schema object that
mirrors the DDL definition format. It's used by the DML layer to return schema information
to clients.

Parameters:
- None (operates on the global database definition)

Returns:
- map[string]interface{}: Schema object with the following structure:
	{
		"name": string (database/schema name),
		"tables": [
			{
				"name": string (table name),
				"primaryKey": string (primary key field name, empty if no PK),
				"fields": [
					{
						"name": string (field name),
						"type": string (field type: "SMALLINT", "INT", "BIGINT", "DECIMAL", "FLOAT", "STRING", "CHAR", "BOOLEAN", "DATE", "TIME"),
						"constraints": [string, ...] (e.g., ["PRIMARY KEY"], ["FOREIGN KEY Departments"], ["OPTIONAL"]),
						"size": int (optional, for CHAR: character count, for STRING: size limit or 0 if unlimited)
					},
					...
				],
				"sets": [
					{
						"name": string (set name),
						"memberTable": string (name of table that stores set members)
					},
					...
				]
			},
			...
		]
	}
- error: ErrDBNotOpen if no database is currently open, nil otherwise
gofmt:on */

func GetSchema() (map[string]interface{}, error) {
	// Check if database is open
	if !DefinitionInitialized() {
		return nil, ErrDBNotOpen
	}

	// Get the database definition
	dbDef := Definition()

	// Build schema object
	schema := map[string]interface{}{
		"name":   dbDef.Name,
		"tables": []map[string]interface{}{},
	}

	tables := []map[string]interface{}{}

	// Iterate through tables
	for _, table := range dbDef.Tables {
		// Determine primary key field name
		primaryKey := ""
		if table.Key >= 0 && table.Key < len(table.RecordLayout.Fields) {
			primaryKey = table.RecordLayout.Fields[table.Key].Name
		}

		// Build fields array
		fields := []map[string]interface{}{}
		for i, field := range table.RecordLayout.Fields {
			// Convert field type to string
			fieldTypeStr := fieldTypeToString(field.Type)

			// Build constraints array
			constraints := []string{}
			if i == table.Key {
				constraints = append(constraints, "PRIMARY KEY")
			}
			if field.IsForeignKey {
				constraints = append(constraints, fmt.Sprintf("FOREIGN KEY %s", field.ForeignKeyTable))
			}
			if field.IsOptional {
				constraints = append(constraints, "OPTIONAL")
			}

			// Build field object
			fieldObj := map[string]interface{}{
				"name":        field.Name,
				"type":        fieldTypeStr,
				"constraints": constraints,
			}

			// Add size for CHAR and STRING types
			if field.Type == CHAR {
				fieldObj["size"] = field.Size // For CHAR, this is the character count
			} else if field.Type == STRING {
				fieldObj["size"] = field.StringSizeLimit // For STRING, this is the size limit (0 if unlimited)
			}

			fields = append(fields, fieldObj)
		}

		// Build sets array
		sets := []map[string]interface{}{}
		for _, set := range table.Sets {
			setObj := map[string]interface{}{
				"name":        set.Name,
				"memberTable": set.MemberTableName,
			}
			sets = append(sets, setObj)
		}

		// Build table object
		tableObj := map[string]interface{}{
			"name":       table.Name,
			"primaryKey": primaryKey,
			"fields":     fields,
			"sets":       sets,
		}

		tables = append(tables, tableObj)
	}

	schema["tables"] = tables

	return schema, nil
}

// fieldTypeToString converts a FieldType enum to its string representation
func fieldTypeToString(ft FieldType) string {
	switch ft {
	case SMALLINT:
		return "SMALLINT"
	case INT:
		return "INT"
	case BIGINT:
		return "BIGINT"
	case DECIMAL:
		return "DECIMAL"
	case FLOAT:
		return "FLOAT"
	case STRING:
		return "STRING"
	case CHAR:
		return "CHAR"
	case BOOLEAN:
		return "BOOLEAN"
	case DATE:
		return "DATE"
	case TIME:
		return "TIME"
	default:
		return "UNKNOWN"
	}
}

/* gofmt:off
GetTableStats retrieves statistics for the specified tables in the currently opened database.

For each requested table, returns:
- allocated_records: Total number of records allocated (including deleted)
- deleted_list_length: Number of deleted/reusable record spaces
- Active records = allocated_records - deleted_list_length
- Dictionary statistics for each STRING field (field name and number of strings)

Parameters:
- tableNames: Slice of table names to retrieve statistics for (must contain at least one name)

Returns:
- []map[string]interface{}: Slice of table statistics objects, one per successfully queried table
- []string: Slice of error messages for tables that failed (empty if all succeeded)
- error: Critical error (e.g., database not open), nil if operation could proceed

Table statistics object structure:
{
	"name": string (table name),
	"allocated_records": int,
	"deleted_list_length": int,
	"dictionaries": [
		{
			"field_name": string,
			"number_of_strings": int
		},
		...
	]
}
gofmt:on */

func GetTableStats(tableNames []string) ([]map[string]interface{}, []string, error) {
	// Check if database is open
	if !DefinitionInitialized() {
		return nil, nil, ErrDBNotOpen
	}

	// Validate tableNames is not empty
	if len(tableNames) == 0 {
		return nil, []string{"table names array cannot be empty"}, nil
	}

	dbDef := Definition()
	stats := []map[string]interface{}{}
	errors := []string{}

	// Process each requested table
	for _, tableName := range tableNames {
		// Find table in schema
		tableIdx, exists := dbDef.TableIndex[tableName]
		if !exists {
			errors = append(errors, fmt.Sprintf("table '%s' not found in schema", tableName))
			continue
		}

		table := dbDef.Tables[tableIdx]

		// Read record file header directly (avoid importing record package to prevent cycle)
		// RecordFileHeader is 12 bytes: TableNo(2) + RecordLength(2) + LastRecordID(4) + FirstDeletedID(4)
		headerBuf := make([]byte, 12)
		_, err := table.RecordFile.ReadAt(headerBuf, 0)
		if err != nil {
			errors = append(errors, fmt.Sprintf("table '%s': failed to read record file header: %v", tableName, err))
			continue
		}

		// LastRecordID is at bytes 4-8, this is the allocated_records
		lastRecordID := binary.LittleEndian.Uint32(headerBuf[4:8])
		allocatedRecords := int(lastRecordID)

		// FirstDeletedID is at bytes 8-12
		firstDeletedID := binary.LittleEndian.Uint32(headerBuf[8:12])
		const NoFirstRecord uint32 = 0xFFFF
		const NoNextRecord uint32 = 0xFFFF

		// Count deleted records by traversing the deleted list
		deletedListLength := 0
		if firstDeletedID != NoFirstRecord {
			currentID := firstDeletedID
			recordLength := int(binary.LittleEndian.Uint16(headerBuf[2:4]))

			for currentID != NoNextRecord {
				deletedListLength++

				// Read the record header to get NextDeletedID (bytes 1-5 of record)
				recordOffset := int64(12) + int64(currentID)*int64(recordLength)
				recordHeaderBuf := make([]byte, 5)
				_, err := table.RecordFile.ReadAt(recordHeaderBuf, recordOffset)
				if err != nil {
					errors = append(errors, fmt.Sprintf("table '%s': failed to read deleted record %d: %v", tableName, currentID, err))
					break
				}

				// NextDeletedID is at bytes 1-5 of the record
				currentID = binary.LittleEndian.Uint32(recordHeaderBuf[1:5])
			}
		}

		// Build dictionaries array for STRING fields
		dictionaries := []map[string]interface{}{}
		for _, field := range table.RecordLayout.Fields {
			if field.Type == STRING && field.Dictionary != nil {
				// Read NumOfStrings from the dictionary strings file header
				// StringsFileHeader is 12 bytes: EndOffset(8) + NumOfStrings(4)
				// NumOfStrings is at bytes 8-12
				dictHeaderBuf := make([]byte, 12)
				_, err := field.Dictionary.StringsFile.ReadAt(dictHeaderBuf, 0)
				if err != nil {
					errors = append(errors, fmt.Sprintf("table '%s', field '%s': failed to read dictionary header: %v", tableName, field.Name, err))
					continue
				}
				numStrings := int(binary.LittleEndian.Uint32(dictHeaderBuf[8:12]))

				dictObj := map[string]interface{}{
					"field_name":        field.Name,
					"number_of_strings": numStrings,
				}
				dictionaries = append(dictionaries, dictObj)
			}
		}

		// Build stats object
		statObj := map[string]interface{}{
			"name":                tableName,
			"allocated_records":   allocatedRecords,
			"deleted_list_length": deletedListLength,
			"dictionaries":        dictionaries,
		}

		stats = append(stats, statObj)
	}

	return stats, errors, nil
}
