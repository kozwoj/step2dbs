package engine

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

/*
FirstActiveRecordID returns the first active (not deleted) record ID in a table.
It exists in step2query because STEP2 exposes table primitives but does not expose a first-record
scan helper at the same abstraction level as GetNextRecord.
Parameters:
- tableName: the name of the table to scan for active records
- dbDef: the database definition containing metadata about tables and their record files
Returns:
- the ID of the first active record in the specified table, or an error if there are no active records or if any issues occur during lookup or file reading
*/
func FirstActiveRecordID(tableName string, dbDef *stepdb.DBDefinition) (uint32, error) {
	table, err := lookupTable(tableName, dbDef)
	if err != nil {
		return 0, err
	}

	header, err := record.GetRecordsFileHeader(table.RecordFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read record file header for table %s: %w", tableName, err)
	}

	for recordID := uint32(1); recordID <= header.LastRecordID; recordID++ {
		recordData, err := record.GetRecordData(table.RecordFile, recordID)
		if err != nil {
			return 0, fmt.Errorf("failed to read record %d from table %s: %w", recordID, tableName, err)
		}
		if len(recordData) < 1 {
			return 0, fmt.Errorf("record %d in table %s is too short", recordID, tableName)
		}
		if recordData[0] == 0x00 {
			return recordID, nil
		}
	}

	return 0, fmt.Errorf("table %s has no active records", tableName)
}

/*
NextActiveRecordID returns the next active (not deleted) record ID after currentRecordID.
It avoids row deserialization because DB-backed query-engine stages often need only record IDs.
Parameters:
- tableName: the name of the table being scanned
- currentRecordID: the current active record ID after which the scan should continue
- dbDef: the database definition containing metadata about tables and their record files
Returns:
- the ID of the next active record in the specified table, or an error if there are no more active records or if any issues occur during lookup or file reading
*/
func NextActiveRecordID(tableName string, currentRecordID uint32, dbDef *stepdb.DBDefinition) (uint32, error) {
	table, err := lookupTable(tableName, dbDef)
	if err != nil {
		return 0, err
	}

	header, err := record.GetRecordsFileHeader(table.RecordFile)
	if err != nil {
		return 0, fmt.Errorf("failed to read record file header for table %s: %w", tableName, err)
	}

	for recordID := currentRecordID + 1; recordID <= header.LastRecordID; recordID++ {
		recordData, err := record.GetRecordData(table.RecordFile, recordID)
		if err != nil {
			return 0, fmt.Errorf("failed to read record %d from table %s: %w", recordID, tableName, err)
		}
		if len(recordData) < 1 {
			return 0, fmt.Errorf("record %d in table %s is too short", recordID, tableName)
		}
		if recordData[0] == 0x00 {
			return recordID, nil
		}
	}

	return 0, fmt.Errorf("no active records after record %d in table %s", currentRecordID, tableName)
}

func lookupTable(tableName string, dbDef *stepdb.DBDefinition) (*stepdb.TableDescription, error) {
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return nil, fmt.Errorf("table %s does not exist", tableName)
	}

	table := dbDef.Tables[tableIndex]
	if table.RecordFile == nil {
		return nil, fmt.Errorf("record file is not initialized for table %s", tableName)
	}

	return table, nil
}
