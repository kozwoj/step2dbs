package engine

import (
	"encoding/binary"
	"errors"
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
	"github.com/kozwoj/step2query/pipeline"
)

var ErrNoMoreRecords = errors.New("no more records")

/*
StateKind identifies the kind of runtime state flowing between pipeline stages.
*/
type StateKind = pipeline.StateKind

const (
	StateSourceDBTableSet StateKind = pipeline.StateSourceDBTableSet
	StateDBTableWorkingSet StateKind = pipeline.StateDBTableWorkingSet
	StateReturnWorkingSet  StateKind = pipeline.StateReturnWorkingSet
)

/*
FieldType is the engine-level field type used by RecordDefinition.
It mirrors STEP2 field types.
*/
type FieldType = pipeline.FieldType

const (
	SMALLINT FieldType = pipeline.SMALLINT
	INT      FieldType = pipeline.INT
	BIGINT   FieldType = pipeline.BIGINT
	DECIMAL  FieldType = pipeline.DECIMAL
	FLOAT    FieldType = pipeline.FLOAT
	STRING   FieldType = pipeline.STRING
	CHAR     FieldType = pipeline.CHAR
	BOOLEAN  FieldType = pipeline.BOOLEAN
	DATE     FieldType = pipeline.DATE
	TIME     FieldType = pipeline.TIME
)

/*
FieldDef is a simplified field description used by the query engine.
It contains only the metadata needed by runtime states and later stage execution.
*/
type FieldDef = pipeline.FieldDef

/*
RecordDefinition is the simplified schema description shared by runtime states.
It is derived either from a STEP2 table description or from a return clause.
*/
type RecordDefinition = pipeline.RecordDefinition

func QualifiedFieldKey(tableName string, fieldName string) string {
	return pipeline.QualifiedFieldKey(tableName, fieldName)
}

/*
State is the minimal common interface implemented by all runtime states.
Parameters:
- none
Returns:
- none
*/
type State interface {
	Kind() StateKind
	TableName() string
	Size() int
	RecordDef() RecordDefinition
	DBDef() *stepdb.DBDefinition
	CurrentRecordID() uint32
	GetFirstRecord() (map[string]interface{}, error)
	GetNextRecord() (map[string]interface{}, error)
}

/*
StateBase stores the metadata shared by all runtime states.
It provides the common methods used by the State interface.
*/
type StateBase struct {
	kind         StateKind
	tableName    string
	recordDef    RecordDefinition
	dbDef        *stepdb.DBDefinition
	currentRecID uint32
}

func (s *StateBase) Kind() StateKind {
	return s.kind
}

func (s *StateBase) TableName() string {
	return s.tableName
}

func (s *StateBase) RecordDef() RecordDefinition {
	return s.recordDef
}

func (s *StateBase) DBDef() *stepdb.DBDefinition {
	return s.dbDef
}

func (s *StateBase) CurrentRecordID() uint32 {
	return s.currentRecID
}

// ==================================== SourceDBTableSet ====================================

/*
SourceDBTableSet represents the initial source DB table state.
It does not hold selected record IDs; it iterates directly over the underlying STEP2 table.
*/
type SourceDBTableSet struct {
	StateBase
}

/*
NewSourceDBTableSet creates a source DB table state for the named table.
Parameters:
- tableName: the name of the STEP2 table used as the pipeline source
- dbDef: the database definition containing table metadata and file references
Returns:
- a SourceDBTableSet initialized for sequential access to the source table, or an error if the table cannot be resolved
*/
func NewSourceDBTableSet(tableName string, dbDef *stepdb.DBDefinition) (*SourceDBTableSet, error) {
	table, err := lookupTable(tableName, dbDef)
	if err != nil {
		return nil, err
	}

	return &SourceDBTableSet{
		StateBase: StateBase{
			kind:      StateSourceDBTableSet,
			tableName: tableName,
			recordDef: buildRecordDefinition(table),
			dbDef:     dbDef,
		},
	}, nil
}

func (s *SourceDBTableSet) Size() int {
	count, _ := countActiveTableRecords(s.tableName, s.dbDef)
	return count
}

/*
GetFirstRecord returns the first active record of the source DB table.
It also updates the current record ID tracked by the state.
*/
func (s *SourceDBTableSet) GetFirstRecord() (map[string]interface{}, error) {
	rec, recID, err := record.GetNextRecord(s.tableName, 0, s.dbDef)
	if err != nil {
		if errors.Is(err, record.ErrNoMoreRecords) {
			return nil, ErrNoMoreRecords
		}
		return nil, err
	}

	s.currentRecID = recID
	return rec, nil
}

/*
GetNextRecord returns the next active record of the source DB table.
If iteration has not started yet, it behaves like GetFirstRecord.
*/
func (s *SourceDBTableSet) GetNextRecord() (map[string]interface{}, error) {
	if s.currentRecID == 0 {
		return s.GetFirstRecord()
	}

	rec, recID, err := record.GetNextRecord(s.tableName, s.currentRecID, s.dbDef)
	if err != nil {
		if errors.Is(err, record.ErrNoMoreRecords) {
			return nil, ErrNoMoreRecords
		}
		return nil, err
	}

	s.currentRecID = recID
	return rec, nil
}

/*
GetRecordIDsByString returns the record IDs of all rows whose STRING field equals the given value.
Parameters:
- fieldName: the name of the STRING field to search
- fieldValue: the exact string value to match
Returns:
- []uint32: the matching record IDs, or an empty slice if no records match
- error: an error if the field cannot be searched or the dictionary lookup fails
*/
func (s *SourceDBTableSet) GetRecordIDsByString(fieldName string, fieldValue string) ([]uint32, error) {
	return record.GetRecordsByString(s.tableName, fieldName, fieldValue, s.dbDef)
}

/*
GetRecordIDsByPrefix returns the record IDs of all rows whose STRING field starts with the given prefix.
Parameters:
- fieldName: the name of the STRING field to search
- prefix: the prefix to match; STEP2 currently supports prefixes up to 8 characters
Returns:
- []uint32: the matching record IDs, or an empty slice if no records match
- error: an error if the field cannot be searched or the prefix lookup fails
*/
func (s *SourceDBTableSet) GetRecordIDsByPrefix(fieldName string, prefix string) ([]uint32, error) {
	return record.GetRecordsBySubstring(s.tableName, fieldName, prefix, s.dbDef)
}

/*
GetRecordIDByPrimaryKey returns the record ID of the row whose primary key equals the given value.
Parameters:
- primaryKey: the value of the table primary key to look up
Returns:
- uint32: the matching record ID
- error: an error if the table has no primary key, the value has the wrong type, or the lookup fails
*/
func (s *SourceDBTableSet) GetRecordIDByPrimaryKey(primaryKey interface{}) (uint32, error) {
	return record.GetRecordID(s.tableName, primaryKey, s.dbDef)
}

// ==================================== DBTableWorkingSet ====================================

/*
DBTableWorkingSet is a DB-backed working set defined by a selected list of record IDs.
The record IDs still refer to records stored in the original source table.
*/
type DBTableWorkingSet struct {
	StateBase
	position  int
	RecordIDs []uint32
}

/*
NewDBTableWorkingSet creates a DB-backed working set for one table and a selected list of record IDs.
Parameters:
- tableName: the name of the underlying STEP2 table
- dbDef: the database definition containing table metadata and file references
- recordIDs: the selected active record IDs belonging to the underlying table
Returns:
- a DBTableWorkingSet initialized for sequential access to the selected records, or an error if the table cannot be resolved
*/
func NewDBTableWorkingSet(tableName string, dbDef *stepdb.DBDefinition, recordIDs []uint32) (*DBTableWorkingSet, error) {
	table, err := lookupTable(tableName, dbDef)
	if err != nil {
		return nil, err
	}

	return &DBTableWorkingSet{
		StateBase: StateBase{
			kind:      StateDBTableWorkingSet,
			tableName: tableName,
			recordDef: buildRecordDefinition(table),
			dbDef:     dbDef,
		},
		position:  -1,
		RecordIDs: recordIDs,
	}, nil
}

func (s *DBTableWorkingSet) Size() int {
	return len(s.RecordIDs)
}

/*
GetFirstRecord returns the first selected record from the working set.
It also updates the current record ID tracked by the state.
*/
func (s *DBTableWorkingSet) GetFirstRecord() (map[string]interface{}, error) {
	if len(s.RecordIDs) == 0 {
		return nil, ErrNoMoreRecords
	}

	s.position = 0
	s.currentRecID = s.RecordIDs[0]
	return record.GetRecordByID(s.tableName, s.currentRecID, s.dbDef)
}

/*
GetNextRecord returns the next selected record from the working set.
If iteration has not started yet, it behaves like GetFirstRecord.
*/
func (s *DBTableWorkingSet) GetNextRecord() (map[string]interface{}, error) {
	if s.position < 0 {
		return s.GetFirstRecord()
	}
	if s.position+1 >= len(s.RecordIDs) {
		return nil, ErrNoMoreRecords
	}

	s.position++
	s.currentRecID = s.RecordIDs[s.position]
	return record.GetRecordByID(s.tableName, s.currentRecID, s.dbDef)
}

// ==================================== ReturnWorkingSet ====================================

/*
ReturnWorkingSet is an in-memory working set with explicit schema and fully materialized rows.
Its logical record IDs are the 1-based positions of the rows in the Records slice.
*/
type ReturnWorkingSet struct {
	StateBase
	name     string
	position int
	Records  []map[string]interface{}
}

/*
NewReturnWorkingSet creates an in-memory working set with explicit schema and materialized rows.
Parameters:
- name: a descriptive name for the working set, typically produced by the builder or stage
- recordDef: the explicit schema of the in-memory rows
- dbDef: the database definition kept with all runtime states for later navigation stages
- records: the fully materialized rows in the working set
Returns:
- a ReturnWorkingSet initialized for sequential access to its records
*/
func NewReturnWorkingSet(name string, recordDef RecordDefinition, dbDef *stepdb.DBDefinition, records []map[string]interface{}) *ReturnWorkingSet {
	return &ReturnWorkingSet{
		StateBase: StateBase{
			kind:      StateReturnWorkingSet,
			tableName: name,
			recordDef: recordDef,
			dbDef:     dbDef,
		},
		name:     name,
		position: -1,
		Records:  records,
	}
}

func (s *ReturnWorkingSet) Size() int {
	return len(s.Records)
}

/*
GetFirstRecord returns the first in-memory row from the working set.
It also updates the current logical record ID tracked by the state.
*/
func (s *ReturnWorkingSet) GetFirstRecord() (map[string]interface{}, error) {
	if len(s.Records) == 0 {
		return nil, ErrNoMoreRecords
	}

	s.position = 0
	s.currentRecID = 1
	return s.Records[0], nil
}

/*
GetNextRecord returns the next in-memory row from the working set.
If iteration has not started yet, it behaves like GetFirstRecord.
*/
func (s *ReturnWorkingSet) GetNextRecord() (map[string]interface{}, error) {
	if s.position < 0 {
		return s.GetFirstRecord()
	}
	if s.position+1 >= len(s.Records) {
		return nil, ErrNoMoreRecords
	}

	s.position++
	s.currentRecID = uint32(s.position + 1)
	return s.Records[s.position], nil
}

func buildRecordDefinition(table *stepdb.TableDescription) RecordDefinition {
	fields := make([]*FieldDef, 0, len(table.RecordLayout.Fields))
	fieldIndex := make(map[string]int, len(table.RecordLayout.FieldIndex))
	qualifiedFieldIndex := make(map[string]int, len(table.RecordLayout.FieldIndex))

	for index, field := range table.RecordLayout.Fields {
		fields = append(fields, &FieldDef{
			Name:            field.Name,
			SourceTableName: table.Name,
			SourceFieldName: field.Name,
			Type:            field.Type,
			IsForeignKey:    field.IsForeignKey,
			ForeignKeyTable: field.ForeignKeyTable,
		})
		fieldIndex[field.Name] = index
		qualifiedFieldIndex[QualifiedFieldKey(table.Name, field.Name)] = index
	}

	return RecordDefinition{
		NoFields:            table.RecordLayout.NoFields,
		PrimaryKey:          table.RecordLayout.PrimaryKey,
		Fields:              fields,
		FieldIndex:          fieldIndex,
		QualifiedFieldIndex: qualifiedFieldIndex,
	}
}

func countActiveTableRecords(tableName string, dbDef *stepdb.DBDefinition) (int, error) {
	table, err := lookupTable(tableName, dbDef)
	if err != nil {
		return 0, err
	}

	// Record file header layout:
	// TableNo(2) + RecordLength(2) + LastRecordID(4) + FirstDeletedID(4)
	headerBuf := make([]byte, 12)
	_, err = table.RecordFile.ReadAt(headerBuf, 0)
	if err != nil {
		return 0, fmt.Errorf("failed to read record file header for table %s: %w", tableName, err)
	}

	allocatedRecords := int(binary.LittleEndian.Uint32(headerBuf[4:8]))
	firstDeletedID := binary.LittleEndian.Uint32(headerBuf[8:12])
	recordLength := int64(binary.LittleEndian.Uint16(headerBuf[2:4]))

	const noFirstDeletedRecord uint32 = 0xFFFF
	const noNextDeletedRecord uint32 = 0xFFFF

	deletedListLength := 0
	for currentID := firstDeletedID; currentID != noFirstDeletedRecord && currentID != noNextDeletedRecord; {
		deletedListLength++

		// Deleted records store NextDeletedID in bytes 1-5 of the record.
		recordHeaderBuf := make([]byte, 5)
		recordOffset := int64(12) + int64(currentID)*recordLength
		_, err = table.RecordFile.ReadAt(recordHeaderBuf, recordOffset)
		if err != nil {
			return 0, fmt.Errorf("failed to read deleted record %d from table %s: %w", currentID, tableName, err)
		}

		currentID = binary.LittleEndian.Uint32(recordHeaderBuf[1:5])
	}

	return allocatedRecords - deletedListLength, nil
}
