package pipeline

import stepdb "github.com/kozwoj/step2/db"

/*
FieldType is the pipeline-level field type shared by builder and engine metadata.
It mirrors STEP2 field types.
*/
type FieldType = stepdb.FieldType

const (
	SMALLINT FieldType = stepdb.SMALLINT
	INT      FieldType = stepdb.INT
	BIGINT   FieldType = stepdb.BIGINT
	DECIMAL  FieldType = stepdb.DECIMAL
	FLOAT    FieldType = stepdb.FLOAT
	STRING   FieldType = stepdb.STRING
	CHAR     FieldType = stepdb.CHAR
	BOOLEAN  FieldType = stepdb.BOOLEAN
	DATE     FieldType = stepdb.DATE
	TIME     FieldType = stepdb.TIME
)

/*
FieldDef is a simplified field description used by the query pipeline model.
It contains only the metadata needed by stage definitions and runtime interpretation.
*/
type FieldDef struct {
	Name            string
	SourceTableName string
	SourceFieldName string
	Type            FieldType
	IsForeignKey    bool
	ForeignKeyTable string
}

func (f *FieldDef) QualifiedName() string {
	if f == nil {
		return ""
	}
	return QualifiedFieldKey(f.SourceTableName, f.SourceFieldName)
}

/*
RecordDefinition is the simplified schema description shared by pipeline states.
It is derived either from a STEP2 table description or from a return clause.
*/
type RecordDefinition struct {
	NoFields            int
	PrimaryKey          int
	Fields              []*FieldDef
	FieldIndex          map[string]int
	QualifiedFieldIndex map[string]int
}

func QualifiedFieldKey(tableName string, fieldName string) string {
	if tableName == "" {
		return fieldName
	}
	return tableName + "." + fieldName
}

func (r RecordDefinition) LookupFieldPosition(tableName string, fieldName string) (int, bool) {
	if r.QualifiedFieldIndex != nil {
		position, exists := r.QualifiedFieldIndex[QualifiedFieldKey(tableName, fieldName)]
		if exists {
			return position, true
		}
	}

	position, exists := r.FieldIndex[fieldName]
	if !exists {
		return 0, false
	}
	if position < 0 || position >= len(r.Fields) {
		return 0, false
	}

	fieldDef := r.Fields[position]
	if fieldDef == nil {
		return 0, false
	}
	if tableName != "" && fieldDef.SourceTableName != "" && fieldDef.SourceTableName != tableName {
		return 0, false
	}
	if fieldDef.SourceFieldName != "" && fieldDef.SourceFieldName != fieldName {
		return 0, false
	}

	return position, true
}

func (r RecordDefinition) LookupField(tableName string, fieldName string) (*FieldDef, int, bool) {
	position, exists := r.LookupFieldPosition(tableName, fieldName)
	if !exists {
		return nil, 0, false
	}
	return r.Fields[position], position, true
}