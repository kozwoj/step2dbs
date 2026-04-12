package db

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"github.com/kozwoj/indexing/primindex"
	"github.com/kozwoj/step2/step2DDLparser"
	"time"
)

// Custom error variables for DB definition operations
var (
	ErrDBFailedToReadSchema   = errors.New("failed to read schema file")
	ErrDBSchemaNameEmpty      = errors.New("schema name is empty")
	ErrDBPathNotDirectory     = errors.New("DB path exists but is not a directory")
	ErrDBDirFailedToRead      = errors.New("failed to read DB directory")
	ErrDBDirNotEmpty          = errors.New("DB directory is not empty")
	ErrDBSchemaNoTables       = errors.New("schema must contain at least one table")
	ErrDBSchemaDuplicateTable = errors.New("duplicate table name found in schema")
)

/*
DBDefinition is a structure describing DB storage objects (records and files). STEP2 creates an instance of this structure based on the parsed schema
when a new DB is created, and then serializes it to schema.json file stored in the DB root directory.
When the DB is subsequently opened, the schema.json file is deserialized and a singleton instance of the structure is created in the globals package.

When the DB created all DB files are created. When the DB is opened, file references for all DB files are initialized and stored in the structure. References
to dictionaries files are stored in the Dictionary object in the FieldDescription struct for STRING type fields.
*/

type DBDefinition struct {
	Name       string    // the same as the schema name in the DDL
	DirPath    string    // path to the DB root directory
	CreatedOn  time.Time // timestamp when the DB was created
	SchemaFile *os.File  // schema.json file ref created when the DB is opened
	Tables     []*TableDescription
	TableIndex map[string]int // maps table names to index in DB.Tables slice
}

type TableDescription struct {
	Name         string           // table name from the DDL schema
	Key          int              // primary key field number if the table has a primary key, -1 otherwise
	RecordFile   *os.File         // file reference for the table's records file, initialized when the DB is opened
	PrimeIndex   *primindex.Index // reference to table's primary index, including primindx.dat file initialized when the DB is opened (nil if no primary key)
	RecordLayout RecordDescription
	Sets         []*SetDescription
	SetIndex     map[string]int // maps set names to index in TableDescription.Sets slice
}

type RecordDescription struct {
	HeaderSize int
	DataSize   int
	NoFields   int
	PrimaryKey int // primary key field number in the RecordDescription.Fields slice, -1 if the table has no primary key
	Fields     []*FieldDescription
	FieldIndex map[string]int // maps field names to index in RecordDescription.Fields slice
}

// ========== FIELD TYPES SET BY PARSER & USED BY DB ==========

type FieldType int

const (
	SMALLINT FieldType = 1 + iota
	INT
	BIGINT
	DECIMAL
	FLOAT
	STRING
	CHAR
	BOOLEAN
	DATE
	TIME
)

type FieldDescription struct {
	Name            string
	Type            FieldType              // field type code (1 - SMALLINT, 2 - INT, 3 - BIGINT, 4 - DECIMAL, 5 - FLOAT, 6 - STRING, 7 - CHAR, 8 - BOOLEAN, 9 - DATE, 10 - TIME)
	IsForeignKey    bool                   // whether the field has a foreign key constraint
	ForeignKeyTable string                 // if IsForeignKey == true, the name of the referenced table
	IsOptional      bool                   // whether the field has an optional constraint
	Dictionary      *dictionary.Dictionary // pointer to the field's dictionary, initialized when the DB is opened for STRING type fields, nil otherwise
	Offset          int
	Size            int // serialization size in bytes (for STRING this is 4 bytes for uint32 ID, for CHAR this is the character count)
	StringSizeLimit int // for STRING type only: size limit from DDL (0 means unlimited), for other types this is 0
}

type SetDescription struct {
	Name            string
	MemberTableName string   // name of the table that stores set member records
	MembersFile     *os.File // file reference for the set's members file, initialized when the DB is opened
}

/*
NewDBDefinitionFromSchema creates a new DBDefinition instance based on the provided schema file, performing
all schema parsing and validation checks. This function does not perform any directory operations or set DirPath.

Input parameters:
- schemaFile: a string representing the path to the DDL schema file

Returns:
- *DBDefinition: a pointer to the created DBDefinition instance (with DirPath left empty)
- error: an error object if any error occurs during the creation process, nil otherwise

note:
If parsing fails the function returns the parsing error. If parsing succeeds but the schema fails consistency checks the function returns an error
describing the first consistency issue found.

The function performs the following consistency checks:

Schema-level checks:
1. Schema name must not be empty.
2. Schema must contain at least one table.
3. All table names must be unique.

Table-level checks:
4. Each table must contain at least one field.
5. All field names within a table must be unique.
6. All set names within a table must be unique.
7. Each table must have at most one field marked as primary key.

Field-level checks:
8. If a field has a primary key constraint, it must be one of the allowed types:
   - SMALLINT, INT, BIGINT, or
   - CHAR with size between 4 and 32 characters (CHAR[4-32])
9. If a field is of type CHAR, it must have a size specified (Size > 0).
10. If a field has a foreign key constraint, the referenced table must exist in the schema.
11. If a field has a foreign key constraint, the referenced field must exist in the referenced table, must
be of the same type as the referencing field, and must be the primary key of the referenced table.

Set-level checks:
12. For each set in a table, the member table name must correspond to an existing table in the DBDefinition.Tables slice.

*/

func NewDBDefinitionFromSchema(schemaFile string) (*DBDefinition, error) {
	// Read and parse the schema file
	data, err := os.ReadFile(schemaFile)
	if err != nil {
		return nil, ErrDBFailedToReadSchema
	}

	input := string(data)
	schema, err := step2DDLparser.ParseSchema(input)
	if err != nil {
		return nil, err
	}

	// Verify schema name is not empty, schema contains at least one table, and build table index map
	if schema.Name == "" {
		return nil, ErrDBSchemaNameEmpty
	}
	if len(schema.Tables) == 0 {
		return nil, ErrDBSchemaNoTables
	}
	tableNames := make(map[string]bool)
	for _, table := range schema.Tables {
		if _, exists := tableNames[table.Name]; exists {
			return nil, ErrDBSchemaDuplicateTable
		}
		tableNames[table.Name] = true
	}

	// Create DBDefinition with schema name (DirPath will be set later by caller if needed)
	dbDef := &DBDefinition{
		Name:       schema.Name,
		DirPath:    "", // Leave empty - caller can set this if needed
		CreatedOn:  time.Now(),
		SchemaFile: nil,
		Tables:     make([]*TableDescription, 0),
		TableIndex: make(map[string]int),
	}
	// Process each table and perform table-level checks
	for i, table := range schema.Tables {
		// initialize TableDescriptions with table name and empty sets slice and set index map
		tableDesc := &TableDescription{
			Name:         table.Name,
			Key:          -1, // default to -1 for no primary key, will update if we find a primary key field
			RecordFile:   nil,
			PrimeIndex:   nil,
			RecordLayout: RecordDescription{},
			Sets:         make([]*SetDescription, 0),
			SetIndex:     make(map[string]int),
		}
		//see if table name is already in the table index map
		if _, exists := dbDef.TableIndex[table.Name]; exists {
			return nil, ErrDBSchemaDuplicateTable
		}
		// add table name and index to the table index map
		dbDef.TableIndex[table.Name] = i

		// process fields and build record layout, perform field-level checks, and set primary key field number in table description if applicable
		recordLayout, err := processTableFields(table)
		if err != nil {
			return nil, err
		}
		tableDesc.RecordLayout = recordLayout
		// Set the table's primary key field number from the record layout
		tableDesc.Key = recordLayout.PrimaryKey

		// process sets and perform set-level checks
		setNames := make(map[string]bool)
		for i, set := range table.Sets {
			if _, exists := setNames[set.Name]; exists {
				return nil, fmt.Errorf("duplicate set name %s found in table %s", set.Name, table.Name)
			}
			setNames[set.Name] = true
			setDesc := &SetDescription{
				Name:            set.Name,
				MemberTableName: set.TableName,
			}
			tableDesc.Sets = append(tableDesc.Sets, setDesc)
			tableDesc.SetIndex[set.Name] = i
		}

		// add fully constructed table description to DB definition's tables slice
		dbDef.Tables = append(dbDef.Tables, tableDesc)
	}

	// check foreign key constraints
	for _, table := range dbDef.Tables {
		for _, field := range table.RecordLayout.Fields {
			if field.IsForeignKey {
				// check referenced table exists
				_, exists := dbDef.TableIndex[field.ForeignKeyTable]
				if !exists {
					return nil, fmt.Errorf("foreign key %s in table %s references non-existent table %s", field.Name, table.Name, field.ForeignKeyTable)
				}
				// check referenced field exists, is primary key, and is of the same type as the referencing field
				refTable := dbDef.Tables[dbDef.TableIndex[field.ForeignKeyTable]]
				if refTable.Key == -1 {
					return nil, fmt.Errorf("foreign key %s in table %s references table %s that has no primary key", field.Name, table.Name, field.ForeignKeyTable)
				}
				refField := refTable.RecordLayout.Fields[refTable.Key]
				if refField.Type != field.Type {
					return nil, fmt.Errorf("foreign key %s in table %s references field %s in table %s that is of a different type", field.Name, table.Name, refField.Name, field.ForeignKeyTable)
				}
				// for CHAR types, also check that sizes match
				if refField.Type == CHAR && refField.Size != field.Size {
					return nil, fmt.Errorf("foreign key %s (CHAR[%d]) in table %s references field %s (CHAR[%d]) in table %s with different size", field.Name, field.Size, table.Name, refField.Name, refField.Size, field.ForeignKeyTable)
				}
			}
		}
	}

	// check set constraints
	for _, table := range dbDef.Tables {
		for _, set := range table.Sets {
			// check member table exists
			_, exists := dbDef.TableIndex[set.MemberTableName]
			if !exists {
				return nil, fmt.Errorf("set %s in table %s references non-existent member table %s", set.Name, table.Name, set.MemberTableName)
			}
		}
	}

	return dbDef, nil
}

/*
CreateDBDefinition creates a new DBDefinition instance based on the provided schema and DB directory path. If the schema parses with no errors,
the function performs additional schema consistency checks described in the note below and returns a pointer to the instance.

This function calls NewDBDefinitionFromSchema to create and validate the DBDefinition, then sets up the DB directory and updates the DirPath.

Input parameters:
- schemaFile: a string representing the path to the DDL schema file
- dbDir: a string representing the path to the parent directory where the DB root directory will be created.
         The DB root directory will be named after the schema name.

Returns:
- *DBDefinition: a pointer to the created DBDefinition instance
- error: an error object if any error occurs during the creation process, nil otherwise

note:
If parsing fails the function returns the parsing error. If parsing succeeds but the schema fails consistency checks the function returns an error
describing the first consistency issue found.

The function performs the same consistency checks as NewDBDefinitionFromSchema, plus directory validation and creation.

*/

func CreateDBDefinition(schemaFile string, dbDir string) (*DBDefinition, error) {
	// Create and validate the DBDefinition from the schema
	dbDef, err := NewDBDefinitionFromSchema(schemaFile)
	if err != nil {
		return nil, err
	}

	// Append schema name to dbDir to create the full DB root directory path
	dbDir = filepath.Join(dbDir, dbDef.Name)

	// Check if dbDir exists and isempty. If it does not exist, create it.
	info, err := os.Stat(dbDir)
	if os.IsNotExist(err) {
		// create the directory
		err = os.Mkdir(dbDir, 0755)
		if err != nil {
			return nil, fmt.Errorf("failed to create DB directory: %w", err)
		}
	} else {
		if !info.IsDir() {
			return nil, ErrDBPathNotDirectory
		}
		// check if directory is empty
		entries, err := os.ReadDir(dbDir)
		if err != nil {
			return nil, ErrDBDirFailedToRead
		}
		if len(entries) > 0 {
			return nil, ErrDBDirNotEmpty
		}
	}

	// Set the DirPath on the DBDefinition
	dbDef.DirPath = dbDir

	return dbDef, nil
}

/*
processTableFields processes the fields of a table, performs field-level consistency checks, calculates field offsets and sizes,
and builds the RecordDescription for the table. It returns the RecordDescription and any error encountered during processing.
Input parameters:
- table: a step2DDLparser.Table struct representing the parsed table from the DDL schema
Returns:
- RecordDescription: the constructed RecordDescription for the table
- error: an error object if any consistency check fails, nil otherwise

note: cross table constraints are checked after all tables are processed, so this function only performs field-level checks
*/
func processTableFields(table step2DDLparser.Table) (RecordDescription, error) {
	if len(table.Columns) == 0 {
		return RecordDescription{}, fmt.Errorf("table %s must contain at least one field", table.Name)
	}
	recordLayout := RecordDescription{
		HeaderSize: 1 + 4 + 4*len(table.Sets), // 1 byte for deleted flag, 4 bytes for next deleted record pointer, 4 bytes per set for block numbers
		DataSize:   0,
		NoFields:   len(table.Columns),
		PrimaryKey: -1,
		Fields:     make([]*FieldDescription, len(table.Columns)),
		FieldIndex: make(map[string]int),
	}
	fieldNames := make(map[string]bool)
	offset := recordLayout.HeaderSize + 1 // start offset at header size + 1 byte for HasValue flag

	for i, field := range table.Columns {
		if _, exists := fieldNames[field.Name]; exists {
			return RecordDescription{}, fmt.Errorf("duplicate field name %s found in table %s", field.Name, table.Name)
		}
		fieldNames[field.Name] = true
		fieldDesc := &FieldDescription{
			Name:            field.Name,
			Type:            FieldType(field.Type),
			IsForeignKey:    false,
			ForeignKeyTable: "",
			Dictionary:      nil,
			Offset:          offset,
			Size:            0,
		}

		switch fieldDesc.Type {
		case SMALLINT:
			fieldDesc.Size = 2
			offset += 2 + 1 // field size + 1 byte for HasValue flag
		case INT:
			fieldDesc.Size = 4
			offset += 4 + 1 // field size + 1 byte for HasValue flag
		case BIGINT:
			fieldDesc.Size = 8
			offset += 8 + 1 // field size + 1 byte for HasValue flag
		case DECIMAL:
			fieldDesc.Size = 19 // 8 bytes for integer part + 8 bytes for fractional part + 2 bytes for scale + 1 byte for sign
			offset += 19 + 1    // field size + 1 byte for HasValue flag
		case FLOAT:
			fieldDesc.Size = 8
			offset += 8 + 1 // field size + 1 byte for HasValue flag
		case STRING:
			fieldDesc.Size = 4                          // uint32 string ID
			fieldDesc.StringSizeLimit = field.SizeLimit // 0 means unlimited
			offset += 4 + 1                             // field size + 1 byte for HasValue flag
		case CHAR:
			fieldDesc.Size = field.SizeLimit // size specified in field definition
			if fieldDesc.Size <= 0 {
				return RecordDescription{}, fmt.Errorf("field %s in table %s is of type CHAR but has invalid size %d", field.Name, table.Name, fieldDesc.Size)
			}
			offset += fieldDesc.Size + 1 // field size + 1 byte for HasValue flag
		case BOOLEAN:
			fieldDesc.Size = 1
			offset += 1 + 1 // field size + 1 byte for HasValue flag
		case DATE:
			fieldDesc.Size = 8
			offset += 8 + 1 // field size + 1 byte for HasValue flag
		case TIME:
			fieldDesc.Size = 8
			offset += 8 + 1 // field size + 1 byte for HasValue flag
		default:
			return RecordDescription{}, fmt.Errorf("field %s in table %s has invalid type code %d", field.Name, table.Name, field.Type)
		}

		// Process constraints if any exist
		for _, constraint := range field.Constraints {
			// check for primary key constraint
			if constraint.IsPrimaryKey {
				if recordLayout.PrimaryKey != -1 {
					return RecordDescription{}, fmt.Errorf("multiple primary key fields found in table %s", table.Name)
				}
				// check key type is allowed
				if fieldDesc.Type != SMALLINT && fieldDesc.Type != INT && fieldDesc.Type != BIGINT &&
					!(fieldDesc.Type == CHAR && fieldDesc.Size >= 4 && fieldDesc.Size <= 32) {
					return RecordDescription{}, fmt.Errorf("field %s in table %s has primary key constraint but invalid type", field.Name, table.Name)
				}
				recordLayout.PrimaryKey = i
			}
			// check for foreign key constraint
			if constraint.IsForeignKey {
				fieldDesc.IsForeignKey = true
				fieldDesc.ForeignKeyTable = constraint.TableName
			}
			// check for optional constraint
			if constraint.IsOptional {
				fieldDesc.IsOptional = true
			}
		}

		// add field to field index and field description slice
		recordLayout.FieldIndex[field.Name] = i
		recordLayout.Fields[i] = fieldDesc
	}

	// Calculate DataSize: offset includes HeaderSize and and one extra byte, so subtract both
	recordLayout.DataSize = offset - recordLayout.HeaderSize - 1

	return recordLayout, nil
}

/*
SaveDefinitionAsJson saves a DB definition as a JSON object in a file defined by filePath.
Parameters:
- db: *DBDefinition object to save
- filePath: path to the file where the JSON object will be saved
Returns:
- error if any
*/
func SaveDefinitionAsJson(db *DBDefinition, filePath string) error {
	// marshal DB as json object
	dbj, err := json.Marshal(&db)
	if err != nil {
		return err
	}
	// write json to file. !! override the file if it exists, otherwise	create a new file
	err = os.WriteFile(filePath, dbj, 0644)
	if err != nil {
		return err
	}
	return nil
}

/*
LoadDefinitionFromJson loads a DBDefinition object from JSON object stored in a file defined by the path.
Parameters:
- path: string - the file path to load the JSON object from
- dbd: *DBDefinition - pointer to the DBDefinition object to unmarshal the JSON into
Returns:
- error if any
*/
func LoadDefinitionFromJson(path string, dbd *DBDefinition) error {
	// read JSON object from file
	dbj, er1 := os.ReadFile(path)
	if er1 != nil {
		return er1
	}
	// unmarshal json object into DB
	er2 := json.Unmarshal(dbj, dbd)
	if er2 != nil {
		return er2
	}
	return nil
}
