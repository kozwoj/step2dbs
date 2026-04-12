package db

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"github.com/kozwoj/indexing/primindex"
)

// Dictionary block size constants optimized for different data structures
const (
	// Postings blocks: smaller size reduces waste for sparse record ID lists
	DefaultPostingsBlockSize = 128

	// Index blocks: larger size for B-tree indexes reduces tree depth and improves performance
	DefaultIndexBlockSize = 1024

	// Prefix blocks: larger size for B-tree indexes (matches primary index size)
	DefaultPrefixBlockSize = 1024

	// Initial block allocation: minimum is 3 (for dicindex128), but 10 provides headroom
	DefaultInitialBlocks = 10
)

// DefaultDictionaryBlockSizes provides the recommended block sizes for dictionary files
var DefaultDictionaryBlockSizes = dictionary.DictionaryBlockSizes{
	PostingsBlockSize: DefaultPostingsBlockSize,
	IndexBlockSize:    DefaultIndexBlockSize,
	PrefixBlockSize:   DefaultPrefixBlockSize,
}

/*
CreateDB creates a new database in a given directory, based on the provided DDL schema file,
and the name equal to the schema name.
Parameters:
- dbDirPath: the directory where the database root directory and subdirectories will be created
- schemaFilePath: the path to the DDL schema file that defines the database structure
Returns:
- error: an error object if any issues occur during database creation, or nil if the operation is successful

note: all information about the database structure necessary for creating the database
is contained in the DDL schema file.
*/
func CreateDB(dbDirPath string, schemaFilePath string) error {
	dbDefinition, err := CreateDBDefinition(schemaFilePath, dbDirPath)
	if err != nil {
		return err
	}
	// the dbRootDirPath directory is created by CreateDBDefinition, and it should be empty at this point.
	dbRootDirPath := filepath.Join(dbDirPath, dbDefinition.Name)
	err = CreateDirectoryTree(dbDefinition, dbRootDirPath)
	if err != nil {
		return err
	}
	// create files and directories for dictionaries and indexes
	err = CreateFilesAndDictionaries(dbDefinition, dbRootDirPath)
	if err != nil {
		return err
	}
	// save the DB definition to a file in the DB root directory as schema.json
	err = SaveDefinitionAsJson(dbDefinition, filepath.Join(dbRootDirPath, "schema.json"))
	if err != nil {
		return err
	}
	return nil
}

/*
CreateDirectoryTree creates the directory tree for the database based on the provided DBDefinition.
Parameters:
- dbDefinition: a pointer to the DBDefinition instance that contains the database structure information
- dbRootDirPath: the root directory path
Returns:
- error: an error object if any issues occur during directory tree creation, or nil if the operation is successful

note: CreateDBDefinition has created the root directory for the database, and
it should be empty at this point. CreateDirectoryTree will create the necessary subdirectories.
*/
func CreateDirectoryTree(dbDefinition *DBDefinition, dbRootDirPath string) error {
	for _, table := range dbDefinition.Tables {
		tableDirPath := filepath.Join(dbRootDirPath, table.Name)
		err := os.Mkdir(tableDirPath, 0755)
		if err != nil {
			return err
		}
	}
	return nil
}

/*
CreateFilesAndDictionaries function creates files and directories in the DB directory tree
Parameters:
- dbDefinition: a pointer to the DBDefinition instance that contains the database structure information
- dbRootDirPath: the root directory path
Returns:
- error: an error object if any issues occur during file and dictionary creation, or nil if the operation is successful
*/
func CreateFilesAndDictionaries(dbDefinition *DBDefinition, dbRootDirPath string) error {
	// Iterate through each table
	for tableNo, table := range dbDefinition.Tables {
		tableDirPath := filepath.Join(dbRootDirPath, table.Name)

		// 1. Create records file
		recordLength := uint16(table.RecordLayout.HeaderSize + table.RecordLayout.DataSize)
		err := createRecordsFile(tableDirPath, uint16(tableNo), recordLength)
		if err != nil {
			return fmt.Errorf("failed to create records file for table %s: %w", table.Name, err)
		}

		// 2. Create primary index file if table has a primary key
		if table.Key != -1 {
			// Get the primary key field
			pkField := table.RecordLayout.Fields[table.Key]

			// Determine the keyType for primindex based on field type and size
			var keyType primindex.KeyType
			switch pkField.Type {
			case SMALLINT:
				keyType = primindex.KeyTypeSMALLINT
			case INT:
				keyType = primindex.KeyTypeINT
			case BIGINT:
				keyType = primindex.KeyTypeBIGINT
			case CHAR:
				// Map CHAR[N] size to KeyTypeBytesN
				keyType, err = getKeyTypeForCharSize(pkField.Size)
				if err != nil {
					return fmt.Errorf("failed to determine key type for table %s primary key: %w", table.Name, err)
				}
			default:
				return fmt.Errorf("invalid primary key type %d for table %s", pkField.Type, table.Name)
			}

			// Create the primary index file
			err = primindex.CreateIndexFile(tableDirPath, "primindex.dat", 1024, 10, keyType, 4)
			if err != nil {
				return fmt.Errorf("failed to create primary index file for table %s: %w", table.Name, err)
			}
		}

		// 3. Create set member files
		for _, set := range table.Sets {
			setFilePath := filepath.Join(tableDirPath, set.Name+".dat")
			err = CreateSetFile(setFilePath, DefaultSetBlockSize, DefaultSetInitialSize)
			if err != nil {
				return fmt.Errorf("failed to create set file %s for table %s: %w", set.Name, table.Name, err)
			}
		}

		// 4. Create dictionary subdirectories and files for STRING fields
		for _, field := range table.RecordLayout.Fields {
			if field.Type == STRING {
				// Create dictionary in a subdirectory named after the field
				dictDirPath := filepath.Join(tableDirPath, field.Name)
				dict, err := dictionary.CreateDictionary(dictDirPath, field.Name, DefaultDictionaryBlockSizes, DefaultInitialBlocks)
				if err != nil {
					return fmt.Errorf("failed to create dictionary for field %s in table %s: %w", field.Name, table.Name, err)
				}
				// Close the dictionary immediately after creation
				dict.Close()
			}
		}
	}

	return nil
}

// getKeyTypeForCharSize returns the appropriate primindex.KeyType for a CHAR field of the given size
func getKeyTypeForCharSize(size int) (primindex.KeyType, error) {
	switch size {
	case 4:
		return primindex.KeyTypeBytes4, nil
	case 5:
		return primindex.KeyTypeBytes5, nil
	case 6:
		return primindex.KeyTypeBytes6, nil
	case 7:
		return primindex.KeyTypeBytes7, nil
	case 8:
		return primindex.KeyTypeBytes8, nil
	case 9:
		return primindex.KeyTypeBytes9, nil
	case 10:
		return primindex.KeyTypeBytes10, nil
	case 11:
		return primindex.KeyTypeBytes11, nil
	case 12:
		return primindex.KeyTypeBytes12, nil
	case 13:
		return primindex.KeyTypeBytes13, nil
	case 14:
		return primindex.KeyTypeBytes14, nil
	case 15:
		return primindex.KeyTypeBytes15, nil
	case 16:
		return primindex.KeyTypeBytes16, nil
	case 17:
		return primindex.KeyTypeBytes17, nil
	case 18:
		return primindex.KeyTypeBytes18, nil
	case 19:
		return primindex.KeyTypeBytes19, nil
	case 20:
		return primindex.KeyTypeBytes20, nil
	case 21:
		return primindex.KeyTypeBytes21, nil
	case 22:
		return primindex.KeyTypeBytes22, nil
	case 23:
		return primindex.KeyTypeBytes23, nil
	case 24:
		return primindex.KeyTypeBytes24, nil
	case 25:
		return primindex.KeyTypeBytes25, nil
	case 26:
		return primindex.KeyTypeBytes26, nil
	case 27:
		return primindex.KeyTypeBytes27, nil
	case 28:
		return primindex.KeyTypeBytes28, nil
	case 29:
		return primindex.KeyTypeBytes29, nil
	case 30:
		return primindex.KeyTypeBytes30, nil
	case 31:
		return primindex.KeyTypeBytes31, nil
	case 32:
		return primindex.KeyTypeBytes32, nil
	default:
		return 0, fmt.Errorf("unsupported CHAR size %d for primary key (must be between 4 and 32)", size)
	}
}

// createRecordsFile creates a new records.dat file with an initial header
// This is inlined here to avoid circular import with the record package
func createRecordsFile(dirPath string, tableNo uint16, recordLength uint16) error {
	const NoFirstRecord uint32 = 0xFFFF // Sentinel value indicating no deleted records

	filePath := filepath.Join(dirPath, "records.dat")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the record file header (12 bytes):
	// - 2 bytes: table number
	// - 2 bytes: record length
	// - 4 bytes: last record ID (initially 0)
	// - 4 bytes: first deleted ID (initially NoFirstRecord)
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint16(buf[0:2], tableNo)
	binary.LittleEndian.PutUint16(buf[2:4], recordLength)
	binary.LittleEndian.PutUint32(buf[4:8], 0)              // LastRecordID
	binary.LittleEndian.PutUint32(buf[8:12], NoFirstRecord) // FirstDeletedID

	_, err = file.WriteAt(buf, 0)
	if err != nil {
		return err
	}

	return nil
}
