package db

import (
	"fmt"
	"os"
	"path/filepath"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"github.com/kozwoj/indexing/primindex"
)

/*
OpenFiles opens all database files for the provided DBDefinition and populates
the file handle fields in the structure.

This includes:
- schema.json file (stored in DBDefinition.SchemaFile)
- records.dat file for each table (stored in TableDescription.RecordFile)
- set member files for each set in each table (stored in SetDescription.MembersFile)

Parameters:
- dbDef: pointer to the DBDefinition structure to populate with file handles

Returns:
- error: if any file fails to open, nil otherwise

Note: If an error occurs, all successfully opened files are closed before returning.
*/
func OpenFiles(dbDef *DBDefinition) error {
	var openedFiles []*os.File

	// Helper function to close all opened files in case of error
	closeAll := func() {
		for _, f := range openedFiles {
			if f != nil {
				f.Close()
			}
		}
	}

	// Open schema.json file
	schemaPath := filepath.Join(dbDef.DirPath, "schema.json")
	schemaFile, err := os.OpenFile(schemaPath, os.O_RDWR, 0644)
	if err != nil {
		return fmt.Errorf("failed to open schema.json: %w", err)
	}
	dbDef.SchemaFile = schemaFile
	openedFiles = append(openedFiles, schemaFile)

	// Open files for each table
	for _, table := range dbDef.Tables {
		tableDirPath := filepath.Join(dbDef.DirPath, table.Name)

		// Open records.dat file
		recordsPath := filepath.Join(tableDirPath, "records.dat")
		recordsFile, err := os.OpenFile(recordsPath, os.O_RDWR, 0644)
		if err != nil {
			closeAll()
			return fmt.Errorf("failed to open records.dat for table %s: %w", table.Name, err)
		}
		table.RecordFile = recordsFile
		openedFiles = append(openedFiles, recordsFile)

		// Open set member files
		for _, set := range table.Sets {
			setFilePath := filepath.Join(tableDirPath, set.Name+".dat")
			setFile, _, err := OpenSetFile(setFilePath)
			if err != nil {
				closeAll()
				return fmt.Errorf("failed to open set file %s for table %s: %w", set.Name, table.Name, err)
			}
			set.MembersFile = setFile
			openedFiles = append(openedFiles, setFile)
		}
	}

	return nil
}

/*
OpenDirectories opens all dictionary directories for STRING fields in the database
and populates the Dictionary pointers in the FieldDescription structures.

Parameters:
- dbDef: pointer to the DBDefinition structure to populate with dictionary handles

Returns:
- error: if any dictionary fails to open, nil otherwise

Note: If an error occurs, all successfully opened dictionaries are closed before returning.
*/
func OpenDirectories(dbDef *DBDefinition) error {
	var openedDicts []*dictionary.Dictionary

	// Helper function to close all opened dictionaries in case of error
	closeAll := func() {
		for _, dict := range openedDicts {
			if dict != nil {
				dict.Close()
			}
		}
	}

	// Open dictionaries for each STRING field in each table
	for _, table := range dbDef.Tables {
		tableDirPath := filepath.Join(dbDef.DirPath, table.Name)

		for _, field := range table.RecordLayout.Fields {
			if field.Type == STRING {
				// Dictionary directory is named after the field
				dictDirPath := filepath.Join(tableDirPath, field.Name)

				dict, err := dictionary.OpenDictionary(dictDirPath, field.Name)
				if err != nil {
					closeAll()
					return fmt.Errorf("failed to open dictionary for field %s in table %s: %w", field.Name, table.Name, err)
				}
				field.Dictionary = dict
				openedDicts = append(openedDicts, dict)
			}
		}
	}

	return nil
}

/*
OpenIndices opens all primary index files for tables that have a primary key
and populates the PrimeIndex pointers in the TableDescription structures.

Parameters:
- dbDef: pointer to the DBDefinition structure to populate with index handles

Returns:
- error: if any index fails to open, nil otherwise

Note: If an error occurs, all successfully opened indices are closed before returning.
*/
func OpenIndices(dbDef *DBDefinition) error {
	var openedIndices []*primindex.Index

	// Helper function to close all opened indices in case of error
	closeAll := func() {
		for _, idx := range openedIndices {
			if idx != nil {
				idx.Close()
			}
		}
	}

	// Open primary index for each table that has a primary key
	for _, table := range dbDef.Tables {
		if table.Key != -1 {
			tableDirPath := filepath.Join(dbDef.DirPath, table.Name)

			index, err := primindex.OpenIndex(tableDirPath, "primindex.dat")
			if err != nil {
				closeAll()
				return fmt.Errorf("failed to open primary index for table %s: %w", table.Name, err)
			}
			table.PrimeIndex = index
			openedIndices = append(openedIndices, index)
		}
	}

	return nil
}

/*
OpenDB opens an existing database from the specified directory path and initializes
the global DBDefinition.

This function:
1. Checks if a database is already open (error if so)
2. Verifies the database directory and schema.json exist
3. Loads the database definition from schema.json
4. Opens all database files (records, sets, schema.json)
5. Opens all dictionary directories for STRING fields
6. Opens all primary index files
7. Initializes the global DBDefinition

Parameters:
- dbDir: string path to the database root directory (where schema.json is located)

Returns:
- error: if any step fails, nil otherwise

Note: If an error occurs, all successfully opened resources are closed before returning.
Use CloseDB() when done with the database to close all resources.
Access the database via the global Definition() function.
*/
func OpenDB(dbDir string) error {
	// Step 0: Check if a database is already open
	if DefinitionInitialized() {
		return fmt.Errorf("a database is already open; close it first with CloseDB()")
	}

	// Step 1: Verify database directory exists
	info, err := os.Stat(dbDir)
	if os.IsNotExist(err) {
		return fmt.Errorf("database directory does not exist: %s", dbDir)
	}
	if !info.IsDir() {
		return fmt.Errorf("path is not a directory: %s", dbDir)
	}

	// Verify schema.json exists
	schemaPath := filepath.Join(dbDir, "schema.json")
	if _, err := os.Stat(schemaPath); os.IsNotExist(err) {
		return fmt.Errorf("schema.json not found in database directory: %s", dbDir)
	}

	// Step 2: Load database definition from schema.json
	var dbDef DBDefinition
	err = LoadDefinitionFromJson(schemaPath, &dbDef)
	if err != nil {
		return fmt.Errorf("failed to load database definition: %w", err)
	}

	// Step 3: Open all database files
	err = OpenFiles(&dbDef)
	if err != nil {
		closeDBHelper(&dbDef)
		return fmt.Errorf("failed to open database files: %w", err)
	}

	// Step 4: Open all dictionary directories
	err = OpenDirectories(&dbDef)
	if err != nil {
		closeDBHelper(&dbDef)
		return fmt.Errorf("failed to open dictionaries: %w", err)
	}

	// Step 5: Open all primary indices
	err = OpenIndices(&dbDef)
	if err != nil {
		closeDBHelper(&dbDef)
		return fmt.Errorf("failed to open indices: %w", err)
	}

	// Step 6: Initialize the global DBDefinition
	err = InitDefinition(&dbDef)
	if err != nil {
		closeDBHelper(&dbDef)
		return fmt.Errorf("failed to initialize global definition: %w", err)
	}

	return nil
}

/*
CloseDB closes all open resources in the global DBDefinition.

This function closes:
- The schema.json file
- All record files
- All set member files
- All dictionary directories
- All primary index files

Note: This function is safe to call even if some resources were not opened.
It will close whatever is not nil. After calling CloseDB, you can open another
database with OpenDB.
*/
func CloseDB() {
	dbDef := Definition()
	if dbDef == nil {
		return
	}

	closeDBHelper(dbDef)

	// Reset the global definition so another database can be opened
	// This should never fail since we checked dbDef != nil above
	_ = ResetDefinition()
}

/*
closeDBHelper is an internal helper function that closes all resources in a DBDefinition.
This is used both by CloseDB (for the global definition) and during OpenDB error cleanup.
*/
func closeDBHelper(dbDef *DBDefinition) {
	if dbDef == nil {
		return
	}

	// Close schema.json file
	if dbDef.SchemaFile != nil {
		dbDef.SchemaFile.Close()
		dbDef.SchemaFile = nil
	}

	// Close table resources
	for _, table := range dbDef.Tables {
		// Close record file
		if table.RecordFile != nil {
			table.RecordFile.Close()
			table.RecordFile = nil
		}

		// Close primary index
		if table.PrimeIndex != nil {
			table.PrimeIndex.Close()
			table.PrimeIndex = nil
		}

		// Close set member files
		for _, set := range table.Sets {
			if set.MembersFile != nil {
				set.MembersFile.Close()
				set.MembersFile = nil
			}
		}

		// Close dictionaries for STRING fields
		for _, field := range table.RecordLayout.Fields {
			if field.Dictionary != nil {
				field.Dictionary.Close()
				field.Dictionary = nil
			}
		}
	}
}
