package dictionary

import (
	"os"
	"github.com/kozwoj/indexing/dictionary/dicindex128"
	"github.com/kozwoj/indexing/dictionary/postings"
	"github.com/kozwoj/indexing/primindex"
)

// File name constants for dictionary files
const (
	StringsFileName  = "strings.dat"
	OffsetsFileName  = "offsets.dat"
	PostingsFileName = "postings.dat"
	IndexFileName    = "index.dat"
	PrefixFileName   = "prefix.dat"
)

// DictionaryBlockSizes specifies the block sizes for each file type in a dictionary.
// This allows different block sizes optimized for different data structures:
// - Smaller blocks for sparse postings lists (reduces waste)
// - Larger blocks for B-tree indexes (reduces tree depth, improves performance)
type DictionaryBlockSizes struct {
	PostingsBlockSize uint32 // Block size for postings.dat (sparse record ID lists)
	IndexBlockSize    uint16 // Block size for index.dat (dicindex128 B-tree)
	PrefixBlockSize   uint16 // Block size for prefix.dat (primindex B-tree)
}

/*
Dictionary stores string property values of a collection of records. In most cases one dictionary will be associated with
one property of those records. One dictionary can be also associated with multiple properties of the records, but in this
cases postings become ambiguous and should not be stored - the dictionary can be used to restore property values, but not
record for search.

Dictionary supports the following operations:
- CreateDictionary - create a new dictionary with default postings format (slice-based)
- CreateDictionaryWithFormat - create a new dictionary with specified postings format (slice or bitmap)
- OpenDictionary - open an existing dictionary from the given directory
- Close - close the dictionary and its files
- AddString - adds a new string to the dictionary and returns dictID and postingsRef
- FindString - given a string returns dictID and postingsRef
- RetrievePostings - retrieves list of recordIDs (postings) for a given postingsRef
- AddRecordID - adds recordID to the postings in the postingsRef
- RemoveRecordID - removes recordID from the postings in the postingsRef
- GetsStringByID - retrieves the string for the given dictID
- PrefixSearch - searches for all strings starting with a given prefix

Dictionary is implemented as a set of files stored in one directory and named after the directory name. The files are
- file with raw strings 	- strings.dat
- file with string offsets 	- offsets.dat
- file with postings 		- postings.dat
- dictionary index file 	- index.dat
- prefix index file 		- prefix.dat

Dictionary is created by CreateDictionary function which initializes all files and data structures.

Dictionary is opened by OpenDictionary function which returns the Dictionary object that implements the operations above.

*/

type Dictionary struct {
	DirPath      string             // directory path where dictionary files are stored
	Name         string             // dictionary name == the last part of DirPath
	Index        *dicindex128.Index // dictionary index
	PostingsFile *os.File
	PostingsList postings.PostingsList // postings list interface for postings implementation (slice or bitmap)
	StringsFile  *os.File
	OffsetsFile  *os.File
	PrefixIndex  *primindex.Index // prefix index for prefix search
}

/*
CreateDictionary creates a new dictionary in the given directory with the default postings format (slice-based).
Parameters:
- dirPath: string path to the directory where dictionary files will be created
- name: string name of the dictionary (used for file naming)
- blockSizes: DictionaryBlockSizes struct specifying block sizes for each file type
- initialBlocks: uint32 initial number of blocks to allocate for all files
Returns:
- *Dictionary: pointer to the created Dictionary object
- error: if any
*/
func CreateDictionary(dirPath string, name string, blockSizes DictionaryBlockSizes, initialBlocks uint32) (*Dictionary, error) {
	return CreateDictionaryWithFormat(dirPath, name, blockSizes, initialBlocks, postings.FormatSlice)
}

/*
CreateDictionaryWithFormat creates a new dictionary in the given directory with a specified postings format (slice or bitmap).
Parameters:
- dirPath: string path to the directory where dictionary files will be created
- name: string name of the dictionary (used for file naming)
- blockSizes: DictionaryBlockSizes struct specifying block sizes for each file type
- initialBlocks: uint32 initial number of blocks to allocate for all files
- format: postings.PostingsFormat format to use (FormatSlice or FormatBitmap)
Returns:
- *Dictionary: pointer to the created Dictionary object
- error: if any
*/
func CreateDictionaryWithFormat(dirPath string, name string, blockSizes DictionaryBlockSizes, initialBlocks uint32, format postings.PostingsFormat) (*Dictionary, error) {
	// Create directory if it doesn't exist
	err := os.MkdirAll(dirPath, 0755)
	if err != nil {
		return nil, err
	}

	// Create file paths
	stringsPath := dirPath + "/" + StringsFileName
	offsetsPath := dirPath + "/" + OffsetsFileName
	postingsPath := dirPath + "/" + PostingsFileName

	// Create strings file
	stringsFile, err := CreateStringsFile(stringsPath)
	if err != nil {
		return nil, err
	}

	// Create offsets file
	offsetsFile, err := CreateOffsetsFile(offsetsPath)
	if err != nil {
		stringsFile.Close()
		return nil, err
	}

	// Create postings file with specified format
	postingsFile, err := postings.CreatePostingsFile(postingsPath, blockSizes.PostingsBlockSize, initialBlocks, format)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		return nil, err
	}

	// Create dictionary index
	_, err = dicindex128.CreateDictionaryIndexFile(dirPath, name, blockSizes.IndexBlockSize, uint16(initialBlocks))
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		return nil, err
	}

	// Open the index
	index, err := dicindex128.OpenDictionaryIndex(dirPath, name)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		return nil, err
	}

	// Create prefix index
	err = CreatePrefixIndex(dirPath, blockSizes.PrefixBlockSize, uint16(initialBlocks))
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		index.Close()
		return nil, err
	}

	// Open prefix index
	prefixIndex, err := OpenPrefixIndex(dirPath)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		index.Close()
		return nil, err
	}

	// Read postings file header to get format and create PostingsList
	postingsHeader, err := postings.ReadPostingsFileHeader(postingsFile)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		index.Close()
		prefixIndex.Close()
		return nil, err
	}

	dict := &Dictionary{
		DirPath:      dirPath,
		Name:         name,
		Index:        index,
		PostingsFile: postingsFile,
		PostingsList: postings.NewPostingsList(postingsHeader.Format, postingsHeader.BlockSize),
		StringsFile:  stringsFile,
		OffsetsFile:  offsetsFile,
		PrefixIndex:  prefixIndex,
	}

	return dict, nil
}

/*
OpenDictionary opens an existing dictionary from the given directory.
Parameters:
- dirPath: string path to the directory where dictionary files are stored
- name: string name of the dictionary
Returns:
- *Dictionary: pointer to the opened Dictionary object
- error: if any
*/
func OpenDictionary(dirPath string, name string) (*Dictionary, error) {
	// Create file paths
	stringsPath := dirPath + "/" + StringsFileName
	offsetsPath := dirPath + "/" + OffsetsFileName
	postingsPath := dirPath + "/" + PostingsFileName

	// Open strings file
	stringsFile, _, err := OpenStringsFile(stringsPath)
	if err != nil {
		return nil, err
	}

	// Open offsets file
	offsetsFile, err := OpenOffsetsFile(offsetsPath)
	if err != nil {
		stringsFile.Close()
		return nil, err
	}

	// Open postings file
	postingsFile, postingsHeader, err := postings.OpenPostingsFile(postingsPath)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		return nil, err
	}

	// Open dictionary index
	index, err := dicindex128.OpenDictionaryIndex(dirPath, name)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		return nil, err
	}

	// Open prefix index
	prefixIndex, err := OpenPrefixIndex(dirPath)
	if err != nil {
		stringsFile.Close()
		offsetsFile.Close()
		postingsFile.Close()
		index.Close()
		return nil, err
	}

	dict := &Dictionary{
		DirPath:      dirPath,
		Name:         name,
		Index:        index,
		PostingsFile: postingsFile,
		PostingsList: postings.NewPostingsList(postingsHeader.Format, postingsHeader.BlockSize),
		StringsFile:  stringsFile,
		OffsetsFile:  offsetsFile,
		PrefixIndex:  prefixIndex,
	}

	return dict, nil
}

/*
Close dictionary and its files.
Returns:
- error: if any
*/
func (d *Dictionary) Close() error {
	var errors []error

	if d.StringsFile != nil {
		if err := d.StringsFile.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if d.OffsetsFile != nil {
		if err := d.OffsetsFile.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if d.PostingsFile != nil {
		if err := d.PostingsFile.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if d.Index != nil {
		if err := d.Index.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if d.PrefixIndex != nil {
		if err := d.PrefixIndex.Close(); err != nil {
			errors = append(errors, err)
		}
	}

	if len(errors) > 0 {
		return errors[0]
	}
	return nil
}

/*
AddString adds a new string to the dictionary and returns dictID and postingsRef.
If the string already exists, it returns the existing dictID and postingsRef.
Parameters:
- str: string to add
Returns:
- dictID: uint32 dictionary ID assigned to the string
- postingsRef: uint32 postings reference for the string
- error: if any
*/
func (d *Dictionary) AddString(str string) (uint32, uint32, error) {
	// Calculate hash of the string
	hash := dicindex128.HashString128(str)

	// Check if string already exists in index
	dictID, postingsRef, err := d.Index.Find(hash)
	if err == nil {
		// String found, return existing IDs
		return dictID, postingsRef, nil
	}

	// String not found, add it
	// Get postings file header
	postingsHeader, err := postings.ReadPostingsFileHeader(d.PostingsFile)
	if err != nil {
		return 0, 0, err
	}

	// Add string entry (creates empty postings list)
	dictID, postingsRef, err = AddStringEntry(d.StringsFile, d.OffsetsFile, d.PostingsFile, postingsHeader, str)
	if err != nil {
		return 0, 0, err
	}

	// Add to index
	entry := &dicindex128.IndexEntry128{
		Hash:        hash,
		DictID:      dictID,
		PostingsRef: postingsRef,
	}
	err = d.Index.Insert(entry)
	if err != nil {
		// Check if entry already exists (idempotent behavior)
		if err == dicindex128.ErrIndexEntryAlreadyExists {
			// Entry already exists, retrieve and return existing values
			existingDictID, existingPostingsRef, findErr := d.Index.Find(hash)
			if findErr == nil {
				return existingDictID, existingPostingsRef, nil
			}
			// If Find also fails, return the original Insert error
			return 0, 0, err
		}
		return 0, 0, err
	}

	// Add to prefix index
	err = AddPrefixEntry(d.PrefixIndex, str, dictID)
	if err != nil {
		// Check if entry already exists (idempotent behavior)
		// primindex returns generic error string for duplicates
		if err.Error() == "entry already exists in index" {
			// Entry already exists, which is fine for idempotent operation
			// The string is already in the dictionary index, so we can return success
			return dictID, postingsRef, nil
		}
		return 0, 0, err
	}

	return dictID, postingsRef, nil
}

/*
FindString finds a string in the dictionary and returns dictID and postingsRef.
Parameters:
- str: string to find
Returns:
- dictID: uint32 dictionary ID of the string
- postingsRef: uint32 postings reference for the string
- error: if string not found or other error
*/
func (d *Dictionary) FindString(str string) (uint32, uint32, error) {
	hash := dicindex128.HashString128(str)
	return d.Index.Find(hash)
}

/*
GetStringByID retrieves the string for the given dictID.
Parameters:
- dictID: uint32 dictionary ID
Returns:
- string: the retrieved string
- error: if any
*/
func (d *Dictionary) GetStringByID(dictID uint32) (string, error) {
	return RetrieveStringEntry(d.StringsFile, d.OffsetsFile, dictID)
}

/*
RetrievePostings retrieves the list of recordIDs for the given postingsRef.
Parameters:
- postingsRef: uint32 postings reference (block number)
Returns:
- []uint32: slice of record IDs
- error: if any
*/
func (d *Dictionary) RetrievePostings(postingsRef uint32) ([]uint32, error) {
	recordIDs, _, err := d.PostingsList.GetRecordsList(d.PostingsFile, postingsRef)
	return recordIDs, err
}

/*
AddRecordID adds a recordID to the postings list for the given postingsRef.
This operation is idempotent - if the recordID already exists, it returns nil without error.
Parameters:
- postingsRef: uint32 postings reference (block number)
- recordID: uint32 record ID to add
- dictID: uint32 dictionary ID for integrity checking
Returns:
- error: if any
*/
func (d *Dictionary) AddRecordID(postingsRef uint32, recordID uint32, dictID uint32) error {
	postingsHeader, err := postings.ReadPostingsFileHeader(d.PostingsFile)
	if err != nil {
		return err
	}

	// Use the PostingsList's idempotent AddRecordID method
	return d.PostingsList.AddRecordID(d.PostingsFile, postingsHeader, postingsRef, recordID, dictID)
}

/*
RemoveRecordID removes a recordID from the postings list for the given postingsRef.
This operation is idempotent - if the recordID doesn't exist, it returns nil without error.
This is used when a record is deleted or when a property value changes.
Parameters:
- postingsRef: uint32 postings reference (block number)
- recordID: uint32 record ID to remove
- dictID: uint32 dictionary ID for integrity checking
Returns:
- error: if any
*/
func (d *Dictionary) RemoveRecordID(postingsRef uint32, recordID uint32, dictID uint32) error {
	postingsHeader, err := postings.ReadPostingsFileHeader(d.PostingsFile)
	if err != nil {
		return err
	}

	// Use the PostingsList's idempotent RemoveRecordID method
	return d.PostingsList.RemoveRecordID(d.PostingsFile, postingsHeader, postingsRef, recordID, dictID)
}

/*
PrefixSearch searches for all strings starting with the given prefix.
Returns slice of dictIDs for strings that match the prefix.

Note: Due to 8-byte prefix clustering, results may include false positives
for prefixes longer than 8 bytes. Use PrefixSearchWithStrings() for verified results.

Parameters:
- prefix: string prefix to search for
Returns:
- []uint32: slice of dictIDs for matching strings
- error: if any
*/
func (d *Dictionary) PrefixSearch(prefix string) ([]uint32, error) {
	if d.PrefixIndex == nil {
		return nil, ErrPrefixIndexNil
	}
	return PrefixSearch(d.PrefixIndex, prefix)
}
