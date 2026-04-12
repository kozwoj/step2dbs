package dictionary

import (
	"encoding/binary"
	"os"
)

/* Offsets of the stings in the raw-strings file are in the same sequence as the corresponding strings. The file does
not need a header, as the entries are of the same size (12 bytes), and the number of entries is the same as the
number of strings.

The operations of the offsets file are:
- CreateOffsetsFile creates the offsets file when a new dictionary is created
- OpenOffsetsFile opens the offsets file when an existing dictionary is opened
- AddOffsetEntry adds new OffsetEntry when a new string is added to the dictionary
- RetrieveOffsetEntry retrieves OffsetEntry for a given string ID

note:
Since strings are only added to the dictionary and never deleted, new offset entries are always appended.
*/

type OffsetEntry struct {
	StringOffset uint64 // offset of the string in the raw-strings file
	PostingsRef  uint32 // reference to the postings list for this string
}

/*
CreateOffsetsFile creates a new offsets file with the given path. The file will be empty initially.
Parameters:
- filePath: string path to the offsets file to create
Returns:
- *os.File: pointer to the created offsets file
- error: if any
*/
func CreateOffsetsFile(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}

	return file, nil
}

/*
OpenOffsetsFile opens an existing offsets file.
Parameters:
- filePath: string path to the offsets file to open
Returns:
- *os.File: pointer to the opened offsets file
- error: if any
*/
func OpenOffsetsFile(filePath string) (*os.File, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, err
	}
	return file, nil
}

/*
AddOffsetEntry adds a new OffsetEntry to the end of the offsets file.
Parameters:
- file: pointer to the offsets file
- entry: pointer to the OffsetEntry to add
Returns:
- error: if any
*/
func AddOffsetEntry(file *os.File, entry *OffsetEntry) error {
	// serialize entry
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[0:8], entry.StringOffset)
	binary.LittleEndian.PutUint32(buf[8:12], entry.PostingsRef)
	// get current file size to append at the end
	fileInfo, err := file.Stat()
	if err != nil {
		return err
	}
	// write to end of file
	_, err = file.WriteAt(buf, fileInfo.Size())
	return err
}

/*
RetrieveOffsetEntry retrieves the OffsetEntry for the given string ID.
Parameters:
- file: pointer to the offsets file
- dictID: uint32 ID of the string (0-based index)
Returns:
- *OffsetEntry: pointer to the retrieved OffsetEntry
- error: if any
*/
func RetrieveOffsetEntry(file *os.File, dictID uint32) (*OffsetEntry, error) {
	// calculate offset in file
	offset := int64(dictID) * 12
	buf := make([]byte, 12)
	_, err := file.ReadAt(buf, offset)
	if err != nil {
		return nil, err
	}
	// deserialize entry
	entry := &OffsetEntry{
		StringOffset: binary.LittleEndian.Uint64(buf[0:8]),
		PostingsRef:  binary.LittleEndian.Uint32(buf[8:12]),
	}
	return entry, nil
}
