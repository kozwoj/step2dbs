package dictionary

import (
	"encoding/binary"
	"os"
	"github.com/kozwoj/indexing/dictionary/postings"
)

/* Strings are stored in a blob file where every new string is appended to the end of the file. Each
string is assigned the sequential number.

The header of the strings file is as follows (12 bytes):
- EndOffset: uint64 - offset of the end of the file where the next string should be written (8 bytes)
- NumOfStrings: uint32 - number of strings already stored in the file (4 bytes)

The operations of the strings file are:
- CreateStringsFile creates the strings file when a new dictionary is created
- OpenStringsFile opens the strings file when an existing dictionary is opened
- AddStringEntry adds new string to the end of the strings file
- RetrieveStringEntry retrieves string for a given string ID

note:
strings are only added to the directory and never deleted, so there is no DeleteString operation.
*/

type StringsFileHeader struct {
	EndOffset    uint64 // offset of the end of the file where the next string should be written
	NumOfStrings uint32 // number of strings already stored in the file
}

/*
CreateStringsFile creates a new strings file with the given path. The file will be creates with an initial header.
Parameters:
- filePath: string path to the strings file to create
Returns:
- *os.File: pointer to the created strings file
- error: if any
*/
func CreateStringsFile(filePath string) (*os.File, error) {
	file, err := os.Create(filePath)
	if err != nil {
		return nil, err
	}
	// initialize header
	header := StringsFileHeader{
		EndOffset:    12, // initial offset after header (8+4 bytes)
		NumOfStrings: 0,
	}
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint64(buf[0:8], header.EndOffset)
	binary.LittleEndian.PutUint32(buf[8:12], header.NumOfStrings)
	_, err = file.WriteAt(buf, 0)
	if err != nil {
		file.Close()
		return nil, err
	}
	return file, nil
}

/*
OpenStringsFile opens an existing strings file and returns the file pointer and the header.
Parameters:
- filePath: string path to the strings file to open
Returns:
- *os.File: pointer to the opened strings file
- *StringsFileHeader: pointer to the header struct
- error: if any
*/
func OpenStringsFile(filePath string) (*os.File, *StringsFileHeader, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, nil, err
	}
	headerBuf := make([]byte, 12)
	_, err = file.ReadAt(headerBuf, 0)
	if err != nil {
		file.Close()
		return nil, nil, err
	}
	header := &StringsFileHeader{}
	header.EndOffset = binary.LittleEndian.Uint64(headerBuf[0:8])
	header.NumOfStrings = binary.LittleEndian.Uint32(headerBuf[8:12])
	return file, header, nil
}

/*
AddStringEntry adds a new string to the end of the strings file. The call should be made after
verifying that the string does not already exist in the dictionary!
Parameters:
- stringFile: pointer to the raw strings file
- offsetFile: pointer to the offsets file
- postingsFile: pointer to the postings file
- postingsHeader: pointer to the postings file header
- string: string to add
Returns:
- dictID: uint64 ID assigned to the added string
- postingsRef: uint32 postings reference assigned to the added string
- error: if any
*/
func AddStringEntry(stringFile *os.File, offsetFile *os.File, postingsFile *os.File, postingsHeader *postings.PostingsFileHeader, str string) (uint32, uint32, error) {
	// read strings file header
	headerBuf := make([]byte, 12)
	_, err := stringFile.ReadAt(headerBuf, 0)
	if err != nil {
		return 0, 0, err
	}
	header := &StringsFileHeader{}
	header.EndOffset = binary.LittleEndian.Uint64(headerBuf[0:8])
	header.NumOfStrings = binary.LittleEndian.Uint32(headerBuf[8:12])

	// get the current string offset
	stringOffset := header.EndOffset
	dictID := header.NumOfStrings

	// write string at EndOffset
	stringBytes := []byte(str)
	_, err = stringFile.WriteAt(stringBytes, int64(header.EndOffset))
	if err != nil {
		return 0, 0, err
	}

	// update strings file header
	header.EndOffset += uint64(len(stringBytes))
	header.NumOfStrings++
	binary.LittleEndian.PutUint64(headerBuf[0:8], header.EndOffset)
	binary.LittleEndian.PutUint32(headerBuf[8:12], header.NumOfStrings)
	_, err = stringFile.WriteAt(headerBuf, 0)
	if err != nil {
		return 0, 0, err
	}

	// create new postings list in the postings file for this new string and get postings reference
	postingsList := postings.NewPostingsList(postingsHeader.Format, postingsHeader.BlockSize)
	postingsRef, err := postingsList.AddNewList(postingsFile, postingsHeader, []uint32{}, dictID)
	if err != nil {
		return 0, 0, err
	}

	// add offset entry with no postings reference yet (NoBlock)
	entry := &OffsetEntry{
		StringOffset: stringOffset,
		PostingsRef:  postingsRef,
	}
	err = AddOffsetEntry(offsetFile, entry)
	if err != nil {
		return 0, 0, err
	}

	return dictID, entry.PostingsRef, nil
}

/*
RetrieveStringEntry retrieves the string for the given dictID.
Parameters:
- stringFile: pointer to the strings file
- offsetFile: pointer to the offsets file
- dictID: uint32 ID of the string to retrieve
Returns:
- string: retrieved string
- error: if any
*/
func RetrieveStringEntry(stringFile *os.File, offsetFile *os.File, dictID uint32) (string, error) {
	// get offset entry for this dictID
	offsetEntry, err := RetrieveOffsetEntry(offsetFile, dictID)
	if err != nil {
		return "", err
	}
	// get the stings file header to see if the dictID is for the last string
	stringFileHeaderBuf := make([]byte, 12)
	_, err = stringFile.ReadAt(stringFileHeaderBuf, 0)
	if err != nil {
		return "", err
	}
	stringFileHeader := &StringsFileHeader{}
	stringFileHeader.EndOffset = binary.LittleEndian.Uint64(stringFileHeaderBuf[0:8])
	stringFileHeader.NumOfStrings = binary.LittleEndian.Uint32(stringFileHeaderBuf[8:12])
	// see if this is the last string
	if dictID+1 == stringFileHeader.NumOfStrings {
		// this is the last string, read to current EndOffset
		stringLength := stringFileHeader.EndOffset - offsetEntry.StringOffset
		stringBytes := make([]byte, stringLength)
		_, err = stringFile.ReadAt(stringBytes, int64(offsetEntry.StringOffset))
		if err != nil {
			return "", err
		}
		return string(stringBytes), nil
	}
	// not the last string, so get the next offset entry to determine string length
	nextOffsetEntry, err := RetrieveOffsetEntry(offsetFile, dictID+1)
	if err != nil {
		return "", err
	}
	stringLength := nextOffsetEntry.StringOffset - offsetEntry.StringOffset
	stringBytes := make([]byte, stringLength)
	_, err = stringFile.ReadAt(stringBytes, int64(offsetEntry.StringOffset))
	if err != nil {
		return "", err
	}
	return string(stringBytes), nil
}
