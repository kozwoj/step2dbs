package record

import (
	"encoding/binary"
	"fmt"
	"os"
	"path/filepath"
	"github.com/kozwoj/step2/db"
)

const (
	NoNextRecord  uint32 = 0xFFFF // Sentinel value indicating end of leaf linked list
	NoFirstRecord uint32 = 0xFFFF // Sentinel value indicating no previous leaf (first leaf in the chain)
	NoSet         uint32 = 0xFFFF // Sentinel value indicating no specific set instance for the record
)

/*
The RecordFileHeader struct defines the header of a records file. A records file is used to store
records/rows for a specific table - each record file corresponds to a single table.
Records are of fixed length and therefore their offset can be calculated based on their sequential
number - recordID.
Header size is 2 + 2 + 4 + 4 = 12 bytes
- 2 bytes - sequential number of the table holding records
- 2 bytes - length of records in the file = HeaderSize + BodySize
- 4 bytes - ID of the last record in the file (end of the file)
- 4 bytes - first deleted record ID or 0xFFFF (NoFirstRecord) if there are no deleted records
*/

type RecordFileHeader struct {
	TableNo        uint16 // sequential number of the table holding records
	RecordLength   uint16 // HeaderSize + BodySize
	LastRecordID   uint32 // ID of the last record in the file (end of the file)
	FirstDeletedID uint32 // ID of the first deleted record or 0xFFFF (NoFirstRecord)
}

/*
RecordHeader defines the header of a record in the records file. Each record has a header and a body.
- NextDeletedID = 0xFFFF (NoNextRecord) if the record is the end of deleted records list.
- DeletedFlag = 0x00 for active record, 0x01 for deleted record
- The size of the record header is 1 + 4 + (4 * number of sets) bytes
- If Sets[i] == 0xFFFF (NoSet), it indicates no specific set instance for the record
*/
type RecordHeader struct {
	DeletedFlag   uint8    // `0x00` for false, `0x01` for true
	NextDeletedID uint32   // recordIS or 0xFFFF (NoNextRecord) if it's the end of the list
	Sets          []uint32 // Sets file block numbers
}

/*
CreateRecordFile creates a new records file with the given path. The file will be creates with an initial header.
Parameters:
- dirPath: path to directory where the records file should be created
- tableNo: sequential number of the table holding records
- recordLength: length of records in the file = HeaderSize + BodySize
Returns:
- error: if there is an issue creating the records file
*/
func CreateRecordFile(dirPath string, tableNo uint16, recordLength uint16) error {
	// create file
	filePath := filepath.Join(dirPath, "records.dat")
	file, err := os.Create(filePath)
	if err != nil {
		return err
	}
	defer file.Close()

	// initialize header
	header := RecordFileHeader{
		TableNo:        tableNo,
		RecordLength:   recordLength,
		LastRecordID:   0,
		FirstDeletedID: NoFirstRecord,
	}
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint16(buf[0:2], header.TableNo)
	binary.LittleEndian.PutUint16(buf[2:4], header.RecordLength)
	binary.LittleEndian.PutUint32(buf[4:8], header.LastRecordID)
	binary.LittleEndian.PutUint32(buf[8:12], header.FirstDeletedID)
	_, err = file.WriteAt(buf, 0)
	if err != nil {
		return err
	}
	return nil
}

/*
OpenRecordFile opens an existing records file and returns the file pointer and the header.
Parameters:
- filePath: string path to the records file to open
- Returns:
  - *os.File: pointer to the opened file
  - RecordFileHeader: header of the opened file
  - error: if there is an issue opening the records file
*/
func OpenRecordFile(filePath string) (*os.File, RecordFileHeader, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR, 0644)
	if err != nil {
		return nil, RecordFileHeader{}, err
	}
	buf := make([]byte, 12)
	_, err = file.ReadAt(buf, 0)
	if err != nil {
		file.Close()
		return nil, RecordFileHeader{}, err
	}
	header := RecordFileHeader{
		TableNo:        binary.LittleEndian.Uint16(buf[0:2]),
		RecordLength:   binary.LittleEndian.Uint16(buf[2:4]),
		LastRecordID:   binary.LittleEndian.Uint32(buf[4:8]),
		FirstDeletedID: binary.LittleEndian.Uint32(buf[8:12]),
	}
	return file, header, nil
}

/*
GetRecordsFileHeader reads the header of a records file and deserializes it into a RecordFileHeader struct.
Parameters:
- file: pointer to an opened records file
Returns:
- RecordFileHeader
- error: if there is an issue reading the header
*/
func GetRecordsFileHeader(file *os.File) (RecordFileHeader, error) {
	buf := make([]byte, 12)
	_, err := file.ReadAt(buf, 0)
	if err != nil {
		return RecordFileHeader{}, err
	}
	header := RecordFileHeader{
		TableNo:        binary.LittleEndian.Uint16(buf[0:2]),
		RecordLength:   binary.LittleEndian.Uint16(buf[2:4]),
		LastRecordID:   binary.LittleEndian.Uint32(buf[4:8]),
		FirstDeletedID: binary.LittleEndian.Uint32(buf[8:12]),
	}
	return header, nil
}

/*
UpdateRecordsFileHeader updates the header of a records file with the given RecordFileHeader struct.
Parameters:
- file: pointer to an opened records file
- header: RecordFileHeader struct with updated values
Returns:
- error: if there is an issue updating the header
*/
func UpdateRecordsFileHeader(file *os.File, header RecordFileHeader) error {
	buf := make([]byte, 12)
	binary.LittleEndian.PutUint16(buf[0:2], header.TableNo)
	binary.LittleEndian.PutUint16(buf[2:4], header.RecordLength)
	binary.LittleEndian.PutUint32(buf[4:8], header.LastRecordID)
	binary.LittleEndian.PutUint32(buf[8:12], header.FirstDeletedID)
	_, err := file.WriteAt(buf, 0)
	return err
}

/* ----------------------------------------------------------------------------------------------
The following functions operate on serialized records passed/returned as byte slices of the
length specified in the RecordFileHeader. The functions are
- AddRecordData: adds a new record to the file and returns its recordID
- GetRecordData: retrieves a record by its recordID
- OverrideRecordData: overrides a record with a given recordID
- DeleteRecordData: deletes a record with a given recordID and adds it to deleted records list

The functions assume that the records are well formed and serialized.
Only AddRecordDataData and DeleteRecordData may modify the file header (LastRecordID and FirstDeletedID)
---------------------------------------------------------------------------------------------- */

/*
AddRecordData adds a new record to the end of the records file. The call should be made after opening
the file.
Parameters:
- file: pointer to opened records file
- recordData: byte slice with the record header and body
Returns:
- recordID: sequential ID of the added record
- error: if there is an issue adding the record

note:
The first byte in the record header should be set to `0x00` for a new record to indicate
that it's not deleted.
If there are deleted records, the new record is added to the position of the first deleted
record and the header will be updated to point to the next deleted record or set
to 0xFFFF (NoFirstRecord).
*/
func AddRecordData(file *os.File, recordData []byte) (uint32, error) {
	// get header{
	header, err := GetRecordsFileHeader(file)
	if err != nil {
		return 0, err
	}
	// find out if there are deleted records to reuse
	var recordID uint32
	if header.FirstDeletedID != NoFirstRecord {
		recordID = header.FirstDeletedID
		// update record file header to point to the next deleted record in the list
		deletedRecordHeader := make([]byte, 5) // 5 size of RecordHeader without sets
		recordOffset := int64(12) + int64(recordID)*int64(header.RecordLength)
		_, err = file.ReadAt(deletedRecordHeader, recordOffset)
		if err != nil {
			return 0, err
		}
		header.FirstDeletedID = binary.LittleEndian.Uint32(deletedRecordHeader[1:5])
	} else {
		recordID = header.LastRecordID + 1
		header.LastRecordID = recordID
	}
	// write record header back to the file
	err = UpdateRecordsFileHeader(file, header)
	if err != nil {
		return 0, err
	}
	// write record data to the file
	recordOffset := int64(12) + int64(recordID)*int64(header.RecordLength)
	_, err = file.WriteAt(recordData, recordOffset)
	if err != nil {
		return 0, err
	}
	return recordID, nil
}

/*
GetRecordData retrieves a record by its recordID. The call should be made after opening the file.
Parameters:
- file: pointer to opened records file
- recordID: sequential ID of the record to retrieve
Returns:
- recordData: byte slice with the record header and body
- error: if there is an issue retrieving the record
*/
func GetRecordData(file *os.File, recordID uint32) ([]byte, error) {
	header, err := GetRecordsFileHeader(file)
	if err != nil {
		return nil, err
	}
	if recordID > header.LastRecordID {
		return nil, fmt.Errorf("recordID %d is out of bounds, last record ID is %d", recordID, header.LastRecordID)
	}
	recordData := make([]byte, header.RecordLength)
	recordOffset := int64(12) + int64(recordID)*int64(header.RecordLength)
	_, err = file.ReadAt(recordData, recordOffset)
	if err != nil {
		return nil, err
	}
	return recordData, nil
}

/*
OverrideRecordData overrides a record with a given recordID. The call should be made after opening the file.
Parameters:
- file: pointer to opened records file
- recordID: sequential ID of the record to override
- recordData: byte slice with the new record header and body
Returns:
- error: if there is an issue overriding the record
*/
func OverrideRecordData(file *os.File, recordID uint32, recordData []byte) error {
	header, err := GetRecordsFileHeader(file)
	if err != nil {
		return err
	}
	if recordID > header.LastRecordID {
		return fmt.Errorf("recordID %d is out of bounds, last record ID is %d", recordID, header.LastRecordID)
	}
	recordOffset := int64(12) + int64(recordID)*int64(header.RecordLength)
	_, err = file.WriteAt(recordData, recordOffset)
	return err
}

/*
DeleteRecordData deletes a record with a given recordID and adds it to the deleted records list. The call should be made after opening the file.
Parameters:
- file: pointer to opened records file
- recordID: sequential ID of the record to delete
Returns:
- error: if there is an issue deleting the record

note:
If the record has instances of sets, the sets should be deleted first and the record header should be updated to remove
the sets info before calling DeleteRecordData.
*/
func DeleteRecordData(file *os.File, recordID uint32) error {
	fileHeader, err := GetRecordsFileHeader(file)
	if err != nil {
		return err
	}
	if recordID > fileHeader.LastRecordID {
		return fmt.Errorf("recordID %d is out of bounds, last record ID is %d", recordID, fileHeader.LastRecordID)
	}
	// get TableDescription for the record from the global DBDefinition
	dbDef := db.Definition()
	if dbDef == nil {
		return fmt.Errorf("Panic: DBDefinition is not initialized")
	}
	tableNo := fileHeader.TableNo
	tableDescription := dbDef.Tables[tableNo]
	if tableDescription == nil {
		return fmt.Errorf("Table with tableNo %d not found in DBDefinition", tableNo)
	}
	// get record header
	recordBytes, err := GetRecordData(file, recordID)
	if err != nil {
		return err
	}
	recordHeader, err := GetRecordHeader(recordBytes, tableDescription)
	if err != nil {
		return err
	}
	// see if the record has been already deleted
	if recordHeader.DeletedFlag == 0x01 {
		return fmt.Errorf("record with recordID %d is already deleted", recordID)
	}
	// delete any sets associated with the record
	if len(recordHeader.Sets) > 0 {
		for i, setBlockNo := range recordHeader.Sets {
			if setBlockNo != NoSet {
				// delete the set instance and update the record header to remove the ref
				membersFile := tableDescription.Sets[i].MembersFile
				// get postings file header
				setHeader, err := db.GetSetFileHeader(membersFile)
				if err != nil {
					return err
				}
				// delete set instance list from the file starting with the block number
				err = db.DeleteSet(membersFile, setHeader, setBlockNo)
				if err != nil {
					return err
				}
				recordHeader.Sets[i] = NoSet
			}
		}
	}
	// mark record as deleted, and add it to beginning of deleted records list
	recordHeader.DeletedFlag = 0x01
	recordHeader.NextDeletedID = fileHeader.FirstDeletedID
	fileHeader.FirstDeletedID = recordID
	// convert record header back to byte slice
	headerData, err := ConvertRecordHeaderToBytes(recordHeader)
	if err != nil {
		return err
	}
	// put header data back to the record data
	copy(recordBytes[0:len(headerData)], headerData)
	// override record data in the file
	err = OverrideRecordData(file, recordID, recordBytes)
	if err != nil {
		return err
	}
	// update file header with new FirstDeletedID
	err = UpdateRecordsFileHeader(file, fileHeader)
	if err != nil {
		return err
	}

	return nil
}
