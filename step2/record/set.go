package record

import (
	"encoding/binary"
	"fmt"
	"github.com/kozwoj/step2/db"
)

/* gofmt:off

AddSetMember adds a member record to a set owned by another record.
Parameters:
- ownerTableName: name of the owner table
- ownerRecordID: the owner record identifier (uint32)
- setName: name of the set
- memberRecordID: the member record identifier to add (uint32)
- dbDef: DBDefinition struct with the database definition
Returns:
- error: if there is an issue adding the set member

The function performs the following steps:
- Find owner table in DBDefinition
- Validate owner record exists and is not deleted
- Find set description by name in owner table's Sets array
- Validate member record exists in member table
- Read owner record header to get current block number for this set
- If block number is NoSet (empty set):
	- Call db.AddSetMembers to create new set with single member
	- Update owner record header with new block number
- Otherwise:
	- Call db.AddMemberToSet to add member to existing set
- Return success

gofmt:on */

func AddSetMember(ownerTableName string, ownerRecordID uint32, setName string, memberRecordID uint32, dbDef *db.DBDefinition) error {
	// Find owner table
	ownerTableIndex, ok := dbDef.TableIndex[ownerTableName]
	if !ok {
		return fmt.Errorf("owner table '%s' not found", ownerTableName)
	}
	ownerTable := dbDef.Tables[ownerTableIndex]

	// Validate owner record exists and is not deleted
	ownerRecordData, err := GetRecordData(ownerTable.RecordFile, ownerRecordID)
	if err != nil {
		return fmt.Errorf("failed to read owner record: %w", err)
	}
	if len(ownerRecordData) < 1 {
		return fmt.Errorf("owner record data too short")
	}
	if ownerRecordData[0] != 0x00 {
		return fmt.Errorf("owner record is deleted")
	}

	// Find set description by name
	var setDesc *db.SetDescription
	var setIndex int
	for i, s := range ownerTable.Sets {
		if s.Name == setName {
			setDesc = s
			setIndex = i
			break
		}
	}
	if setDesc == nil {
		return fmt.Errorf("set '%s' not found in table '%s'", setName, ownerTableName)
	}

	// Find member table
	memberTableIndex, ok := dbDef.TableIndex[setDesc.MemberTableName]
	if !ok {
		return fmt.Errorf("member table '%s' not found", setDesc.MemberTableName)
	}
	memberTable := dbDef.Tables[memberTableIndex]

	// Validate member record exists
	memberRecordData, err := GetRecordData(memberTable.RecordFile, memberRecordID)
	if err != nil {
		return fmt.Errorf("failed to read member record: %w", err)
	}
	if len(memberRecordData) < 1 {
		return fmt.Errorf("member record data too short")
	}
	if memberRecordData[0] != 0x00 {
		return fmt.Errorf("member record is deleted")
	}

	// Read owner record header to get block number for this set
	// Header structure: [DeletedFlag:1][NextDeletedID:4][Set0:4][Set1:4]...
	setBlockOffset := 5 + (setIndex * 4)
	if len(ownerRecordData) < setBlockOffset+4 {
		return fmt.Errorf("owner record header too short for set data")
	}
	currentBlockNumber := binary.LittleEndian.Uint32(ownerRecordData[setBlockOffset : setBlockOffset+4])

	// Get set file header
	setFileHeader, err := db.GetSetFileHeader(setDesc.MembersFile)
	if err != nil {
		return fmt.Errorf("failed to read set file header: %w", err)
	}

	// Check if set is empty (NoSet)
	if currentBlockNumber == uint32(NoSet) {
		// Create new set with single member
		memberIDs := []uint32{memberRecordID}
		newBlockNumber, err := db.AddSetMembers(setDesc.MembersFile, setFileHeader, memberIDs, ownerRecordID)
		if err != nil {
			return fmt.Errorf("failed to create new set: %w", err)
		}

		// Update owner record header with new block number
		binary.LittleEndian.PutUint32(ownerRecordData[setBlockOffset:setBlockOffset+4], newBlockNumber)
		err = OverrideRecordData(ownerTable.RecordFile, ownerRecordID, ownerRecordData)
		if err != nil {
			return fmt.Errorf("failed to update owner record header: %w", err)
		}
	} else {
		// Add member to existing set
		err = db.AddMemberToSet(setDesc.MembersFile, setFileHeader, currentBlockNumber, memberRecordID, ownerRecordID)
		if err != nil {
			return fmt.Errorf("failed to add member to set: %w", err)
		}
	}

	return nil
}

/* gofmt:off

GetSetMembers retrieves all member recordIDs from a set.
Parameters:
- ownerTableName: name of the owner table
- ownerRecordID: the owner record identifier (uint32)
- setName: name of the set
- dbDef: DBDefinition struct with the database definition
Returns:
- []uint32: array of member recordIDs
- error: if there is an issue retrieving set members

The function performs the following steps:
- Find owner table in DBDefinition
- Validate owner record exists and is not deleted
- Find set description by name in owner table's Sets array
- Read owner record header to get block number for this set
- If block number is NoSet (empty set), return empty members array
- Otherwise, call db.GetSetMembers to retrieve member recordIDs
- Return member IDs array

gofmt:on */

func GetSetMembers(ownerTableName string, ownerRecordID uint32, setName string, dbDef *db.DBDefinition) ([]uint32, error) {
	// Find owner table
	ownerTableIndex, ok := dbDef.TableIndex[ownerTableName]
	if !ok {
		return nil, fmt.Errorf("owner table '%s' not found", ownerTableName)
	}
	ownerTable := dbDef.Tables[ownerTableIndex]

	// Validate owner record exists and is not deleted
	ownerRecordData, err := GetRecordData(ownerTable.RecordFile, ownerRecordID)
	if err != nil {
		return nil, fmt.Errorf("failed to read owner record: %w", err)
	}
	if len(ownerRecordData) < 1 {
		return nil, fmt.Errorf("owner record data too short")
	}
	if ownerRecordData[0] != 0x00 {
		return nil, fmt.Errorf("owner record is deleted")
	}

	// Find set description by name
	var setDesc *db.SetDescription
	var setIndex int
	for i, s := range ownerTable.Sets {
		if s.Name == setName {
			setDesc = s
			setIndex = i
			break
		}
	}
	if setDesc == nil {
		return nil, fmt.Errorf("set '%s' not found in table '%s'", setName, ownerTableName)
	}

	// Read owner record header to get block number for this set
	// Header structure: [DeletedFlag:1][NextDeletedID:4][Set0:4][Set1:4]...
	setBlockOffset := 5 + (setIndex * 4)
	if len(ownerRecordData) < setBlockOffset+4 {
		return nil, fmt.Errorf("owner record header too short for set data")
	}
	currentBlockNumber := binary.LittleEndian.Uint32(ownerRecordData[setBlockOffset : setBlockOffset+4])

	// Get member IDs
	var memberIDs []uint32
	if currentBlockNumber == uint32(NoSet) {
		// Set is empty, return empty array
		memberIDs = []uint32{}
	} else {
		// Get set file header
		setFileHeader, err := db.GetSetFileHeader(setDesc.MembersFile)
		if err != nil {
			return nil, fmt.Errorf("failed to read set file header: %w", err)
		}

		// Retrieve members from set
		memberIDs, err = db.GetSetMembers(setDesc.MembersFile, currentBlockNumber, setFileHeader.BlockSize)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve set members: %w", err)
		}
	}

	return memberIDs, nil
}

/* gofmt:off

RemoveSetMember removes a member record from a set.
Parameters:
- ownerTableName: name of the owner table
- ownerRecordID: the owner record identifier (uint32)
- setName: name of the set
- memberRecordID: the member record identifier to remove (uint32)
- dbDef: DBDefinition struct with the database definition
Returns:
- error: if there is an issue removing the set member

The function performs the following steps:
- Find owner table in DBDefinition
- Validate owner record exists and is not deleted
- Find set description by name in owner table's Sets array
- Read owner record header to get current block number for this set
- If block number is NoSet (empty set), return error
- Call db.RemoveMemberFromSet to remove the member
- Check if set is now empty by calling db.GetSetMembers
- If empty:
	- Call db.DeleteSet to free the blocks
	- Update owner record header to set block number back to NoSet
- Return success

gofmt:on */

func RemoveSetMember(ownerTableName string, ownerRecordID uint32, setName string, memberRecordID uint32, dbDef *db.DBDefinition) error {
	// Find owner table
	ownerTableIndex, ok := dbDef.TableIndex[ownerTableName]
	if !ok {
		return fmt.Errorf("owner table '%s' not found", ownerTableName)
	}
	ownerTable := dbDef.Tables[ownerTableIndex]

	// Validate owner record exists and is not deleted
	ownerRecordData, err := GetRecordData(ownerTable.RecordFile, ownerRecordID)
	if err != nil {
		return fmt.Errorf("failed to read owner record: %w", err)
	}
	if len(ownerRecordData) < 1 {
		return fmt.Errorf("owner record data too short")
	}
	if ownerRecordData[0] != 0x00 {
		return fmt.Errorf("owner record is deleted")
	}

	// Find set description by name
	var setDesc *db.SetDescription
	var setIndex int
	for i, s := range ownerTable.Sets {
		if s.Name == setName {
			setDesc = s
			setIndex = i
			break
		}
	}
	if setDesc == nil {
		return fmt.Errorf("set '%s' not found in table '%s'", setName, ownerTableName)
	}

	// Read owner record header to get block number for this set
	// Header structure: [DeletedFlag:1][NextDeletedID:4][Set0:4][Set1:4]...
	setBlockOffset := 5 + (setIndex * 4)
	if len(ownerRecordData) < setBlockOffset+4 {
		return fmt.Errorf("owner record header too short for set data")
	}
	currentBlockNumber := binary.LittleEndian.Uint32(ownerRecordData[setBlockOffset : setBlockOffset+4])

	// Check if set is empty
	if currentBlockNumber == uint32(NoSet) {
		return fmt.Errorf("set is empty, cannot remove member")
	}

	// Get set file header
	setFileHeader, err := db.GetSetFileHeader(setDesc.MembersFile)
	if err != nil {
		return fmt.Errorf("failed to read set file header: %w", err)
	}

	// Remove member from set
	err = db.RemoveMemberFromSet(setDesc.MembersFile, setFileHeader, currentBlockNumber, memberRecordID, ownerRecordID)
	if err != nil {
		return fmt.Errorf("failed to remove member from set: %w", err)
	}

	// Check if set is now empty
	memberIDs, err := db.GetSetMembers(setDesc.MembersFile, currentBlockNumber, setFileHeader.BlockSize)
	if err != nil {
		return fmt.Errorf("failed to check set members after removal: %w", err)
	}

	// If set is empty, delete it and update owner record header
	if len(memberIDs) == 0 {
		// Delete the set (free the blocks)
		err = db.DeleteSet(setDesc.MembersFile, setFileHeader, currentBlockNumber)
		if err != nil {
			return fmt.Errorf("failed to delete empty set: %w", err)
		}

		// Update owner record header to set block number back to NoSet
		binary.LittleEndian.PutUint32(ownerRecordData[setBlockOffset:setBlockOffset+4], uint32(NoSet))
		err = OverrideRecordData(ownerTable.RecordFile, ownerRecordID, ownerRecordData)
		if err != nil {
			return fmt.Errorf("failed to update owner record header: %w", err)
		}
	}

	return nil
}
