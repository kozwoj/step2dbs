package engine

import (
	"path/filepath"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

func TestFirstActiveRecordIDReturnsFirstRecord(t *testing.T) {
	dbDef := loadAllTypesDB(t)

	addAllTypesRecord(t, dbDef, 100)
	addAllTypesRecord(t, dbDef, 200)

	firstID, err := FirstActiveRecordID("AllTypes", dbDef)
	if err != nil {
		t.Fatalf("FirstActiveRecordID returned error: %v", err)
	}

	if firstID != 1 {
		t.Fatalf("expected first active record ID 1, got %d", firstID)
	}
}

func TestFirstActiveRecordIDSkipsDeletedRecords(t *testing.T) {
	dbDef := loadAllTypesDB(t)

	firstInsertedID := addAllTypesRecord(t, dbDef, 100)
	secondInsertedID := addAllTypesRecord(t, dbDef, 200)

	if err := record.DeleteRecord("AllTypes", firstInsertedID, dbDef); err != nil {
		t.Fatalf("DeleteRecord returned error: %v", err)
	}

	firstID, err := FirstActiveRecordID("AllTypes", dbDef)
	if err != nil {
		t.Fatalf("FirstActiveRecordID returned error: %v", err)
	}

	if firstID != secondInsertedID {
		t.Fatalf("expected first active record ID %d, got %d", secondInsertedID, firstID)
	}
}

func TestNextActiveRecordIDSkipsDeletedRecords(t *testing.T) {
	dbDef := loadAllTypesDB(t)

	firstID := addAllTypesRecord(t, dbDef, 100)
	middleID := addAllTypesRecord(t, dbDef, 200)
	thirdID := addAllTypesRecord(t, dbDef, 300)

	if err := record.DeleteRecord("AllTypes", middleID, dbDef); err != nil {
		t.Fatalf("DeleteRecord returned error: %v", err)
	}

	nextID, err := NextActiveRecordID("AllTypes", firstID, dbDef)
	if err != nil {
		t.Fatalf("NextActiveRecordID returned error: %v", err)
	}

	if nextID != thirdID {
		t.Fatalf("expected next active record ID %d, got %d", thirdID, nextID)
	}
}

func TestFirstActiveRecordIDRejectsEmptyTable(t *testing.T) {
	dbDef := loadAllTypesDB(t)

	_, err := FirstActiveRecordID("AllTypes", dbDef)
	if err == nil {
		t.Fatal("expected error for empty table, got nil")
	}
}

func loadAllTypesDB(t *testing.T) *stepdb.DBDefinition {
	t.Helper()

	tempDir := t.TempDir()
	schemaFile := filepath.Join("..", "..", "step2", "docs", "testdata", "AllTypes.ddl")

	if err := stepdb.CreateDB(tempDir, schemaFile); err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}

	dbDir := filepath.Join(tempDir, "AllTypesTes")
	if err := stepdb.OpenDB(dbDir); err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}

	t.Cleanup(func() {
		stepdb.CloseDB()
	})

	return stepdb.Definition()
}

func addAllTypesRecord(t *testing.T, dbDef *stepdb.DBDefinition, integerValue int) uint32 {
	t.Helper()

	recordID, err := record.AddNewRecord("AllTypes", map[string]interface{}{
		"Integer_value":        float64(integerValue),
		"Small_int_value":      float64(1),
		"Big_int_value":        float64(1000),
		"Decimal_value":        "10.50",
		"Float_value":          1.5,
		"String_size_value":    "Test",
		"String_no_size_value": "Test",
		"Char_array_value":     "TESTCHAR0000000",
		"Boolean_value":        true,
		"Date_value":           "2024-03-01",
		"Time_value":           "10:00:00",
	}, dbDef)
	if err != nil {
		t.Fatalf("AddNewRecord failed: %v", err)
	}

	return recordID
}
