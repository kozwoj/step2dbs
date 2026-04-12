package testdb

import (
	"testing"

	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

func TestCreateAndPopulateNIPDatabaseLoadsEnrollmentSets(t *testing.T) {
	tempDir := t.TempDir()

	_, stats, err := CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(db.CloseDB)

	if stats["Enrollment"] == 0 {
		t.Fatal("expected Enrollment relationships to be loaded")
	}

	dbDef := db.Definition()
	math202Section1ID := mustGetRecordID(t, "Classes", "MATH202-01", dbDef)
	math202Section2ID := mustGetRecordID(t, "Classes", "MATH202-02", dbDef)

	section1Members, err := record.GetSetMembers("Classes", math202Section1ID, "Enrollment", dbDef)
	if err != nil {
		t.Fatalf("GetSetMembers for MATH202-01 failed: %v", err)
	}
	section2Members, err := record.GetSetMembers("Classes", math202Section2ID, "Enrollment", dbDef)
	if err != nil {
		t.Fatalf("GetSetMembers for MATH202-02 failed: %v", err)
	}

	assertContainsRecordIDs(t, section1Members,
		mustGetRecordID(t, "Students", "NIP2209001", dbDef),
		mustGetRecordID(t, "Students", "NIP2209008", dbDef),
	)
	assertContainsRecordIDs(t, section2Members,
		mustGetRecordID(t, "Students", "NIP2209009", dbDef),
		mustGetRecordID(t, "Students", "NIP2309011", dbDef),
	)

	studentID := mustGetRecordID(t, "Students", "NIP2209009", dbDef)
	takesClasses, err := record.GetSetMembers("Students", studentID, "TakesClasses", dbDef)
	if err != nil {
		t.Fatalf("GetSetMembers for Students.TakesClasses failed: %v", err)
	}
	assertContainsRecordIDs(t, takesClasses, math202Section2ID)
}

func mustGetRecordID(t *testing.T, tableName string, primaryKey interface{}, dbDef *db.DBDefinition) uint32 {
	t.Helper()

	recordID, err := record.GetRecordID(tableName, primaryKey, dbDef)
	if err != nil {
		t.Fatalf("GetRecordID(%s, %v) failed: %v", tableName, primaryKey, err)
	}

	return recordID
}

func assertContainsRecordIDs(t *testing.T, got []uint32, want ...uint32) {
	t.Helper()

	gotSet := make(map[uint32]struct{}, len(got))
	for _, recordID := range got {
		gotSet[recordID] = struct{}{}
	}

	for _, recordID := range want {
		if _, ok := gotSet[recordID]; !ok {
			t.Fatalf("expected record ID %d in set, got %v", recordID, got)
		}
	}
}