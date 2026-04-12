package test_db

import (
	"os"
	"testing"

	"github.com/kozwoj/step2/db"
)

// go test -run TestCreateAndPopulateNIPDatabase -v -count=1 ./test_db

func TestCreateAndPopulateNIPDatabase(t *testing.T) {
	tempDir := t.TempDir()
	// tempDir := "C:\\temp\\nip_test_db"

	dbPath, stats, err := CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(db.CloseDB)

	if _, err := os.Stat(dbPath); err != nil {
		t.Fatalf("expected database path to exist at %q: %v", dbPath, err)
	}

	if !db.DefinitionInitialized() {
		t.Fatal("expected database definition to be initialized after opening test database")
	}

	// print record counts for each table
	t.Log("\nRecord counts:")
	for tableName, count := range stats {
		t.Logf("  %-15s: %3d records", tableName, count)
	}
}
