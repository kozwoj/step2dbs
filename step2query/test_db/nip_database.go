package test_db

import "github.com/kozwoj/step2/testdb"

// CreateAndPopulateNIPDatabase creates the shared North Idaho Politechnic fixture DB
// using the canonical schema and JSONL data stored in the STEP2 repository.
func CreateAndPopulateNIPDatabase(tempDir string) (string, map[string]int, error) {
	return testdb.CreateAndPopulateNIPDatabase(tempDir)
}
