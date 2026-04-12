package testdb

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

// CreateAndPopulateNIPDatabase creates the North Idaho Politechnic fixture database
// under tempDir, opens it, and loads the JSONL data from the STEP2 repository.
// The returned database remains open; callers should invoke db.CloseDB when finished.
func CreateAndPopulateNIPDatabase(tempDir string) (string, map[string]int, error) {
	step2Root, err := step2RootDir()
	if err != nil {
		return "", nil, err
	}

	schemaFile := filepath.Join(step2Root, "docs", "testdata", "College.ddl")
	dataDir := filepath.Join(step2Root, "docs", "testdata", "NortIdahoPolitechnic")

	err = db.CreateDB(tempDir, schemaFile)
	if err != nil {
		return "", nil, fmt.Errorf("CreateDB failed: %w", err)
	}

	dbPath := filepath.Join(tempDir, "College")
	err = db.OpenDB(dbPath)
	if err != nil {
		return "", nil, fmt.Errorf("OpenDB failed: %w", err)
	}

	dbDef := db.Definition()
	stats := make(map[string]int)

	dataFiles := []struct {
		filename  string
		tableName string
	}{
		{"Departments.jsonl", "Departments"},
		{"Courses.jsonl", "Courses"},
		{"Teachers.jsonl", "Teachers"},
		{"Students.jsonl", "Students"},
		{"Classes.jsonl", "Classes"},
		{"Grades.jsonl", "Grades"},
		{"Majors.jsonl", "Majors"},
	}

	for _, dataFile := range dataFiles {
		filePath := filepath.Join(dataDir, dataFile.filename)
		count, loadErr := loadTableJSONLinesFile(filePath, dataFile.tableName, dbDef)
		if loadErr != nil {
			db.CloseDB()
			return "", nil, fmt.Errorf("failed to load %s: %w", dataFile.filename, loadErr)
		}
		stats[dataFile.tableName] = count
	}

	enrollmentFilePath := filepath.Join(dataDir, "Enrollment.jsonl")
	enrollmentCount, err := loadEnrollmentSetFile(enrollmentFilePath, dbDef)
	if err != nil {
		db.CloseDB()
		return "", nil, fmt.Errorf("failed to load Enrollment.jsonl: %w", err)
	}
	stats["Enrollment"] = enrollmentCount

	return dbPath, stats, nil
}

func step2RootDir() (string, error) {
	_, filePath, _, ok := runtime.Caller(0)
	if !ok {
		return "", fmt.Errorf("failed to resolve STEP2 source location")
	}

	return filepath.Clean(filepath.Join(filepath.Dir(filePath), "..")), nil
}

func loadTableJSONLinesFile(filePath string, expectedTable string, dbDef *db.DBDefinition) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	count := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var entry struct {
			TableName string                 `json:"tableName"`
			Record    map[string]interface{} `json:"record"`
		}

		err := json.Unmarshal([]byte(line), &entry)
		if err != nil {
			return count, fmt.Errorf("line %d: failed to parse JSON: %w", lineNum, err)
		}

		if entry.TableName != expectedTable {
			return count, fmt.Errorf("line %d: expected tableName '%s', got '%s'", lineNum, expectedTable, entry.TableName)
		}

		recordID, err := record.AddNewRecord(entry.TableName, entry.Record, dbDef)
		if err != nil {
			return count, fmt.Errorf("line %d: failed to add record: %w", lineNum, err)
		}

		count++
		if count%50 == 0 {
			fmt.Printf("  %s: loaded %d records...\n", expectedTable, count)
		}

		_ = recordID
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading file: %w", err)
	}

	return count, nil
}

type enrollmentSetEntry struct {
	Relationship string `json:"relationship"`
	ClassCode    string `json:"classCode"`
	StudentID    string `json:"studentId"`
}

func loadEnrollmentSetFile(filePath string, dbDef *db.DBDefinition) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNum := 0
	count := 0

	for scanner.Scan() {
		lineNum++
		line := scanner.Text()
		if len(line) == 0 {
			continue
		}

		var entry enrollmentSetEntry
		if err := json.Unmarshal([]byte(line), &entry); err != nil {
			return count, fmt.Errorf("line %d: failed to parse JSON: %w", lineNum, err)
		}

		if err := validateEnrollmentSetEntry(entry, lineNum); err != nil {
			return count, err
		}

		classRecordID, err := record.GetRecordID("Classes", entry.ClassCode, dbDef)
		if err != nil {
			return count, fmt.Errorf("line %d: failed to resolve Classes.Class_code %q: %w", lineNum, entry.ClassCode, err)
		}

		studentRecordID, err := record.GetRecordID("Students", entry.StudentID, dbDef)
		if err != nil {
			return count, fmt.Errorf("line %d: failed to resolve Students.Student_id %q: %w", lineNum, entry.StudentID, err)
		}

		if err := addSetMemberIgnoringDuplicates("Classes", classRecordID, "Enrollment", studentRecordID, dbDef); err != nil {
			return count, fmt.Errorf("line %d: failed to populate Classes.Enrollment: %w", lineNum, err)
		}

		if err := addSetMemberIgnoringDuplicates("Students", studentRecordID, "TakesClasses", classRecordID, dbDef); err != nil {
			return count, fmt.Errorf("line %d: failed to populate Students.TakesClasses: %w", lineNum, err)
		}

		count++
		if count%50 == 0 {
			fmt.Printf("  Enrollment: loaded %d relationships...\n", count)
		}
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading file: %w", err)
	}

	return count, nil
}

func validateEnrollmentSetEntry(entry enrollmentSetEntry, lineNum int) error {
	if entry.Relationship != "Enrollment" {
		return fmt.Errorf("line %d: expected relationship %q, got %q", lineNum, "Enrollment", entry.Relationship)
	}
	if entry.ClassCode == "" {
		return fmt.Errorf("line %d: classCode is required", lineNum)
	}
	if entry.StudentID == "" {
		return fmt.Errorf("line %d: studentId is required", lineNum)
	}
	return nil
}

func addSetMemberIgnoringDuplicates(ownerTableName string, ownerRecordID uint32, setName string, memberRecordID uint32, dbDef *db.DBDefinition) error {
	err := record.AddSetMember(ownerTableName, ownerRecordID, setName, memberRecordID, dbDef)
	if err != nil && !errors.Is(err, db.ErrSetDuplicate) {
		return err
	}
	return nil
}
