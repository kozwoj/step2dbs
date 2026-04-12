package engine_test

import (
	"bufio"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/builder"
	"github.com/kozwoj/step2query/engine"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
	"github.com/kozwoj/step2query/test_db"
)

// to run the test from command line use:
// go test ./engine -run TestDBWhereStageExecuteStudentsYearTwoOrThreeNeedsFullScan -v

func TestDBWhereStageExecuteStudentsYearTwoOrThreeNeedsFullScan(t *testing.T) {
	tempDir := t.TempDir()
	_, _, err := test_db.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(stepdb.CloseDB)

	dbDef := stepdb.Definition()
	if dbDef == nil {
		t.Fatal("expected opened DBDefinition after creating NIP test database")
	}

	queryText := `Students | where Students.Year == 2 or Students.Year == 3 | return Students.Student_id, Students.Year`
	query, err := parser.Parse(queryText)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}

	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}
	if len(builtPipeline.Stages) == 0 {
		t.Fatal("expected pipeline to contain at least one stage")
	}

	whereStage, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	if whereStage.Plan.DBPlan == nil {
		t.Fatal("expected DBWhereStage to carry a DB analysis plan")
	}
	if whereStage.Plan.DBPlan.RootClass != pipeline.WhereExprNeedsFullInput {
		t.Fatalf("expected DBWhereStage root class %d, got %d", pipeline.WhereExprNeedsFullInput, whereStage.Plan.DBPlan.RootClass)
	}

	input, err := engine.NewSourceDBTableSet("Students", dbDef)
	if err != nil {
		t.Fatalf("NewSourceDBTableSet returned error: %v", err)
	}

	output, err := engine.ExecuteStage(whereStage, input)
	if err != nil {
		t.Fatalf("ExecuteStage returned error: %v", err)
	}

	workingSet, ok := output.(*engine.DBTableWorkingSet)
	if !ok {
		t.Fatalf("expected output state to be *engine.DBTableWorkingSet, got %T", output)
	}

	expectedCount := countStudentsWithYears(t, filepath.Join("..", "..", "step2", "docs", "testdata", "NortIdahoPolitechnic", "Students.jsonl"), 2, 3)
	if len(workingSet.RecordIDs) != expectedCount {
		t.Fatalf("expected %d matching Students records, got %d", expectedCount, len(workingSet.RecordIDs))
	}

	assertWorkingSetYears(t, workingSet, 2, 3)
}

func TestPipelineExecuteWhereReturnStudentsYearTwoOrThree(t *testing.T) {
	tempDir := t.TempDir()
	_, _, err := test_db.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(stepdb.CloseDB)

	dbDef := stepdb.Definition()
	if dbDef == nil {
		t.Fatal("expected opened DBDefinition after creating NIP test database")
	}

	queryText := `Students | where Students.Year == 2 or Students.Year == 3 | return Students.Student_id, Students.Year`
	query, err := parser.Parse(queryText)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}

	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	output, err := engine.ExecutePipeline(builtPipeline, dbDef)
	if err != nil {
		t.Fatalf("ExecutePipeline returned error: %v", err)
	}

	returnSet, ok := output.(*engine.ReturnWorkingSet)
	if !ok {
		t.Fatalf("expected pipeline output to be *engine.ReturnWorkingSet, got %T", output)
	}
	if returnSet.RecordDef().NoFields != 2 {
		t.Fatalf("expected return working set to have 2 fields, got %d", returnSet.RecordDef().NoFields)
	}
	if returnSet.RecordDef().Fields[0].Name != "Students.Student_id" {
		t.Fatalf("expected first projected field Students.Student_id, got %q", returnSet.RecordDef().Fields[0].Name)
	}
	if returnSet.RecordDef().Fields[1].Name != "Students.Year" {
		t.Fatalf("expected second projected field Students.Year, got %q", returnSet.RecordDef().Fields[1].Name)
	}

	expectedCount := countStudentsWithYears(t, filepath.Join("..", "..", "step2", "docs", "testdata", "NortIdahoPolitechnic", "Students.jsonl"), 2, 3)
	if returnSet.Size() != expectedCount {
		t.Fatalf("expected %d projected Students rows, got %d", expectedCount, returnSet.Size())
	}

	assertReturnWorkingSetYears(t, returnSet, 2, 3)
}

func countStudentsWithYears(t *testing.T, filePath string, years ...int) int {
	t.Helper()

	allowedYears := make(map[int]struct{}, len(years))
	for _, year := range years {
		allowedYears[year] = struct{}{}
	}

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open %s: %v", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var entry struct {
			TableName string `json:"tableName"`
			Record    struct {
				Year int `json:"Year"`
			} `json:"record"`
		}

		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("failed to parse Students.jsonl line: %v", err)
		}
		if entry.TableName != "Students" {
			t.Fatalf("expected tableName Students in Students.jsonl, got %q", entry.TableName)
		}
		if _, ok := allowedYears[entry.Record.Year]; ok {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan %s: %v", filePath, err)
	}

	return count
}

func assertWorkingSetYears(t *testing.T, workingSet *engine.DBTableWorkingSet, years ...int) {
	t.Helper()

	allowedYears := make(map[int]struct{}, len(years))
	for _, year := range years {
		allowedYears[year] = struct{}{}
	}

	currentRecord, err := workingSet.GetFirstRecord()
	if err != nil {
		if errors.Is(err, engine.ErrNoMoreRecords) {
			return
		}
		t.Fatalf("GetFirstRecord returned error: %v", err)
	}

	for currentRecord != nil {
		yearValue, ok := currentRecord["Year"].(int)
		if !ok {
			t.Fatalf("expected working-set record Year to be int, got %T", currentRecord["Year"])
		}
		if _, ok := allowedYears[yearValue]; !ok {
			t.Fatalf("expected working-set Year to be one of %v, got %d", years, yearValue)
		}

		currentRecord, err = workingSet.GetNextRecord()
		if err != nil {
			if errors.Is(err, engine.ErrNoMoreRecords) {
				break
			}
			t.Fatalf("GetNextRecord returned error: %v", err)
		}
	}
}

func assertReturnWorkingSetYears(t *testing.T, returnSet *engine.ReturnWorkingSet, years ...int) {
	t.Helper()

	allowedYears := make(map[int]struct{}, len(years))
	for _, year := range years {
		allowedYears[year] = struct{}{}
	}

	currentRecord, err := returnSet.GetFirstRecord()
	if err != nil {
		if errors.Is(err, engine.ErrNoMoreRecords) {
			return
		}
		t.Fatalf("GetFirstRecord returned error: %v", err)
	}

	for currentRecord != nil {
		studentID, ok := currentRecord["Students.Student_id"].(string)
		if !ok {
			t.Fatalf("expected projected Students.Student_id to be string, got %T", currentRecord["Students.Student_id"])
		}
		if studentID == "" {
			t.Fatal("expected projected Student_id to be non-empty")
		}

		yearValue, ok := currentRecord["Students.Year"].(int)
		if !ok {
			t.Fatalf("expected projected Students.Year to be int, got %T", currentRecord["Students.Year"])
		}
		if _, ok := allowedYears[yearValue]; !ok {
			t.Fatalf("expected projected Year to be one of %v, got %d", years, yearValue)
		}

		currentRecord, err = returnSet.GetNextRecord()
		if err != nil {
			if errors.Is(err, engine.ErrNoMoreRecords) {
				break
			}
			t.Fatalf("GetNextRecord returned error: %v", err)
		}
	}
}

func TestDBWhereStageOrOfFiltersWithSearchUsesOptimizedPath(t *testing.T) {
	tempDir := t.TempDir()
	_, _, err := test_db.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(stepdb.CloseDB)

	dbDef := stepdb.Definition()
	if dbDef == nil {
		t.Fatal("expected opened DBDefinition after creating NIP test database")
	}

	queryText := `Students | where Students.State_or_Country == "Colorado" and Students.Year > 2 or Students.State_or_Country == "Nevada" and Students.Year == 1 | return Students.Student_id, Students.Last_name, Students.Year, Students.State_or_Country`
	query, err := parser.Parse(queryText)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}

	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}
	if len(builtPipeline.Stages) == 0 {
		t.Fatal("expected pipeline to contain at least one stage")
	}

	whereStage, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	if whereStage.Plan.DBPlan == nil {
		t.Fatal("expected DBWhereStage to carry a DB analysis plan")
	}
	if whereStage.Plan.DBPlan.RootClass != pipeline.WhereExprFilterOnInput {
		t.Fatalf("expected root class FilterOnInput (%d), got %d", pipeline.WhereExprFilterOnInput, whereStage.Plan.DBPlan.RootClass)
	}
	if whereStage.Plan.DBPlan.Search == nil {
		t.Fatal("expected search plan to be non-nil for OR-of-filters query")
	}
	if whereStage.Plan.DBPlan.Search.Kind != pipeline.DBWhereSearchOr {
		t.Fatalf("expected OR search kind, got %d", whereStage.Plan.DBPlan.Search.Kind)
	}

	input, err := engine.NewSourceDBTableSet("Students", dbDef)
	if err != nil {
		t.Fatalf("NewSourceDBTableSet returned error: %v", err)
	}

	output, err := engine.ExecuteStage(whereStage, input)
	if err != nil {
		t.Fatalf("ExecuteStage returned error: %v", err)
	}

	workingSet, ok := output.(*engine.DBTableWorkingSet)
	if !ok {
		t.Fatalf("expected output to be *engine.DBTableWorkingSet, got %T", output)
	}

	expectedCount := countStudentsByPredicate(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "NortIdahoPolitechnic", "Students.jsonl"),
		func(rec map[string]interface{}) bool {
			state, _ := rec["State_or_Country"].(string)
			year, _ := rec["Year"].(float64)
			return (state == "Colorado" && year > 2) || (state == "Nevada" && year == 1)
		},
	)
	if len(workingSet.RecordIDs) != expectedCount {
		t.Fatalf("expected %d matching records, got %d", expectedCount, len(workingSet.RecordIDs))
	}
}

func countStudentsByPredicate(t *testing.T, filePath string, predicate func(map[string]interface{}) bool) int {
	t.Helper()

	file, err := os.Open(filePath)
	if err != nil {
		t.Fatalf("failed to open %s: %v", filePath, err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0
	for scanner.Scan() {
		var entry struct {
			Record map[string]interface{} `json:"record"`
		}
		if err := json.Unmarshal(scanner.Bytes(), &entry); err != nil {
			t.Fatalf("failed to parse JSONL line: %v", err)
		}
		if predicate(entry.Record) {
			count++
		}
	}
	if err := scanner.Err(); err != nil {
		t.Fatalf("failed to scan %s: %v", filePath, err)
	}
	return count
}
