package engine

import (
	"path/filepath"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/builder"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
	"github.com/kozwoj/step2query/test_db"
)

const queryExampleWhereNavigateFK = `Classes | where Classes.Course == "MATH202 " | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.Works_for, Teachers.First_name, Teachers.Last_name, Teachers.Office`

const queryExampleWhereReturnNavigateFK = `Classes | where Classes.Course == "MATH202 " | return Classes.Class_code, Classes.Teacher, Classes.Course | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.Works_for, Teachers.First_name, Teachers.Last_name, Teachers.Office`

func TestBuildNavigateOutputRecordClassesToTeachersExample(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseValidatedQuery(t, dbDef, queryExampleWhereNavigateFK)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}
	if len(builtPipeline.Stages) != 2 {
		t.Fatalf("expected pipeline to contain 2 stages, got %d", len(builtPipeline.Stages))
	}

	navigateStage, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateFKStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateFKStage, got %T", builtPipeline.Stages[1])
	}

	classesDef := buildRecordDefinition(dbDef.Tables[dbDef.TableIndex["Classes"]])
	teachersDef := buildRecordDefinition(dbDef.Tables[dbDef.TableIndex["Teachers"]])

	// These rows are copied from the NIP JSONL fixture files for the MATH202-01 class and its teacher.
	inputRecord := map[string]interface{}{
		"Class_code": "MATH202-01",
		"Teacher":    "2014T283",
		"Course":     "MATH202 ",
	}
	targetRecord := map[string]interface{}{
		"Employee_id": "2014T283",
		"Works_for":   "MATH    ",
		"First_name":  "Ahmed",
		"Last_name":   "Hassan",
		"Office":      "205       ",
	}

	outputDef := navigateStage.Output().RecordDef
	outputRecord, err := buildNavigateOutputRecord(navigateStage.Plan.ReturnItems, classesDef, teachersDef, outputDef, inputRecord, targetRecord)
	if err != nil {
		t.Fatalf("buildNavigateOutputRecord returned error: %v", err)
	}

	assertOutputRecordValue(t, outputRecord, "Classes.Class_code", "MATH202-01")
	assertOutputRecordValue(t, outputRecord, "Teachers.Works_for", "MATH    ")
	assertOutputRecordValue(t, outputRecord, "Teachers.First_name", "Ahmed")
	assertOutputRecordValue(t, outputRecord, "Teachers.Last_name", "Hassan")
	assertOutputRecordValue(t, outputRecord, "Teachers.Office", "205       ")
	if len(outputRecord) != outputDef.NoFields {
		t.Fatalf("expected output record to have %d fields, got %d", outputDef.NoFields, len(outputRecord))
	}
}

func TestLoadNavigateFKTargetRecordClassesToTeachers(t *testing.T) {
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

	query := parseValidatedQuery(t, dbDef, queryExampleWhereNavigateFK)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	navigateStage, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateFKStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateFKStage, got %T", builtPipeline.Stages[1])
	}

	classesDef := buildRecordDefinition(dbDef.Tables[dbDef.TableIndex["Classes"]])

	// Simulate an input row from the Classes table with FK Teacher pointing at Teachers.Employee_id.
	inputRecord := map[string]interface{}{
		"Class_code": "MATH202-01",
		"Teacher":    "2014T283",
		"Course":     "MATH202 ",
	}

	targetRecord, err := loadNavigateFKTargetRecord(navigateStage.Plan, classesDef, inputRecord, dbDef)
	if err != nil {
		t.Fatalf("loadNavigateFKTargetRecord returned error: %v", err)
	}

	// The target Teachers record should have the matching Employee_id and its known field values.
	assertOutputRecordValue(t, targetRecord, "Employee_id", "2014T283")
	assertOutputRecordValue(t, targetRecord, "First_name", "Ahmed")
	assertOutputRecordValue(t, targetRecord, "Last_name", "Hassan")
}

func TestExecuteNavigateFKWithDBBackedInput(t *testing.T) {
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

	query := parseValidatedQuery(t, dbDef, queryExampleWhereNavigateFK)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	output, err := ExecutePipeline(builtPipeline, dbDef)
	if err != nil {
		t.Fatalf("ExecutePipeline returned error: %v", err)
	}

	returnSet, ok := output.(*ReturnWorkingSet)
	if !ok {
		t.Fatalf("expected pipeline output to be *ReturnWorkingSet, got %T", output)
	}
	if returnSet.Size() != 2 {
		t.Fatalf("expected 2 output rows, got %d", returnSet.Size())
	}

	assertNavigateFKOutputRows(t, returnSet)
}

func TestExecuteNavigateFKWithInMemoryInput(t *testing.T) {
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

	query := parseValidatedQuery(t, dbDef, queryExampleWhereReturnNavigateFK)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	// Verify the navigate stage receives in-memory input.
	if len(builtPipeline.Stages) != 3 {
		t.Fatalf("expected pipeline to contain 3 stages, got %d", len(builtPipeline.Stages))
	}
	navigateStage, ok := builtPipeline.Stages[2].(*pipeline.MemoryNavigateFKStage)
	if !ok {
		t.Fatalf("expected third stage to be *pipeline.MemoryNavigateFKStage, got %T", builtPipeline.Stages[2])
	}
	if navigateStage.Input().Kind != pipeline.StateReturnWorkingSet {
		t.Fatalf("expected navigate stage input kind ReturnWorkingSet, got %d", navigateStage.Input().Kind)
	}

	output, err := ExecutePipeline(builtPipeline, dbDef)
	if err != nil {
		t.Fatalf("ExecutePipeline returned error: %v", err)
	}

	returnSet, ok := output.(*ReturnWorkingSet)
	if !ok {
		t.Fatalf("expected pipeline output to be *ReturnWorkingSet, got %T", output)
	}
	if returnSet.Size() != 2 {
		t.Fatalf("expected 2 output rows, got %d", returnSet.Size())
	}

	assertNavigateFKOutputRows(t, returnSet)
}

func assertNavigateFKOutputRows(t *testing.T, returnSet *ReturnWorkingSet) {
	t.Helper()

	expectedRows := []struct {
		classCode string
		worksFor  string
		firstName string
		lastName  string
		office    string
	}{
		{"MATH202-01", "MATH    ", "Ahmed", "Hassan", "205       "},
		{"MATH202-02", "MATH    ", "Yuki", "Tanaka", "208       "},
	}

	matched := make([]bool, len(expectedRows))

	currentRecord, err := returnSet.GetFirstRecord()
	if err != nil {
		t.Fatalf("GetFirstRecord returned error: %v", err)
	}
	rowCount := 0
	for currentRecord != nil {
		rowCount++
		classCode, _ := currentRecord["Classes.Class_code"].(string)
		for i, exp := range expectedRows {
			if !matched[i] && classCode == exp.classCode {
				matched[i] = true
				assertOutputRecordValue(t, currentRecord, "Teachers.Works_for", exp.worksFor)
				assertOutputRecordValue(t, currentRecord, "Teachers.First_name", exp.firstName)
				assertOutputRecordValue(t, currentRecord, "Teachers.Last_name", exp.lastName)
				assertOutputRecordValue(t, currentRecord, "Teachers.Office", exp.office)
			}
		}

		currentRecord, err = returnSet.GetNextRecord()
		if err != nil {
			if err.Error() == ErrNoMoreRecords.Error() {
				break
			}
			t.Fatalf("GetNextRecord returned error: %v", err)
		}
	}

	for i, exp := range expectedRows {
		if !matched[i] {
			t.Fatalf("expected row with Class_code %q not found in output", exp.classCode)
		}
	}
}

const queryExampleNavigateFKThenMemoryWhere = `Classes | where Classes.Course == "MATH202 " | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Teachers.Office | where Teachers.Last_name == "Hassan" | return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Teachers.Office`

func TestExecuteMemoryWhereAfterNavigateFK(t *testing.T) {
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

	query := parseValidatedQuery(t, dbDef, queryExampleNavigateFKThenMemoryWhere)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	// Verify the pipeline has 4 stages and the third is a MemoryWhereStage.
	if len(builtPipeline.Stages) != 4 {
		t.Fatalf("expected pipeline to contain 4 stages, got %d", len(builtPipeline.Stages))
	}
	memoryWhereStage, ok := builtPipeline.Stages[2].(*pipeline.MemoryWhereStage)
	if !ok {
		t.Fatalf("expected third stage to be *pipeline.MemoryWhereStage, got %T", builtPipeline.Stages[2])
	}
	if memoryWhereStage.Input().Kind != pipeline.StateReturnWorkingSet {
		t.Fatalf("expected memory where stage input kind ReturnWorkingSet, got %d", memoryWhereStage.Input().Kind)
	}

	output, err := ExecutePipeline(builtPipeline, dbDef)
	if err != nil {
		t.Fatalf("ExecutePipeline returned error: %v", err)
	}

	returnSet, ok := output.(*ReturnWorkingSet)
	if !ok {
		t.Fatalf("expected pipeline output to be *ReturnWorkingSet, got %T", output)
	}

	// The navigate FK produces 2 rows (Hassan, Tanaka). The memory where keeps only Hassan.
	if returnSet.Size() != 1 {
		t.Fatalf("expected 1 output row, got %d", returnSet.Size())
	}

	row, err := returnSet.GetFirstRecord()
	if err != nil {
		t.Fatalf("GetFirstRecord returned error: %v", err)
	}

	assertOutputRecordValue(t, row, "Classes.Class_code", "MATH202-01")
	assertOutputRecordValue(t, row, "Teachers.First_name", "Ahmed")
	assertOutputRecordValue(t, row, "Teachers.Last_name", "Hassan")
	assertOutputRecordValue(t, row, "Teachers.Office", "205       ")
}

func loadCollegeDefinition(t *testing.T) *stepdb.DBDefinition {
	t.Helper()

	schemaFile := filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl")
	dbDef, err := stepdb.NewDBDefinitionFromSchema(schemaFile)
	if err != nil {
		t.Fatalf("failed to load DBDefinition from schema: %v", err)
	}

	return dbDef
}

func parseValidatedQuery(t *testing.T, dbDef *stepdb.DBDefinition, input string) *parser.Query {
	t.Helper()

	query, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}

	return query
}

func assertOutputRecordValue(t *testing.T, record map[string]interface{}, fieldName string, want interface{}) {
	t.Helper()

	got, exists := record[fieldName]
	if !exists {
		t.Fatalf("expected output record field %q to exist", fieldName)
	}
	if got != want {
		t.Fatalf("expected output record field %q to be %#v, got %#v", fieldName, want, got)
	}
}
