package builder

import (
	"path/filepath"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

const queryExampleWherePrimaryKey = `Students | where Students.Student_id == "NIP2409002" | return Students.First_name, Students.Last_name`

func TestBuildWhereStageDefinitionFromSourceState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	whereStage := parseWhereStageFromQueryExample(t, dbDef, queryExampleWherePrimaryKey)
	currentState := buildSourceStateDescription(t, dbDef, "Students")

	builtStage, err := buildWhereStageDefinition(whereStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildWhereStageDefinition returned error: %v", err)
	}

	dbWhereStage, ok := builtStage.(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected *pipeline.DBWhereStage, got %T", builtStage)
	}
	if dbWhereStage.Kind() != pipeline.StageDBWhere {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageDBWhere, dbWhereStage.Kind())
	}
	if dbWhereStage.Input().Kind != pipeline.StateSourceDBTableSet {
		t.Fatalf("expected input state kind %d, got %d", pipeline.StateSourceDBTableSet, dbWhereStage.Input().Kind)
	}
	if dbWhereStage.Output().Kind != pipeline.StateDBTableWorkingSet {
		t.Fatalf("expected output state kind %d, got %d", pipeline.StateDBTableWorkingSet, dbWhereStage.Output().Kind)
	}
	if dbWhereStage.Output().TableName != "Students" {
		t.Fatalf("expected output table Students, got %q", dbWhereStage.Output().TableName)
	}
	if dbWhereStage.Plan.ExecutionMode != "db-scan" {
		t.Fatalf("expected execution mode db-scan, got %q", dbWhereStage.Plan.ExecutionMode)
	}
	if dbWhereStage.Plan.DBPlan == nil {
		t.Fatal("expected DB where analysis plan, got nil")
	}
	if dbWhereStage.Plan.DBPlan.RootClass != pipeline.WhereExprSearchable {
		t.Fatalf("expected DB where root class %d, got %d", pipeline.WhereExprSearchable, dbWhereStage.Plan.DBPlan.RootClass)
	}
	if dbWhereStage.Plan.DBPlan.Search == nil {
		t.Fatal("expected searchable DB where plan, got nil")
	}
	if dbWhereStage.Plan.DBPlan.Search.Kind != pipeline.DBWhereSearchPrimaryKeyExact {
		t.Fatalf("expected primary-key search plan kind %d, got %d", pipeline.DBWhereSearchPrimaryKeyExact, dbWhereStage.Plan.DBPlan.Search.Kind)
	}
	if dbWhereStage.Output().RecordDef.FieldIndex["Student_id"] != currentState.RecordDef.FieldIndex["Student_id"] {
		t.Fatalf("expected output record definition to preserve field index for Student_id")
	}
}

func TestBuildWhereStageDefinitionFromReturnState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	whereStage := parseWhereStageFromQueryExample(t, dbDef, queryExampleWherePrimaryKey)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Students",
		RecordDef: buildSourceStateDescription(t, dbDef, "Students").RecordDef,
	}

	builtStage, err := buildWhereStageDefinition(whereStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildWhereStageDefinition returned error: %v", err)
	}

	memoryWhereStage, ok := builtStage.(*pipeline.MemoryWhereStage)
	if !ok {
		t.Fatalf("expected *pipeline.MemoryWhereStage, got %T", builtStage)
	}
	if memoryWhereStage.Kind() != pipeline.StageMemoryWhere {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageMemoryWhere, memoryWhereStage.Kind())
	}
	if memoryWhereStage.Output().Kind != pipeline.StateReturnWorkingSet {
		t.Fatalf("expected output state kind %d, got %d", pipeline.StateReturnWorkingSet, memoryWhereStage.Output().Kind)
	}
	if memoryWhereStage.Output().TableName != currentState.TableName {
		t.Fatalf("expected output table %q, got %q", currentState.TableName, memoryWhereStage.Output().TableName)
	}
	if memoryWhereStage.Plan.ExecutionMode != "memory-scan" {
		t.Fatalf("expected execution mode memory-scan, got %q", memoryWhereStage.Plan.ExecutionMode)
	}
	if memoryWhereStage.Plan.DBPlan != nil {
		t.Fatalf("expected memory where stage to have no DB analysis plan, got %+v", memoryWhereStage.Plan.DBPlan)
	}
	if memoryWhereStage.Output().RecordDef.NoFields != currentState.RecordDef.NoFields {
		t.Fatalf("expected output record definition to preserve number of fields")
	}
}

func TestBuildWhereStageDefinitionFromDBWorkingSetDoesNotAnalyze(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	whereStage := parseWhereStageFromQueryExample(t, dbDef, queryExampleWherePrimaryKey)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Students",
		RecordDef: buildSourceStateDescription(t, dbDef, "Students").RecordDef,
	}

	builtStage, err := buildWhereStageDefinition(whereStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildWhereStageDefinition returned error: %v", err)
	}

	dbWhereStage, ok := builtStage.(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected *pipeline.DBWhereStage, got %T", builtStage)
	}
	if dbWhereStage.Plan.DBPlan != nil {
		t.Fatalf("expected DB working-set where stage to have no analysis plan, got %+v", dbWhereStage.Plan.DBPlan)
	}
	if dbWhereStage.Input().Kind != pipeline.StateDBTableWorkingSet {
		t.Fatalf("expected input state kind %d, got %d", pipeline.StateDBTableWorkingSet, dbWhereStage.Input().Kind)
	}
	if dbWhereStage.Output().Kind != pipeline.StateDBTableWorkingSet {
		t.Fatalf("expected output state kind %d, got %d", pipeline.StateDBTableWorkingSet, dbWhereStage.Output().Kind)
	}
}

func TestBuildReturnStageDefinitionFromDBState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	returnStage := parseReturnStageFromQueryExample(t, dbDef, queryExampleWherePrimaryKey)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Students",
		RecordDef: buildSourceStateDescription(t, dbDef, "Students").RecordDef,
	}

	builtStage, err := buildReturnStageDefinition(returnStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildReturnStageDefinition returned error: %v", err)
	}

	dbReturnStage, ok := builtStage.(*pipeline.DBReturnStage)
	if !ok {
		t.Fatalf("expected *pipeline.DBReturnStage, got %T", builtStage)
	}
	if dbReturnStage.Kind() != pipeline.StageDBReturn {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageDBReturn, dbReturnStage.Kind())
	}
	if dbReturnStage.Output().Kind != pipeline.StateReturnWorkingSet {
		t.Fatalf("expected output state kind %d, got %d", pipeline.StateReturnWorkingSet, dbReturnStage.Output().Kind)
	}
	assertReturnRecordDefinition(t, dbReturnStage.Output().RecordDef, []string{"Students.First_name", "Students.Last_name"})
	if len(dbReturnStage.Plan.Items) != 2 {
		t.Fatalf("expected 2 return items, got %d", len(dbReturnStage.Plan.Items))
	}
}

func TestBuildReturnStageDefinitionFromReturnState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	returnStage := parseReturnStageFromQueryExample(t, dbDef, queryExampleWherePrimaryKey)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Students",
		RecordDef: buildSourceStateDescription(t, dbDef, "Students").RecordDef,
	}

	builtStage, err := buildReturnStageDefinition(returnStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildReturnStageDefinition returned error: %v", err)
	}

	memoryReturnStage, ok := builtStage.(*pipeline.MemoryReturnStage)
	if !ok {
		t.Fatalf("expected *pipeline.MemoryReturnStage, got %T", builtStage)
	}
	if memoryReturnStage.Kind() != pipeline.StageMemoryReturn {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageMemoryReturn, memoryReturnStage.Kind())
	}
	if memoryReturnStage.Output().Kind != pipeline.StateReturnWorkingSet {
		t.Fatalf("expected output state kind %d, got %d", pipeline.StateReturnWorkingSet, memoryReturnStage.Output().Kind)
	}
	assertReturnRecordDefinition(t, memoryReturnStage.Output().RecordDef, []string{"Students.First_name", "Students.Last_name"})
}

func TestBuildPipelineWhereReturnQueryExample(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := parser.Parse(queryExampleWherePrimaryKey)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}

	builtPipeline, err := BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	if builtPipeline.InitialState.Kind != pipeline.StateSourceDBTableSet {
		t.Fatalf("expected initial state kind %d, got %d", pipeline.StateSourceDBTableSet, builtPipeline.InitialState.Kind)
	}
	if builtPipeline.InitialState.TableName != "Students" {
		t.Fatalf("expected initial table Students, got %q", builtPipeline.InitialState.TableName)
	}
	if len(builtPipeline.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(builtPipeline.Stages))
	}
	if _, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage); !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	returnStage, ok := builtPipeline.Stages[1].(*pipeline.DBReturnStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBReturnStage, got %T", builtPipeline.Stages[1])
	}
	assertReturnRecordDefinition(t, returnStage.Output().RecordDef, []string{"Students.First_name", "Students.Last_name"})
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

func parseWhereStageFromQueryExample(t *testing.T, dbDef *stepdb.DBDefinition, input string) *parser.WhereStage {
	t.Helper()
	query, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
	whereStage, ok := query.Stages[0].(*parser.WhereStage)
	if !ok {
		t.Fatalf("expected first stage to be *parser.WhereStage, got %T", query.Stages[0])
	}
	return whereStage
}

func parseReturnStageFromQueryExample(t *testing.T, dbDef *stepdb.DBDefinition, input string) *parser.ReturnStage {
	t.Helper()
	query, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
	returnStage, ok := query.Stages[len(query.Stages)-1].(*parser.ReturnStage)
	if !ok {
		t.Fatalf("expected last stage to be *parser.ReturnStage, got %T", query.Stages[len(query.Stages)-1])
	}
	return returnStage
}

func buildSourceStateDescription(t *testing.T, dbDef *stepdb.DBDefinition, tableName string) pipeline.StateDescription {
	t.Helper()
	tableIndex, exists := dbDef.TableIndex[tableName]
	if !exists {
		t.Fatalf("table %s does not exist", tableName)
	}
	table := dbDef.Tables[tableIndex]
	return pipeline.StateDescription{
		Kind:      pipeline.StateSourceDBTableSet,
		TableName: tableName,
		RecordDef: buildRecordDefinition(table),
	}
}

func assertReturnRecordDefinition(t *testing.T, recordDef pipeline.RecordDefinition, wantFields []string) {
	t.Helper()
	if recordDef.NoFields != len(wantFields) {
		t.Fatalf("expected %d output fields, got %d", len(wantFields), recordDef.NoFields)
	}
	for index, fieldName := range wantFields {
		if recordDef.Fields[index].Name != fieldName {
			t.Fatalf("expected output field %d to be %q, got %q", index, fieldName, recordDef.Fields[index].Name)
		}
		if recordDef.FieldIndex[fieldName] != index {
			t.Fatalf("expected field index for %q to be %d, got %d", fieldName, index, recordDef.FieldIndex[fieldName])
		}
	}
	if recordDef.PrimaryKey != -1 {
		t.Fatalf("expected return working set to have no primary key, got %d", recordDef.PrimaryKey)
	}
}
