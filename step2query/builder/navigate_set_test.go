package builder

import (
	"strings"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

const queryExampleWhereNavigateSetPrimaryKey = `Classes | where Classes.Class_code == "MATH101-01" | navigate set Classes.Enrollment return Students.Student_id, Students.Advisor`
const queryExampleWhereNavigateSetCourse = `Classes | where Classes.Course == "MATH202 " | navigate set Classes.Enrollment return Students.Student_id, Students.Last_name, Students.First_name`

func TestBuildNavigateSetStageDefinitionFromDBState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	navigateSetStage := parseNavigateSetStageFromQueryExample(t, dbDef, queryExampleWhereNavigateSetPrimaryKey)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Classes",
		RecordDef: buildSourceStateDescription(t, dbDef, "Classes").RecordDef,
	}

	builtStage, err := buildNavigateSetStageDefinition(navigateSetStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildNavigateSetStageDefinition returned error: %v", err)
	}

	dbNavigateSetStage, ok := builtStage.(*pipeline.DBNavigateSetStage)
	if !ok {
		t.Fatalf("expected *pipeline.DBNavigateSetStage, got %T", builtStage)
	}
	if dbNavigateSetStage.Kind() != pipeline.StageDBNavigateSet {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageDBNavigateSet, dbNavigateSetStage.Kind())
	}
	if dbNavigateSetStage.Input().Kind != pipeline.StateDBTableWorkingSet {
		t.Fatalf("expected input state kind %d, got %d", pipeline.StateDBTableWorkingSet, dbNavigateSetStage.Input().Kind)
	}
	if dbNavigateSetStage.Output().Kind != pipeline.StateReturnWorkingSet {
		t.Fatalf("expected output state kind %d, got %d", pipeline.StateReturnWorkingSet, dbNavigateSetStage.Output().Kind)
	}
	if dbNavigateSetStage.Plan.SourceTable != "Classes" {
		t.Fatalf("expected source table Classes, got %q", dbNavigateSetStage.Plan.SourceTable)
	}
	if dbNavigateSetStage.Plan.TargetTable != "Students" {
		t.Fatalf("expected target table Students, got %q", dbNavigateSetStage.Plan.TargetTable)
	}
	if dbNavigateSetStage.Plan.SetName != "Enrollment" {
		t.Fatalf("expected set name Enrollment, got %q", dbNavigateSetStage.Plan.SetName)
	}
	assertReturnRecordDefinition(t, dbNavigateSetStage.Output().RecordDef, []string{"Students.Student_id", "Students.Advisor"})
	if len(dbNavigateSetStage.Plan.ReturnItems) != 2 {
		t.Fatalf("expected 2 return items, got %d", len(dbNavigateSetStage.Plan.ReturnItems))
	}
}

func TestBuildNavigateSetStageDefinitionRejectsReturnWorkingSetInput(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	navigateSetStage := parseNavigateSetStageFromQueryExample(t, dbDef, queryExampleWhereNavigateSetPrimaryKey)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Classes",
		RecordDef: buildSourceStateDescription(t, dbDef, "Classes").RecordDef,
	}

	_, err := buildNavigateSetStageDefinition(navigateSetStage, currentState, dbDef)
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), "navigate set is not supported for return working set input") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestBuildPipelineWhereNavigateSetQueryExample(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := parser.Parse(queryExampleWhereNavigateSetCourse)
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
	if builtPipeline.InitialState.TableName != "Classes" {
		t.Fatalf("expected initial table Classes, got %q", builtPipeline.InitialState.TableName)
	}
	if len(builtPipeline.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(builtPipeline.Stages))
	}
	if _, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage); !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	navigateSetStage, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateSetStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateSetStage, got %T", builtPipeline.Stages[1])
	}
	assertReturnRecordDefinition(t, navigateSetStage.Output().RecordDef, []string{"Students.Student_id", "Students.Last_name", "Students.First_name"})
}

func parseNavigateSetStageFromQueryExample(t *testing.T, dbDef *stepdb.DBDefinition, input string) *parser.NavigateSetStage {
	t.Helper()
	query, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
	navigateSetStage, ok := query.Stages[len(query.Stages)-1].(*parser.NavigateSetStage)
	if !ok {
		t.Fatalf("expected last stage to be *parser.NavigateSetStage, got %T", query.Stages[len(query.Stages)-1])
	}
	return navigateSetStage
}
