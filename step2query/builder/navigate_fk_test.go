package builder

import (
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

const queryExampleWhereNavigateFK = `Classes | where Classes.Course == "MATH202 " | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.Works_for, Teachers.First_name, Teachers.Last_name, Teachers.Office`
const queryExampleWhereTwoNavigateFK = `Classes | where Classes.Course == "MATH202 " | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.Works_for, Teachers.First_name, Teachers.Last_name, Teachers.Office | navigate Departments on Teachers.Works_for == Departments.Department_code return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Departments.Building_name, Teachers.Office`

func TestBuildNavigateFKStageDefinitionFromDBState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	navigateFKStage := parseNavigateFKStageFromQueryExample(t, dbDef, queryExampleWhereNavigateFK, 2)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Classes",
		RecordDef: buildSourceStateDescription(t, dbDef, "Classes").RecordDef,
	}

	builtStage, err := buildNavigateFKStageDefinition(navigateFKStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildNavigateFKStageDefinition returned error: %v", err)
	}

	dbNavigateFKStage, ok := builtStage.(*pipeline.DBNavigateFKStage)
	if !ok {
		t.Fatalf("expected *pipeline.DBNavigateFKStage, got %T", builtStage)
	}
	if dbNavigateFKStage.Kind() != pipeline.StageDBNavigateFK {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageDBNavigateFK, dbNavigateFKStage.Kind())
	}
	if dbNavigateFKStage.Plan.SourceTable != "Classes" {
		t.Fatalf("expected source table Classes, got %q", dbNavigateFKStage.Plan.SourceTable)
	}
	if dbNavigateFKStage.Plan.TargetTable != "Teachers" {
		t.Fatalf("expected target table Teachers, got %q", dbNavigateFKStage.Plan.TargetTable)
	}
	if dbNavigateFKStage.Plan.ForeignKey.TableName != "Classes" || dbNavigateFKStage.Plan.ForeignKey.FieldName != "Teacher" {
		t.Fatalf("unexpected foreign key ref: %+v", dbNavigateFKStage.Plan.ForeignKey)
	}
	if dbNavigateFKStage.Plan.PrimaryKey.TableName != "Teachers" || dbNavigateFKStage.Plan.PrimaryKey.FieldName != "Employee_id" {
		t.Fatalf("unexpected primary key ref: %+v", dbNavigateFKStage.Plan.PrimaryKey)
	}
	assertReturnRecordDefinition(t, dbNavigateFKStage.Output().RecordDef, []string{"Classes.Class_code", "Teachers.Works_for", "Teachers.First_name", "Teachers.Last_name", "Teachers.Office"})
}

func TestBuildNavigateFKStageDefinitionFromReturnState(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	navigateFKStage := parseNavigateFKStageFromQueryExample(t, dbDef, queryExampleWhereTwoNavigateFK, 3)
	currentState := pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Teachers",
		RecordDef: pipeline.RecordDefinition{
			NoFields:   5,
			PrimaryKey: -1,
			Fields: []*pipeline.FieldDef{
				{Name: "Classes.Class_code", SourceTableName: "Classes", SourceFieldName: "Class_code", Type: pipeline.CHAR},
				{Name: "Teachers.Works_for", SourceTableName: "Teachers", SourceFieldName: "Works_for", Type: pipeline.CHAR, IsForeignKey: true, ForeignKeyTable: "Departments"},
				{Name: "Teachers.First_name", SourceTableName: "Teachers", SourceFieldName: "First_name", Type: pipeline.STRING},
				{Name: "Teachers.Last_name", SourceTableName: "Teachers", SourceFieldName: "Last_name", Type: pipeline.STRING},
				{Name: "Teachers.Office", SourceTableName: "Teachers", SourceFieldName: "Office", Type: pipeline.CHAR},
			},
			FieldIndex: map[string]int{
				"Classes.Class_code":  0,
				"Teachers.Works_for":  1,
				"Teachers.First_name": 2,
				"Teachers.Last_name":  3,
				"Teachers.Office":     4,
			},
			QualifiedFieldIndex: map[string]int{
				"Classes.Class_code":  0,
				"Teachers.Works_for":  1,
				"Teachers.First_name": 2,
				"Teachers.Last_name":  3,
				"Teachers.Office":     4,
			},
		},
	}

	builtStage, err := buildNavigateFKStageDefinition(navigateFKStage, currentState, dbDef)
	if err != nil {
		t.Fatalf("buildNavigateFKStageDefinition returned error: %v", err)
	}

	memoryNavigateFKStage, ok := builtStage.(*pipeline.MemoryNavigateFKStage)
	if !ok {
		t.Fatalf("expected *pipeline.MemoryNavigateFKStage, got %T", builtStage)
	}
	if memoryNavigateFKStage.Kind() != pipeline.StageMemoryNavigateFK {
		t.Fatalf("expected stage kind %d, got %d", pipeline.StageMemoryNavigateFK, memoryNavigateFKStage.Kind())
	}
	if memoryNavigateFKStage.Plan.ForeignKey.TableName != "Teachers" || memoryNavigateFKStage.Plan.ForeignKey.FieldName != "Works_for" {
		t.Fatalf("unexpected foreign key ref: %+v", memoryNavigateFKStage.Plan.ForeignKey)
	}
	if memoryNavigateFKStage.Plan.PrimaryKey.TableName != "Departments" || memoryNavigateFKStage.Plan.PrimaryKey.FieldName != "Department_code" {
		t.Fatalf("unexpected primary key ref: %+v", memoryNavigateFKStage.Plan.PrimaryKey)
	}
	assertReturnRecordDefinition(t, memoryNavigateFKStage.Output().RecordDef, []string{"Classes.Class_code", "Teachers.First_name", "Teachers.Last_name", "Departments.Building_name", "Teachers.Office"})
}

func TestBuildPipelineWhereTwoNavigateFKQueryExample(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := parser.Parse(queryExampleWhereTwoNavigateFK)
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

	if len(builtPipeline.Stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(builtPipeline.Stages))
	}
	if _, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage); !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	if _, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateFKStage); !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateFKStage, got %T", builtPipeline.Stages[1])
	}
	lastStage, ok := builtPipeline.Stages[2].(*pipeline.MemoryNavigateFKStage)
	if !ok {
		t.Fatalf("expected third stage to be *pipeline.MemoryNavigateFKStage, got %T", builtPipeline.Stages[2])
	}
	assertReturnRecordDefinition(t, lastStage.Output().RecordDef, []string{"Classes.Class_code", "Teachers.First_name", "Teachers.Last_name", "Departments.Building_name", "Teachers.Office"})
}

func parseNavigateFKStageFromQueryExample(t *testing.T, dbDef *stepdb.DBDefinition, input string, stageNumber int) *parser.NavigateFKStage {
	t.Helper()
	query, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
	stageIndex := stageNumber - 1
	if stageIndex < 0 || stageIndex >= len(query.Stages) {
		t.Fatalf("requested stage number %d out of range for query with %d stages", stageNumber, len(query.Stages))
	}
	navigateFKStage, ok := query.Stages[stageIndex].(*parser.NavigateFKStage)
	if !ok {
		t.Fatalf("expected stage %d to be *parser.NavigateFKStage, got %T", stageNumber, query.Stages[stageIndex])
	}
	return navigateFKStage
}
