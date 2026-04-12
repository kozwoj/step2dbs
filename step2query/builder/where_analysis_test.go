package builder

import (
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

func TestGetBDWherePlanPrimaryKeyExactIsSearchable(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Student_id == "NIP2409002" | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprSearchable)
	if plan.Search.Residual != nil {
		t.Fatalf("expected no residual predicate, got %T", plan.Search.Residual)
	}
	assertSearchLeaf(t, plan.Search, pipeline.DBWhereSearchPrimaryKeyExact, "Students", "Student_id", inputState.RecordDef.PrimaryKey, pipeline.CHAR)
}

func TestGetBDWherePlanStringExactIsSearchable(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name == "Perry" | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprSearchable)
	assertSearchLeaf(t, plan.Search, pipeline.DBWhereSearchStringExact, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
	stringLiteral, ok := plan.Search.Literal.(*parser.StringLiteral)
	if !ok {
		t.Fatalf("expected string literal, got %T", plan.Search.Literal)
	}
	if stringLiteral.Value != "Perry" {
		t.Fatalf("expected string literal value Perry, got %q", stringLiteral.Value)
	}
}

func TestGetBDWherePlanStringPrefixIsSearchable(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name like "Je*" | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprSearchable)
	assertSearchLeaf(t, plan.Search, pipeline.DBWhereSearchStringPrefix, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
	if plan.Search.Prefix != "Je" {
		t.Fatalf("expected prefix Je, got %q", plan.Search.Prefix)
	}
}

func TestGetBDWherePlanNumericLeafNeedsFiltering(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Year >= 2 | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprFilterOnInput)
	if plan.Search != nil {
		t.Fatalf("expected no search plan, got %+v", plan.Search)
	}
}

func TestGetBDWherePlanAndBuildsCandidatePlusResidual(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name == "Perry" and Students.Year >= 2 | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprFilterOnInput)
	assertSearchLeaf(t, plan.Search, pipeline.DBWhereSearchStringExact, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
	if plan.Search.Residual == nil {
		t.Fatal("expected search node to carry residual")
	}
	andExpr := whereStage.Expr.(*parser.AndExpr)
	if plan.Search.Residual != andExpr.Right {
		t.Fatalf("expected search residual to be the non-searchable AND branch")
	}
}

func TestGetBDWherePlanOrOfSearchablesStaysSearchable(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name == "Perry" or Students.Last_name == "Jackson" | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprSearchable)
	if plan.Search == nil {
		t.Fatal("expected search plan, got nil")
	}
	if plan.Search.Kind != pipeline.DBWhereSearchOr {
		t.Fatalf("expected OR search kind %d, got %d", pipeline.DBWhereSearchOr, plan.Search.Kind)
	}
	assertSearchLeaf(t, plan.Search.Left, pipeline.DBWhereSearchStringExact, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
	assertSearchLeaf(t, plan.Search.Right, pipeline.DBWhereSearchStringExact, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
}

func TestGetBDWherePlanOrWithFilterNeedsFullInput(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name == "Perry" or Students.Year >= 2 | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprNeedsFullInput)
	if plan.Search != nil {
		t.Fatalf("expected no search plan, got %+v", plan.Search)
	}
}

func TestGetBDWherePlanAndDoesNotPropagateNeedsFullInput(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where (Students.Last_name == "Perry" or Students.Year >= 2) and Students.Student_id == "NIP2409002" | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprFilterOnInput)
	assertSearchLeaf(t, plan.Search, pipeline.DBWhereSearchPrimaryKeyExact, "Students", "Student_id", inputState.RecordDef.PrimaryKey, pipeline.CHAR)
	if plan.Search.Residual == nil {
		t.Fatal("expected search node to carry residual")
	}
	andExpr := whereStage.Expr.(*parser.AndExpr)
	if plan.Search.Residual != andExpr.Left {
		t.Fatalf("expected search residual to be the non-searchable AND branch (the OR expression)")
	}
}

func TestGetBDWherePlanNotNeedsFullInput(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where not (Students.Last_name == "Perry") | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprNeedsFullInput)
	if plan.Search != nil {
		t.Fatalf("expected no search plan, got %+v", plan.Search)
	}
}

func TestGetBDWherePlanOrOfFiltersWithSearchProducesOrNode(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name == "Perry" and Students.Year >= 2 or Students.Last_name == "Jackson" and Students.Year == 1 | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err != nil {
		t.Fatalf("GetBDWherePlan returned error: %v", err)
	}

	assertAnalysisRootClass(t, plan, pipeline.WhereExprFilterOnInput)
	if plan.Search == nil {
		t.Fatal("expected search plan, got nil")
	}
	if plan.Search.Kind != pipeline.DBWhereSearchOr {
		t.Fatalf("expected OR search kind, got %d", plan.Search.Kind)
	}

	// Left branch: Last_name == "Perry" with Residual = Year >= 2
	leftSearch := plan.Search.Left
	assertSearchLeaf(t, leftSearch, pipeline.DBWhereSearchStringExact, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
	if leftSearch.Residual == nil {
		t.Fatal("expected left search branch to carry residual")
	}

	// Right branch: Last_name == "Jackson" with Residual = Year == 1
	rightSearch := plan.Search.Right
	assertSearchLeaf(t, rightSearch, pipeline.DBWhereSearchStringExact, "Students", "Last_name", inputState.RecordDef.FieldIndex["Last_name"], pipeline.STRING)
	if rightSearch.Residual == nil {
		t.Fatal("expected right search branch to carry residual")
	}
}

func TestGetBDWherePlanRejectsWorkingSetInput(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseWhereAnalysisQuery(t, dbDef, `Students | where Students.Last_name == "Perry" | return Students.First_name`)
	whereStage := query.Stages[0].(*parser.WhereStage)
	inputState := buildSourceStateDescription(t, dbDef, "Students")
	inputState.Kind = pipeline.StateDBTableWorkingSet

	plan, err := GetBDWherePlan(whereStage.Expr, inputState)
	if err == nil {
		t.Fatal("expected error for DBTableWorkingSet input, got nil")
	}
	if plan != nil {
		t.Fatalf("expected nil plan for DBTableWorkingSet input, got %+v", plan)
	}
	if err.Error() != "DB where analysis is only supported for source DB table input" {
		t.Fatalf("unexpected error: %v", err)
	}
}

func assertAnalysisRootClass(t *testing.T, plan *pipeline.DBWhereAnalysisPlan, want pipeline.WhereExprClass) {
	t.Helper()
	if plan == nil {
		t.Fatal("expected analysis plan, got nil")
	}
	if plan.RootClass != want {
		t.Fatalf("expected root class %d, got %d", want, plan.RootClass)
	}
}

func assertSearchLeaf(t *testing.T, search *pipeline.DBWhereSearchPlan, wantKind pipeline.DBWhereSearchKind, wantTable string, wantField string, wantPosition int, wantType pipeline.FieldType) {
	t.Helper()
	if search == nil {
		t.Fatal("expected search plan, got nil")
	}
	if search.Kind != wantKind {
		t.Fatalf("expected search kind %d, got %d", wantKind, search.Kind)
	}
	if search.Field.TableName != wantTable {
		t.Fatalf("expected search field table %q, got %q", wantTable, search.Field.TableName)
	}
	if search.Field.FieldName != wantField {
		t.Fatalf("expected search field name %q, got %q", wantField, search.Field.FieldName)
	}
	if search.Field.Position != wantPosition {
		t.Fatalf("expected search field position %d, got %d", wantPosition, search.Field.Position)
	}
	if search.Field.Type != wantType {
		t.Fatalf("expected search field type %d, got %d", wantType, search.Field.Type)
	}
}

func parseWhereAnalysisQuery(t *testing.T, dbDef *stepdb.DBDefinition, input string) *parser.Query {
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
