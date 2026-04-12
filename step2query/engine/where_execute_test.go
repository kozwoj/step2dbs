package engine

import (
	"path/filepath"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
)

// to run all tests in this file run from the step2query directory:
// go test ./engine -run '^TestEvaluateBoolExpr' -v

func TestEvaluateBoolExprPrimaryKeyExample(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.Student_id == "NIP2409002" | return Students.First_name, Students.Last_name`,
	)
	record := map[string]interface{}{
		"Student_id": "NIP2409002",
		"First_name": "Sophia",
		"Last_name":  "Brown",
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected primary-key example to match")
	}
}

func TestEvaluateBoolExprStringEqualityExample(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.Last_name == "Perry" | return Students.First_name, Students.Last_name`,
	)
	record := map[string]interface{}{
		"Last_name":  "Perry",
		"First_name": "Alex",
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected string equality example to match")
	}
}

func TestEvaluateBoolExprLikeExample(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.Last_name like "Je*" | return Students.First_name, Students.Last_name, Students.Year`,
	)
	record := map[string]interface{}{
		"Last_name":  "Jensen",
		"First_name": "Casey",
		"Year":       2,
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected like example to match")
	}
}

func TestEvaluateBoolExprStringAndResidualExample(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.State_or_Country == "Colorado" and Students.Year > 2 | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits`,
	)
	record := map[string]interface{}{
		"State_or_Country": "Colorado",
		"Year":             3,
		"Student_id":       "NIP2209001",
		"Last_name":        "Johnson",
		"Credits":          85,
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected string-and-residual example to match")
	}
}

func TestEvaluateBoolExprGroupedAndOrExample(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.State_or_Country == "Colorado" and (Students.Year == 2 or Students.Year == 3) | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits`,
	)
	record := map[string]interface{}{
		"State_or_Country": "Colorado",
		"Year":             2,
		"Student_id":       "NIP2309004",
		"Last_name":        "Jackson",
		"Credits":          50,
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected grouped and/or example to match")
	}
}

func TestEvaluateBoolExprDateComparison(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.Birth_date < 2005-01-01 | return Students.Student_id`,
	)
	record := map[string]interface{}{
		"Birth_date": "2004-05-12",
		"Student_id": "NIP2309006",
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected date comparison to match")
	}
}

func TestEvaluateBoolExprMissingOptionalFieldReturnsFalse(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "College.ddl"),
		`Students | where Students.Preferred_name == "Sophie" | return Students.Student_id`,
	)
	record := map[string]interface{}{
		"Student_id": "NIP2309001",
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if matched {
		t.Fatal("expected missing optional field comparison to return false")
	}
}

func TestEvaluateBoolExprTimeComparison(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "AllTypes.ddl"),
		`AllTypes | where AllTypes.Time_value < 10:00:00 | return AllTypes.Integer_value`,
	)
	record := map[string]interface{}{
		"Time_value":    "9:30:00",
		"Integer_value": 100,
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected time comparison to match")
	}
}

func TestEvaluateBoolExprDecimalComparison(t *testing.T) {
	expr, recordDef := parseValidatedWhereExpr(t,
		filepath.Join("..", "..", "step2", "docs", "testdata", "AllTypes.ddl"),
		`AllTypes | where AllTypes.Decimal_value > 2.25 | return AllTypes.Integer_value`,
	)
	record := map[string]interface{}{
		"Decimal_value": "10.50",
		"Integer_value": 100,
	}

	matched, err := evaluateBoolExpr(expr, record, recordDef)
	if err != nil {
		t.Fatalf("evaluateBoolExpr returned error: %v", err)
	}
	if !matched {
		t.Fatal("expected decimal comparison to match")
	}
}

func parseValidatedWhereExpr(t *testing.T, schemaFile string, input string) (parser.BoolExpr, RecordDefinition) {
	t.Helper()

	dbDef, err := stepdb.NewDBDefinitionFromSchema(schemaFile)
	if err != nil {
		t.Fatalf("NewDBDefinitionFromSchema returned error: %v", err)
	}

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

	tableIndex, exists := dbDef.TableIndex[query.Source.Table]
	if !exists {
		t.Fatalf("source table %s not found in DBDefinition", query.Source.Table)
	}

	return whereStage.Expr, buildRecordDefinition(dbDef.Tables[tableIndex])
}
