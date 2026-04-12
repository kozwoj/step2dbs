package parser

import (
	"path/filepath"
	"strings"
	"testing"

	stepdb "github.com/kozwoj/step2/db"
)

func TestValidateASTSuccess(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Year >= 2 | navigate Teachers on Students.Advisor == Teachers.Employee_id return Students.Student_id, Teachers.Last_name`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if err := ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
}

func TestValidateASTUnknownSourceTable(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Studnts | return Students.Student_id`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "source table Studnts does not exist")
}

func TestValidateASTUnknownField(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Unknown == true`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "field Unknown does not exist in table Students")
}

func TestValidateASTUnknownSet(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Classes | navigate set Classes.Unknown return Students.Student_id`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "set Unknown does not exist in table Classes")
}

func TestValidateASTInvalidNavigateJoin(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | navigate Departments on Students.Advisor == Departments.Department_code return Departments.Name`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "is not a valid foreign-key to primary-key relationship")
}

func TestValidateASTLikeRequiresStringField(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Year like "Koza*"`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "operator like requires a string-compatible left operand")
}

func TestValidateASTLikePatternLength(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Last_name like "Alphabeti*"`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "exceeds the maximum prefix length of 8 characters")
}

func TestValidateASTRejectsBooleanOrderingComparison(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Grades | where Grades.In_major > false | return Grades.Course_code`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "operator > is not supported for boolean operands")
}

func TestValidateASTRejectsStringOrderingComparison(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Last_name > "Perry" | return Students.Student_id`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "operator > is not supported for string operands")
}

func TestValidateASTAllowsStringEqualityComparison(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Last_name == "Perry" | return Students.Student_id`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if err := ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
}

func TestValidateASTRejectsInvalidDateLiteralFormat(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := &Query{
		Source: &SourceStage{Table: "Students"},
		Stages: []Stage{
			&WhereStage{Expr: &CompareExpr{
				Left:  &FieldRef{Field: QualifiedIdent{Table: "Students", Name: "Birth_date"}},
				Op:    CompareEq,
				Right: &LiteralExpr{Literal: &DateLiteral{Value: "2024-99-99"}},
			}},
			&ReturnStage{Items: []ReturnItem{{Field: QualifiedIdent{Table: "Students", Name: "Student_id"}}}},
		},
	}

	err := ValidateAST(query, dbDef)
	assertErrorContains(t, err, "invalid DATE literal format")
}

func TestValidateASTRejectsInvalidTimeLiteralFormatOnTimeField(t *testing.T) {
	dbDef := loadAllTypesDefinition(t)
	query := &Query{
		Source: &SourceStage{Table: "AllTypes"},
		Stages: []Stage{
			&WhereStage{Expr: &CompareExpr{
				Left:  &FieldRef{Field: QualifiedIdent{Table: "AllTypes", Name: "Time_value"}},
				Op:    CompareEq,
				Right: &LiteralExpr{Literal: &TimeLiteral{Value: "14:30:45:123"}},
			}},
			&ReturnStage{Items: []ReturnItem{{Field: QualifiedIdent{Table: "AllTypes", Name: "Integer_value"}}}},
		},
	}

	err := ValidateAST(query, dbDef)
	assertErrorContains(t, err, "invalid TIME literal format")
}

func TestValidateASTRejectsInvalidDecimalLiteralFormat(t *testing.T) {
	dbDef := loadAllTypesDefinition(t)
	query := &Query{
		Source: &SourceStage{Table: "AllTypes"},
		Stages: []Stage{
			&WhereStage{Expr: &CompareExpr{
				Left:  &FieldRef{Field: QualifiedIdent{Table: "AllTypes", Name: "Decimal_value"}},
				Op:    CompareEq,
				Right: &LiteralExpr{Literal: &NumberLiteral{Value: "12.34.56"}},
			}},
			&ReturnStage{Items: []ReturnItem{{Field: QualifiedIdent{Table: "AllTypes", Name: "Integer_value"}}}},
		},
	}

	err := ValidateAST(query, dbDef)
	assertErrorContains(t, err, "invalid DECIMAL literal format")
}

func TestValidateASTRejectsTerminalWhere(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Year >= 2`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "pipeline cannot end with where")
}

func TestValidateASTAllowsWhereFollowedByReturn(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Students | where Students.Year >= 2 | return Students.Student_id, Students.Last_name`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if err := ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
}

func TestValidateASTRejectsChainedNavigateWithoutProjectedFK(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Classes | where Classes.Course == "MATH202 " | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Teachers.Office | navigate Departments on Teachers.Works_for == Departments.Department_code return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Departments.Building_name, Teachers.Office`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	err = ValidateAST(query, dbDef)
	assertErrorContains(t, err, "field Teachers.Works_for is not available in the current pipeline state")
}

func TestValidateASTAllowsChainedNavigateWithProjectedFK(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query, err := Parse(`Classes | where Classes.Course == "MATH202 " | navigate Teachers on Classes.Teacher == Teachers.Employee_id return Classes.Class_code, Teachers.Works_for, Teachers.First_name, Teachers.Last_name, Teachers.Office | navigate Departments on Teachers.Works_for == Departments.Department_code return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Departments.Building_name, Teachers.Office`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if err := ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
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

func loadAllTypesDefinition(t *testing.T) *stepdb.DBDefinition {
	t.Helper()
	schemaFile := filepath.Join("..", "..", "step2", "docs", "testdata", "AllTypes.ddl")
	dbDef, err := stepdb.NewDBDefinitionFromSchema(schemaFile)
	if err != nil {
		t.Fatalf("failed to load DBDefinition from schema: %v", err)
	}
	return dbDef
}

func assertErrorContains(t *testing.T, err error, want string) {
	t.Helper()
	if err == nil {
		t.Fatal("expected error, got nil")
	}
	if !strings.Contains(err.Error(), want) {
		t.Fatalf("expected error containing %q, got %q", want, err.Error())
	}
}
