package parser

import "testing"

func TestParseWhereReturnQuery(t *testing.T) {
	query, err := Parse(`Students | where Students.Active == true | return Students.StudentID`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if query.Source.Table != "Students" {
		t.Fatalf("expected source table Students, got %q", query.Source.Table)
	}
	if len(query.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(query.Stages))
	}

	whereStage, ok := query.Stages[0].(*WhereStage)
	if !ok {
		t.Fatalf("expected first stage to be WhereStage, got %T", query.Stages[0])
	}

	compare, ok := whereStage.Expr.(*CompareExpr)
	if !ok {
		t.Fatalf("expected where expression to be CompareExpr, got %T", whereStage.Expr)
	}
	if compare.Op != CompareEq {
		t.Fatalf("expected CompareEq, got %s", compare.Op)
	}

	returnStage, ok := query.Stages[1].(*ReturnStage)
	if !ok {
		t.Fatalf("expected second stage to be ReturnStage, got %T", query.Stages[1])
	}
	if len(returnStage.Items) != 1 || returnStage.Items[0].Field.Name != "StudentID" {
		t.Fatalf("unexpected return items: %+v", returnStage.Items)
	}
}

func TestParseNavigateFKStage(t *testing.T) {
	query, err := Parse(`Customers | navigate Orders on Customers.CustomerID == Orders.CustomerID return Customers.CustomerName, Orders.OrderID`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	if len(query.Stages) != 1 {
		t.Fatalf("expected 1 stage, got %d", len(query.Stages))
	}

	navigateStage, ok := query.Stages[0].(*NavigateFKStage)
	if !ok {
		t.Fatalf("expected NavigateFKStage, got %T", query.Stages[0])
	}
	if navigateStage.TargetTable != "Orders" {
		t.Fatalf("expected target table Orders, got %q", navigateStage.TargetTable)
	}
	if navigateStage.Join.Left.Table != "Customers" || navigateStage.Join.Right.Table != "Orders" {
		t.Fatalf("unexpected join condition: %+v", navigateStage.Join)
	}
	if len(navigateStage.Return) != 2 {
		t.Fatalf("expected 2 return items, got %d", len(navigateStage.Return))
	}
}

func TestParseNavigateSetStage(t *testing.T) {
	query, err := Parse(`Classes | navigate set Classes.Enrollment return Students.StudentID, Students.Advisor`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	stage, ok := query.Stages[0].(*NavigateSetStage)
	if !ok {
		t.Fatalf("expected NavigateSetStage, got %T", query.Stages[0])
	}
	if stage.SetRef.Table != "Classes" || stage.SetRef.Name != "Enrollment" {
		t.Fatalf("unexpected set reference: %+v", stage.SetRef)
	}
}

func TestParseBooleanPrecedence(t *testing.T) {
	query, err := Parse(`Students | where not Students.Active == true or Students.Name like "Koza*" and Students.Count >= 10`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	whereStage := query.Stages[0].(*WhereStage)
	orExpr, ok := whereStage.Expr.(*OrExpr)
	if !ok {
		t.Fatalf("expected top-level OrExpr, got %T", whereStage.Expr)
	}

	notExpr, ok := orExpr.Left.(*NotExpr)
	if !ok {
		t.Fatalf("expected left side to be NotExpr, got %T", orExpr.Left)
	}
	if _, ok := notExpr.Expr.(*CompareExpr); !ok {
		t.Fatalf("expected not to apply to a full comparison, got %T", notExpr.Expr)
	}
	if _, ok := orExpr.Right.(*AndExpr); !ok {
		t.Fatalf("expected right side to be AndExpr, got %T", orExpr.Right)
	}
}

func TestParseNotOverParenthesizedExpression(t *testing.T) {
	query, err := Parse(`Students | where not (Students.Active == true or Students.Name like "Koza*")`)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}

	whereStage := query.Stages[0].(*WhereStage)
	notExpr, ok := whereStage.Expr.(*NotExpr)
	if !ok {
		t.Fatalf("expected top-level NotExpr, got %T", whereStage.Expr)
	}

	if _, ok := notExpr.Expr.(*OrExpr); !ok {
		t.Fatalf("expected not to apply to a parenthesized OrExpr, got %T", notExpr.Expr)
	}
}

func TestParseReportsUnexpectedToken(t *testing.T) {
	_, err := Parse(`Students | where return`)
	if err == nil {
		t.Fatal("expected parse error")
	}
}
