package builder

import (
	"fmt"
	"strings"

	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

type dbWhereAnalysisResult struct {
	rootClass pipeline.WhereExprClass
	search    *pipeline.DBWhereSearchPlan
}

/*
GetBDWherePlan is intended only for the first where stage in a pipeline, when the input is SourceDBTableSet.

It builds evaluation plan for one validated where expression where some of the expression nodes can be evaluated
using DB dictionaries or indexes. The resulting DBWhereAnalysisPlan captures the searchable portion of
the expression as a tree of search plans. The remaining portion of the expression that cannot be evaluated using
DB indexes/dictionaries is returned as residual.

The plan determines if the where stage can be executed without the full table scan.

Later pipeline where stages over DBTableWorkingSet are evaluated without this pre-analysis step.
*/
func GetBDWherePlan(expr parser.BoolExpr, inputState pipeline.StateDescription) (*pipeline.DBWhereAnalysisPlan, error) {
	if expr == nil {
		return nil, fmt.Errorf("where expression is nil")
	}

	switch inputState.Kind {
	case pipeline.StateSourceDBTableSet:
		result, err := analyzeDBWhereExpr(expr, inputState)
		if err != nil {
			return nil, err
		}
		return &pipeline.DBWhereAnalysisPlan{
			RootClass: result.rootClass,
			Search:    result.search,
		}, nil
	case pipeline.StateDBTableWorkingSet:
		return nil, fmt.Errorf("DB where analysis is only supported for source DB table input")
	default:
		return nil, fmt.Errorf("DB where analysis is not supported for state kind %d", inputState.Kind)
	}
}

/* analyzeDBWhereExpr recursively analyzes the boolean expression to identify parts that can be evaluated using DB indexes/dictionaries.

It classifies the expression into three categories:
- WhereExprSearchable: The expression can be fully evaluated using DB indexes/dictionaries.
- WhereExprFilterOnInput: The expression has a searchable portion, but also has a residual that requires filtering on the input.
- WhereExprNeedsFullInput: The expression cannot be evaluated using DB indexes/dictionaries and requires a full scan of the input.
*/

func analyzeDBWhereExpr(expr parser.BoolExpr, inputState pipeline.StateDescription) (dbWhereAnalysisResult, error) {
	switch typed := expr.(type) {
	case *parser.CompareExpr:
		search, ok, err := buildSearchLeafPlan(typed, inputState)
		if err != nil {
			return dbWhereAnalysisResult{}, err
		}
		if ok {
			return dbWhereAnalysisResult{rootClass: pipeline.WhereExprSearchable, search: search}, nil
		}
		return dbWhereAnalysisResult{rootClass: pipeline.WhereExprFilterOnInput}, nil

	case *parser.AndExpr:
		left, err := analyzeDBWhereExpr(typed.Left, inputState)
		if err != nil {
			return dbWhereAnalysisResult{}, err
		}
		right, err := analyzeDBWhereExpr(typed.Right, inputState)
		if err != nil {
			return dbWhereAnalysisResult{}, err
		}

		search := combineAndSearch(left, right, typed)
		if left.rootClass == pipeline.WhereExprSearchable && right.rootClass == pipeline.WhereExprSearchable {
			return dbWhereAnalysisResult{rootClass: pipeline.WhereExprSearchable, search: search}, nil
		}
		if search != nil {
			residual := collectNonSearchableExpr(left, typed.Left, right, typed.Right)
			if residual != nil {
				search.Residual = combineBoolAnd(search.Residual, residual)
			}
			return dbWhereAnalysisResult{rootClass: pipeline.WhereExprFilterOnInput, search: search}, nil
		}
		return dbWhereAnalysisResult{rootClass: pipeline.WhereExprFilterOnInput}, nil

	case *parser.OrExpr:
		left, err := analyzeDBWhereExpr(typed.Left, inputState)
		if err != nil {
			return dbWhereAnalysisResult{}, err
		}
		right, err := analyzeDBWhereExpr(typed.Right, inputState)
		if err != nil {
			return dbWhereAnalysisResult{}, err
		}

		if left.search != nil && right.search != nil {
			rootClass := pipeline.WhereExprSearchable
			if left.rootClass != pipeline.WhereExprSearchable || right.rootClass != pipeline.WhereExprSearchable {
				rootClass = pipeline.WhereExprFilterOnInput
			}
			return dbWhereAnalysisResult{
				rootClass: rootClass,
				search: &pipeline.DBWhereSearchPlan{
					Kind:  pipeline.DBWhereSearchOr,
					Expr:  expr,
					Left:  left.search,
					Right: right.search,
				},
			}, nil
		}
		return dbWhereAnalysisResult{rootClass: pipeline.WhereExprNeedsFullInput}, nil

	case *parser.NotExpr:
		return dbWhereAnalysisResult{rootClass: pipeline.WhereExprNeedsFullInput}, nil

	default:
		return dbWhereAnalysisResult{}, fmt.Errorf("unsupported boolean expression type %T", expr)
	}
}

func combineAndSearch(left dbWhereAnalysisResult, right dbWhereAnalysisResult, expr parser.BoolExpr) *pipeline.DBWhereSearchPlan {
	leftSearchable := left.rootClass != pipeline.WhereExprNeedsFullInput && left.search != nil
	rightSearchable := right.rootClass != pipeline.WhereExprNeedsFullInput && right.search != nil

	switch {
	case leftSearchable && rightSearchable:
		return &pipeline.DBWhereSearchPlan{
			Kind:  pipeline.DBWhereSearchAnd,
			Expr:  expr,
			Left:  left.search,
			Right: right.search,
		}
	case leftSearchable:
		return left.search
	case rightSearchable:
		return right.search
	default:
		return nil
	}
}

// collectNonSearchableExpr returns the combined expression for AND branches that have no search plan.
// If both branches have search plans, returns nil (residuals are carried inside the search nodes).
func collectNonSearchableExpr(left dbWhereAnalysisResult, leftExpr parser.BoolExpr, right dbWhereAnalysisResult, rightExpr parser.BoolExpr) parser.BoolExpr {
	var a, b parser.BoolExpr
	if left.search == nil {
		a = leftExpr
	}
	if right.search == nil {
		b = rightExpr
	}
	return combineBoolAnd(a, b)
}

// combineBoolAnd combines two boolean expressions with AND, returning nil if both are nil.
func combineBoolAnd(a, b parser.BoolExpr) parser.BoolExpr {
	if a == nil {
		return b
	}
	if b == nil {
		return a
	}
	return &parser.AndExpr{Left: a, Right: b}
}

func buildSearchLeafPlan(expr *parser.CompareExpr, inputState pipeline.StateDescription) (*pipeline.DBWhereSearchPlan, bool, error) {
	if expr == nil {
		return nil, false, fmt.Errorf("compare expression is nil")
	}

	fieldRef, fieldPosition, fieldDef, literal, ok := resolveFieldLiteralOperands(expr, inputState)
	if !ok {
		return nil, false, nil
	}

	qualifiedField := pipeline.QualifiedFieldRef{
		TableName: fieldRef.Table,
		FieldName: fieldRef.Name,
		Position:  fieldPosition,
		Type:      fieldDef.Type,
	}

	if expr.Op == parser.CompareEq && fieldPosition == inputState.RecordDef.PrimaryKey {
		return &pipeline.DBWhereSearchPlan{
			Kind:    pipeline.DBWhereSearchPrimaryKeyExact,
			Expr:    expr,
			Field:   qualifiedField,
			Literal: literal,
		}, true, nil
	}

	stringLiteral, ok := literal.(*parser.StringLiteral)
	if !ok || fieldDef.Type != pipeline.STRING {
		return nil, false, nil
	}

	switch expr.Op {
	case parser.CompareEq:
		return &pipeline.DBWhereSearchPlan{
			Kind:    pipeline.DBWhereSearchStringExact,
			Expr:    expr,
			Field:   qualifiedField,
			Literal: literal,
		}, true, nil
	case parser.CompareLike:
		return &pipeline.DBWhereSearchPlan{
			Kind:    pipeline.DBWhereSearchStringPrefix,
			Expr:    expr,
			Field:   qualifiedField,
			Literal: literal,
			Prefix:  strings.TrimSuffix(stringLiteral.Value, "*"),
		}, true, nil
	default:
		return nil, false, nil
	}
}

func resolveFieldLiteralOperands(expr *parser.CompareExpr, inputState pipeline.StateDescription) (parser.QualifiedIdent, int, *pipeline.FieldDef, parser.Literal, bool) {
	if fieldRef, literal, ok := resolveFieldLiteralPair(expr.Left, expr.Right, inputState); ok {
		return fieldRef.field.Field, fieldRef.position, fieldRef.definition, literal, true
	}
	if expr.Op != parser.CompareEq {
		return parser.QualifiedIdent{}, 0, nil, nil, false
	}
	if fieldRef, literal, ok := resolveFieldLiteralPair(expr.Right, expr.Left, inputState); ok {
		return fieldRef.field.Field, fieldRef.position, fieldRef.definition, literal, true
	}
	return parser.QualifiedIdent{}, 0, nil, nil, false
}

type resolvedFieldRef struct {
	field      *parser.FieldRef
	position   int
	definition *pipeline.FieldDef
}

func resolveFieldLiteralPair(fieldExpr parser.ValueExpr, literalExpr parser.ValueExpr, inputState pipeline.StateDescription) (resolvedFieldRef, parser.Literal, bool) {
	fieldRef, ok := fieldExpr.(*parser.FieldRef)
	if !ok {
		return resolvedFieldRef{}, nil, false
	}
	fieldDef, fieldPosition, exists := inputState.RecordDef.LookupField(fieldRef.Field.Table, fieldRef.Field.Name)
	if !exists {
		return resolvedFieldRef{}, nil, false
	}
	literalValue, ok := literalExpr.(*parser.LiteralExpr)
	if !ok {
		return resolvedFieldRef{}, nil, false
	}
	return resolvedFieldRef{
		field:      fieldRef,
		position:   fieldPosition,
		definition: fieldDef,
	}, literalValue.Literal, true
}
