package pipeline

import "github.com/kozwoj/step2query/parser"

type WhereExprClass int

const (
	WhereExprSearchable WhereExprClass = iota + 1
	WhereExprFilterOnInput
	WhereExprNeedsFullInput
)

type DBWhereSearchKind int

const (
	DBWhereSearchStringExact DBWhereSearchKind = iota + 1
	DBWhereSearchStringPrefix
	DBWhereSearchPrimaryKeyExact
	DBWhereSearchAnd
	DBWhereSearchOr
)

/*
DBWhereSearchPlan describes one node in the searchable portion of a DB-backed where expression.
Leaf nodes describe one dictionary or primary-key lookup. Internal nodes combine child lookups
through intersection (and) or union (or). Any node may carry a Residual predicate that must be
evaluated record-by-record on the candidate set produced by its search.
*/
type DBWhereSearchPlan struct {
	Kind     DBWhereSearchKind
	Expr     parser.BoolExpr
	Field    QualifiedFieldRef
	Literal  parser.Literal
	Prefix   string
	Residual parser.BoolExpr
	Left     *DBWhereSearchPlan
	Right    *DBWhereSearchPlan
}

/*
DBWhereAnalysisPlan describes how a DB-backed where expression should be evaluated.
If Search is non-nil, the engine can build a candidate record-ID set from it,
evaluating any Residual predicates carried by search nodes along the way.
NeedsFullInput means the full incoming record set must be scanned.
*/
type DBWhereAnalysisPlan struct {
	RootClass WhereExprClass
	Search    *DBWhereSearchPlan
}

/*
WherePlan captures the resolved predicate carried by a where stage.
The Expr field references the validated AST subtree.
DBPlan is populated only for the first DB-backed where stage, when the input is SourceDBTableSet.
ExecutionMode is still a coarse builder hint for the current implementation.
*/
type WherePlan struct {
	Expr          parser.BoolExpr
	DBPlan        *DBWhereAnalysisPlan
	ExecutionMode string
}