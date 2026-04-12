package parser

type Span struct {
	Start Position
	End   Position
}

type Node interface {
	NodeSpan() Span
}

type Query struct {
	Source *SourceStage
	Stages []Stage
	Span   Span
}

func (q *Query) NodeSpan() Span {
	return q.Span
}

type SourceStage struct {
	Table string
	Span  Span
}

func (s *SourceStage) NodeSpan() Span {
	return s.Span
}

type Stage interface {
	Node
	stageNode()
}

type WhereStage struct {
	Expr BoolExpr
	Span Span
}

func (s *WhereStage) NodeSpan() Span {
	return s.Span
}

func (*WhereStage) stageNode() {}

type NavigateFKStage struct {
	TargetTable string
	Join        JoinCond
	Return      []ReturnItem
	Span        Span
}

func (s *NavigateFKStage) NodeSpan() Span {
	return s.Span
}

func (*NavigateFKStage) stageNode() {}

type NavigateSetStage struct {
	SetRef QualifiedIdent
	Return []ReturnItem
	Span   Span
}

func (s *NavigateSetStage) NodeSpan() Span {
	return s.Span
}

func (*NavigateSetStage) stageNode() {}

type ReturnStage struct {
	Items []ReturnItem
	Span  Span
}

func (s *ReturnStage) NodeSpan() Span {
	return s.Span
}

func (*ReturnStage) stageNode() {}

type ReturnItem struct {
	Field QualifiedIdent
	Span  Span
}

func (r *ReturnItem) NodeSpan() Span {
	return r.Span
}

type JoinCond struct {
	Left  QualifiedIdent
	Right QualifiedIdent
	Span  Span
}

func (j *JoinCond) NodeSpan() Span {
	return j.Span
}

type QualifiedIdent struct {
	Table string
	Name  string
	Span  Span
}

func (q *QualifiedIdent) NodeSpan() Span {
	return q.Span
}

type BoolExpr interface {
	Node
	boolExprNode()
}

type OrExpr struct {
	Left  BoolExpr
	Right BoolExpr
	Span  Span
}

func (e *OrExpr) NodeSpan() Span {
	return e.Span
}

func (*OrExpr) boolExprNode() {}

type AndExpr struct {
	Left  BoolExpr
	Right BoolExpr
	Span  Span
}

func (e *AndExpr) NodeSpan() Span {
	return e.Span
}

func (*AndExpr) boolExprNode() {}

type NotExpr struct {
	Expr BoolExpr
	Span Span
}

func (e *NotExpr) NodeSpan() Span {
	return e.Span
}

func (*NotExpr) boolExprNode() {}

type CompareOp string

const (
	CompareEq   CompareOp = "=="
	CompareNe   CompareOp = "!="
	CompareLt   CompareOp = "<"
	CompareLe   CompareOp = "<="
	CompareGt   CompareOp = ">"
	CompareGe   CompareOp = ">="
	CompareLike CompareOp = "like"
)

type CompareExpr struct {
	Left  ValueExpr
	Op    CompareOp
	Right ValueExpr
	Span  Span
}

func (e *CompareExpr) NodeSpan() Span {
	return e.Span
}

func (*CompareExpr) boolExprNode() {}

type ValueExpr interface {
	Node
	valueExprNode()
}

type FieldRef struct {
	Field QualifiedIdent
	Span  Span
}

func (e *FieldRef) NodeSpan() Span {
	return e.Span
}

func (*FieldRef) valueExprNode() {}

type LiteralExpr struct {
	Literal Literal
	Span    Span
}

func (e *LiteralExpr) NodeSpan() Span {
	return e.Span
}

func (*LiteralExpr) valueExprNode() {}

type Literal interface {
	Node
	literalNode()
}

type StringLiteral struct {
	Raw   string
	Value string
	Span  Span
}

func (l *StringLiteral) NodeSpan() Span {
	return l.Span
}

func (*StringLiteral) literalNode() {}

type NumberLiteral struct {
	Raw   string
	Value string
	Span  Span
}

func (l *NumberLiteral) NodeSpan() Span {
	return l.Span
}

func (*NumberLiteral) literalNode() {}

type DateLiteral struct {
	Raw   string
	Value string
	Span  Span
}

func (l *DateLiteral) NodeSpan() Span {
	return l.Span
}

func (*DateLiteral) literalNode() {}

type TimeLiteral struct {
	Raw   string
	Value string
	Span  Span
}

func (l *TimeLiteral) NodeSpan() Span {
	return l.Span
}

func (*TimeLiteral) literalNode() {}

type BooleanLiteral struct {
	Raw   string
	Value bool
	Span  Span
}

func (l *BooleanLiteral) NodeSpan() Span {
	return l.Span
}

func (*BooleanLiteral) literalNode() {}
