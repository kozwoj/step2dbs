package pipeline

/*
FKNavigationPlan captures the resolved FK-to-PK navigation description.
*/
type FKNavigationPlan struct {
	SourceTable string
	TargetTable string
	ForeignKey  QualifiedFieldRef
	PrimaryKey  QualifiedFieldRef
	ReturnItems []ReturnFieldRef
}

/*
SetNavigationPlan captures the resolved SET navigation description.
*/
type SetNavigationPlan struct {
	SourceTable string
	TargetTable string
	SetName     string
	ReturnItems []ReturnFieldRef
}

/*
ReturnPlan captures an explicit projection.
*/
type ReturnPlan struct {
	Items []ReturnFieldRef
}