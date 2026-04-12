package pipeline

/*
StageKind identifies the concrete instantiated pipeline stage.
*/
type StageKind int

const (
	StageDBWhere StageKind = iota + 1
	StageMemoryWhere
	StageDBNavigateFK
	StageMemoryNavigateFK
	StageDBNavigateSet
	StageDBReturn
	StageMemoryReturn
)

/*
Stage is the passive interface implemented by all pipeline stage definitions.
The builder creates values of these concrete types.
The engine later executes them against runtime State values.
*/
type Stage interface {
	Kind() StageKind
	Input() StateDescription
	Output() StateDescription
}

/*
StageBase stores the metadata common to all stage definitions.
*/
type StageBase struct {
	StageKind   StageKind
	InputState  StateDescription
	OutputState StateDescription
}

func (b *StageBase) Kind() StageKind {
	return b.StageKind
}

func (b *StageBase) Input() StateDescription {
	return b.InputState
}

func (b *StageBase) Output() StateDescription {
	return b.OutputState
}

type DBWhereStage struct {
	StageBase
	Plan WherePlan
}

type MemoryWhereStage struct {
	StageBase
	Plan WherePlan
}

type DBNavigateFKStage struct {
	StageBase
	Plan FKNavigationPlan
}

type MemoryNavigateFKStage struct {
	StageBase
	Plan FKNavigationPlan
}

type DBNavigateSetStage struct {
	StageBase
	Plan SetNavigationPlan
}

type DBReturnStage struct {
	StageBase
	Plan ReturnPlan
}

type MemoryReturnStage struct {
	StageBase
	Plan ReturnPlan
}