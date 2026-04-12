package pipeline

/*
StateKind identifies the kind of state flowing between pipeline stages.
This is metadata for pipeline planning, not the runtime State interface itself.
*/
type StateKind int

const (
	StateSourceDBTableSet StateKind = iota + 1
	StateDBTableWorkingSet
	StateReturnWorkingSet
)

/*
StateDescription is a lightweight description of the state that a stage consumes or produces.
It is metadata, not the runtime data container itself.
*/
type StateDescription struct {
	Kind      StateKind
	TableName string
	RecordDef RecordDefinition
}