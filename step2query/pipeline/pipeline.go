package pipeline

/*
Pipeline is the passive definition of one query execution plan.
It starts from an initial state description and applies the listed stages in order.
Execution behavior is intentionally owned by the engine package rather than this model package.
*/
type Pipeline struct {
	InitialState StateDescription
	Stages       []Stage
}