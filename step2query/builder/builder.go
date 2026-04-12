package builder

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

/*
BuildPipeline builds a pipeline description from a validated query AST and a DB definition.
Parameters:
- query: the validated parsed query whose source and stage nodes will be converted into pipeline stages
- dbDef: the database definition used to resolve source tables, field metadata, and navigation targets
Returns:
- *pipeline.Pipeline: the pipeline description that the engine can later execute
- error: an error if the query cannot be converted into a valid pipeline description
*/
func BuildPipeline(query *parser.Query, dbDef *stepdb.DBDefinition) (*pipeline.Pipeline, error) {
	if query == nil {
		return nil, fmt.Errorf("query is nil")
	}
	if query.Source == nil {
		return nil, fmt.Errorf("query source is nil")
	}
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	// Step 1: Resolve query.Source into the initial pipeline state description.
	tableIndex, exists := dbDef.TableIndex[query.Source.Table]
	if !exists {
		return nil, fmt.Errorf("source table %s does not exist", query.Source.Table)
	}
	table := dbDef.Tables[tableIndex]
	initialState := pipeline.StateDescription{
		Kind:      pipeline.StateSourceDBTableSet,
		TableName: query.Source.Table,
		RecordDef: buildRecordDefinition(table),
	}

	// Step 2: Initialize a Pipeline value with that InitialState and an empty Stages slice.
	builtPipeline := &pipeline.Pipeline{
		InitialState: initialState,
		Stages:       []pipeline.Stage{},
	}

	// Step 3: Track the current state description, starting from pipeline.InitialState. Each next AST
	// stage is resolved against this current state.
	currentState := builtPipeline.InitialState

	/* Step 4: Iterate over query.Stages from left to right and build stage definitions based on
	- AST node type: *parser.WhereStage, *parser.NavigateFKStage, *parser.NavigateSetStage, *parser.ReturnStage
	- current state kind: pipeline.StateSourceDBTableSet, pipeline.StateDBTableWorkingSet, pipeline.StateReturnWorkingSet
	*/
	for _, astStage := range query.Stages {
		var builtStage pipeline.Stage

		switch typed := astStage.(type) {
		case *parser.WhereStage:
			built, err := buildWhereStageDefinition(typed, currentState, dbDef)
			if err != nil {
				return nil, err
			}
			builtStage = built
		case *parser.NavigateFKStage:
			built, err := buildNavigateFKStageDefinition(typed, currentState, dbDef)
			if err != nil {
				return nil, err
			}
			builtStage = built
		case *parser.NavigateSetStage:
			built, err := buildNavigateSetStageDefinition(typed, currentState, dbDef)
			if err != nil {
				return nil, err
			}
			builtStage = built
		case *parser.ReturnStage:
			built, err := buildReturnStageDefinition(typed, currentState, dbDef)
			if err != nil {
				return nil, err
			}
			builtStage = built
		default:
			return nil, fmt.Errorf("unsupported stage type %T", astStage)
		}

		// Append the built stage to pipeline.Stages.
		builtPipeline.Stages = append(builtPipeline.Stages, builtStage)

		// Update the current state description to the new output state, so next AST stage will be resolved against it.
		currentState = builtStage.Output()
	}

	// Step 5: If an AST stage and the current state form an illegal combination return an error.
	// For example, NavigateSetStage is not valid on ReturnWorkingSet as input.

	// Step 6: Return fully built pipeline. Later validation may also enforce that the final stage produces a ReturnWorkingSet.

	return builtPipeline, nil
}
