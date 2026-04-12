package engine

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/pipeline"
)

/*
ExecutePipeline runs a passive pipeline definition against an opened STEP2 database.
*/
func ExecutePipeline(plan *pipeline.Pipeline, dbDef *stepdb.DBDefinition) (State, error) {
	if plan == nil {
		return nil, fmt.Errorf("pipeline is nil")
	}
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	var current State

	if plan.InitialState.Kind == pipeline.StateSourceDBTableSet {
		state, err := NewSourceDBTableSet(plan.InitialState.TableName, dbDef)
		if err != nil {
			return nil, fmt.Errorf("failed to create initial source state for table %s: %w", plan.InitialState.TableName, err)
		}
		current = state
	} else {
		return nil, fmt.Errorf("pipeline execution for initial state kind %d is not implemented yet", plan.InitialState.Kind)
	}

	for index, stage := range plan.Stages {
		if stage == nil {
			return nil, fmt.Errorf("pipeline stage %d is nil", index)
		}

		next, err := ExecuteStage(stage, current)
		if err != nil {
			return nil, fmt.Errorf("pipeline stage %d (%T) failed: %w", index, stage, err)
		}
		current = next
	}

	return current, nil
}
