package builder

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

/*
buildReturnStageDefinition constructs a return stage definition based on the current state and the return stage AST node.
It validates that the return stage is applicable to the current state kind and that the projected fields are available in the input state.
It also builds the projection plan for the engine to execute.
*/
func buildReturnStageDefinition(stage *parser.ReturnStage, currentState pipeline.StateDescription, dbDef *stepdb.DBDefinition) (pipeline.Stage, error) {
	if stage == nil {
		return nil, fmt.Errorf("return stage is nil")
	}
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	items, outputState, err := buildProjection(stage.Items, currentState.TableName, func(item parser.ReturnItem) (*pipeline.FieldDef, int, error) {
		return resolveFieldFromState(currentState, item.Field.Table, item.Field.Name)
	})
	if err != nil {
		return nil, err
	}

	plan := pipeline.ReturnPlan{Items: items}

	switch currentState.Kind {
	case pipeline.StateSourceDBTableSet, pipeline.StateDBTableWorkingSet:
		return &pipeline.DBReturnStage{
			StageBase: pipeline.StageBase{
				StageKind:   pipeline.StageDBReturn,
				InputState:  currentState,
				OutputState: outputState,
			},
			Plan: plan,
		}, nil
	case pipeline.StateReturnWorkingSet:
		return &pipeline.MemoryReturnStage{
			StageBase: pipeline.StageBase{
				StageKind:   pipeline.StageMemoryReturn,
				InputState:  currentState,
				OutputState: outputState,
			},
			Plan: plan,
		}, nil
	default:
		return nil, fmt.Errorf("return stage is not supported for state kind %d", currentState.Kind)
	}
}
