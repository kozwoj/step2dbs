package builder

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

/*
buildWhereStageDefinition constructs a where stage definition based on the current state and the where stage AST node.
It performs validation to ensure that the where stage is applicable to the current state kind and that necessary information is available.
For DB-backed states, it also performs analysis to populate the DBPlan used by the engine for optimized execution.
*/
func buildWhereStageDefinition(stage *parser.WhereStage, currentState pipeline.StateDescription, dbDef *stepdb.DBDefinition) (pipeline.Stage, error) {
	if stage == nil {
		return nil, fmt.Errorf("where stage is nil")
	}
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	switch currentState.Kind {
	case pipeline.StateSourceDBTableSet, pipeline.StateDBTableWorkingSet:
		outputState := pipeline.StateDescription{
			Kind:      pipeline.StateDBTableWorkingSet,
			TableName: currentState.TableName,
			RecordDef: currentState.RecordDef,
		}

		var dbPlan *pipeline.DBWhereAnalysisPlan
		if currentState.Kind == pipeline.StateSourceDBTableSet {
			analyzedPlan, err := GetBDWherePlan(stage.Expr, currentState)
			if err != nil {
				return nil, err
			}
			dbPlan = analyzedPlan
		}

		return &pipeline.DBWhereStage{
			StageBase: pipeline.StageBase{
				StageKind:   pipeline.StageDBWhere,
				InputState:  currentState,
				OutputState: outputState,
			},
			Plan: pipeline.WherePlan{
				Expr:          stage.Expr,
				DBPlan:        dbPlan,
				ExecutionMode: "db-scan",
			},
		}, nil

	case pipeline.StateReturnWorkingSet:
		return &pipeline.MemoryWhereStage{
			StageBase: pipeline.StageBase{
				StageKind:   pipeline.StageMemoryWhere,
				InputState:  currentState,
				OutputState: currentState,
			},
			Plan: pipeline.WherePlan{
				Expr:          stage.Expr,
				ExecutionMode: "memory-scan",
			},
		}, nil
	default:
		return nil, fmt.Errorf("where stage is not supported for state kind %d", currentState.Kind)
	}
}
