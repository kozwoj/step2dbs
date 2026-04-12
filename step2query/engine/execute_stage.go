package engine

import (
	"errors"
	"fmt"

	"github.com/kozwoj/step2query/pipeline"
)

/*
ExecuteStage executes one passive pipeline stage definition against the supplied runtime state.
It exists as a transition API while plan definitions live in the pipeline package and execution behavior remains in engine.
*/
func ExecuteStage(stage pipeline.Stage, input State) (State, error) {
	switch typed := stage.(type) {
	case *pipeline.DBWhereStage:
		return executeDBWhereStage(typed, input)
	case *pipeline.MemoryWhereStage:
		return executeMemoryWhereStage(typed, input)
	case *pipeline.DBNavigateFKStage:
		return executeDBNavigateFKStage(typed, input)
	case *pipeline.MemoryNavigateFKStage:
		return executeMemoryNavigateFKStage(typed, input)
	case *pipeline.DBNavigateSetStage:
		return executeDBNavigateSetStage(typed, input)
	case *pipeline.DBReturnStage:
		return executeDBReturnStage(typed, input)
	case *pipeline.MemoryReturnStage:
		return executeMemoryReturnStage(typed, input)
	default:
		return nil, fmt.Errorf("unsupported pipeline stage type %T", stage)
	}
}

func executeDBWhereStage(stage *pipeline.DBWhereStage, input State) (State, error) {
	if stage.InputState.Kind != StateSourceDBTableSet && stage.InputState.Kind != StateDBTableWorkingSet {
		return nil, fmt.Errorf("DBWhereStage expects input state kind to be SourceDBTableSet or DBTableWorkingSet, got %v", stage.InputState.Kind)
	}
	if stage.InputState.Kind == StateDBTableWorkingSet {
		return nil, fmt.Errorf("DBWhereStage execution for StateDBTableWorkingSet input is not implemented yet")
	}
	if stage.Plan.DBPlan == nil {
		return nil, fmt.Errorf("DBWhereStage requires a non-nil Plan")
	}
	if stage.Plan.Expr == nil {
		return nil, fmt.Errorf("DBWhereStage requires a non-nil Expr in the Plan")
	}

	if stage.Plan.DBPlan.Search != nil {
		outRecordIDs, err := executeSearchPlan(stage.Plan.DBPlan.Search, input.TableName(), input.DBDef(), stage.InputState.RecordDef)
		if err != nil {
			return nil, fmt.Errorf("DBWhereStage search plan execution failed: %w", err)
		}
		return NewDBTableWorkingSet(input.TableName(), input.DBDef(), outRecordIDs)
	}

	// if there is no search plan, we need to scan all records in the input and evaluate the expression in memory
	var outRecordIDs []uint32
	currentRecord, err := input.GetFirstRecord()
	if err != nil {
		return nil, fmt.Errorf("DBWhereStage failed to get first record from input state: %w", err)
	}
	for currentRecord != nil {
		match, err := evaluateBoolExpr(stage.Plan.Expr, currentRecord, stage.InputState.RecordDef)
		if err != nil {
			return nil, fmt.Errorf("DBWhereStage failed to evaluate expression: %w", err)
		}
		if match {
			outRecordIDs = append(outRecordIDs, input.CurrentRecordID())
		}

		currentRecord, err = input.GetNextRecord()
		if err != nil {
			if errors.Is(err, ErrNoMoreRecords) {
				break
			}
			return nil, fmt.Errorf("DBWhereStage failed to get next record from input state: %w", err)
		}
	}

	return NewDBTableWorkingSet(input.TableName(), input.DBDef(), outRecordIDs)
}

func executeMemoryWhereStage(stage *pipeline.MemoryWhereStage, input State) (State, error) {
	if input == nil {
		return nil, fmt.Errorf("MemoryWhereStage requires a non-nil input state")
	}
	if stage.InputState.Kind != StateReturnWorkingSet {
		return nil, fmt.Errorf("MemoryWhereStage expects input state kind ReturnWorkingSet, got %v", stage.InputState.Kind)
	}
	if stage.Plan.Expr == nil {
		return nil, fmt.Errorf("MemoryWhereStage requires a non-nil Expr in the Plan")
	}

	inputDef := input.RecordDef()
	var matchingRecords []map[string]interface{}

	currentRecord, err := input.GetFirstRecord()
	if err != nil {
		if errors.Is(err, ErrNoMoreRecords) {
			return NewReturnWorkingSet(input.TableName(), inputDef, input.DBDef(), matchingRecords), nil
		}
		return nil, fmt.Errorf("MemoryWhereStage failed to get first record: %w", err)
	}

	for currentRecord != nil {
		match, err := evaluateBoolExpr(stage.Plan.Expr, currentRecord, inputDef)
		if err != nil {
			return nil, fmt.Errorf("MemoryWhereStage failed to evaluate expression: %w", err)
		}
		if match {
			matchingRecords = append(matchingRecords, currentRecord)
		}

		currentRecord, err = input.GetNextRecord()
		if err != nil {
			if errors.Is(err, ErrNoMoreRecords) {
				break
			}
			return nil, fmt.Errorf("MemoryWhereStage failed to get next record: %w", err)
		}
	}

	return NewReturnWorkingSet(input.TableName(), inputDef, input.DBDef(), matchingRecords), nil
}

func executeDBNavigateFKStage(stage *pipeline.DBNavigateFKStage, input State) (State, error) {
	return executeNavigateFK(input, stage.OutputState, stage.Plan, "DBNavigateFKStage")
}

func executeMemoryNavigateFKStage(stage *pipeline.MemoryNavigateFKStage, input State) (State, error) {
	return executeNavigateFK(input, stage.OutputState, stage.Plan, "MemoryNavigateFKStage")
}

func executeDBNavigateSetStage(stage *pipeline.DBNavigateSetStage, input State) (State, error) {
	return executeNavigateSet(input, stage.OutputState, stage.Plan, "DBNavigateSetStage")
}

func executeDBReturnStage(stage *pipeline.DBReturnStage, input State) (State, error) {
	if input == nil {
		return nil, fmt.Errorf("DBReturnStage requires a non-nil input state")
	}
	if stage.InputState.Kind != StateSourceDBTableSet && stage.InputState.Kind != StateDBTableWorkingSet {
		return nil, fmt.Errorf("DBReturnStage expects input state kind to be SourceDBTableSet or DBTableWorkingSet, got %v", stage.InputState.Kind)
	}

	return executeReturnProjection(input, stage.OutputState, stage.Plan, "DBReturnStage")
}

func executeMemoryReturnStage(stage *pipeline.MemoryReturnStage, input State) (State, error) {
	if input == nil {
		return nil, fmt.Errorf("MemoryReturnStage requires a non-nil input state")
	}
	if stage.InputState.Kind != StateReturnWorkingSet {
		return nil, fmt.Errorf("MemoryReturnStage expects input state kind to be ReturnWorkingSet, got %v", stage.InputState.Kind)
	}

	return executeReturnProjection(input, stage.OutputState, stage.Plan, "MemoryReturnStage")
}
