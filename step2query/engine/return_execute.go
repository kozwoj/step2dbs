package engine

import (
	"errors"
	"fmt"

	"github.com/kozwoj/step2query/pipeline"
)

func executeReturnProjection(input State, outputState pipeline.StateDescription, plan pipeline.ReturnPlan, stageName string) (State, error) {
	projectedRecords := make([]map[string]interface{}, 0, input.Size())

	currentRecord, err := input.GetFirstRecord()
	if err != nil {
		if errors.Is(err, ErrNoMoreRecords) {
			return NewReturnWorkingSet(outputState.TableName, outputState.RecordDef, input.DBDef(), projectedRecords), nil
		}
		return nil, fmt.Errorf("%s failed to get first record from input state: %w", stageName, err)
	}

	for currentRecord != nil {
		projectedRecord, err := buildProjectedRecord(plan.Items, input.RecordDef(), outputState.RecordDef, currentRecord)
		if err != nil {
			return nil, fmt.Errorf("%s failed to project record: %w", stageName, err)
		}
		projectedRecords = append(projectedRecords, projectedRecord)

		currentRecord, err = input.GetNextRecord()
		if err != nil {
			if errors.Is(err, ErrNoMoreRecords) {
				break
			}
			return nil, fmt.Errorf("%s failed to get next record from input state: %w", stageName, err)
		}
	}

	return NewReturnWorkingSet(outputState.TableName, outputState.RecordDef, input.DBDef(), projectedRecords), nil
}

func buildProjectedRecord(items []pipeline.ReturnFieldRef, inputDef RecordDefinition, outputDef RecordDefinition, inputRecord map[string]interface{}) (map[string]interface{}, error) {
	if len(items) != len(outputDef.Fields) {
		return nil, fmt.Errorf("return plan/output schema length mismatch: %d items, %d fields", len(items), len(outputDef.Fields))
	}

	projected := make(map[string]interface{}, len(items))
	for index, item := range items {
		inputFieldDef, _, exists := inputDef.LookupField(item.Source.TableName, item.Source.FieldName)
		if !exists {
			return nil, fmt.Errorf("return source field %s.%s is not present in runtime record definition", item.Source.TableName, item.Source.FieldName)
		}

		outputFieldName := outputDef.Fields[index].Name
		if value, exists := lookupRuntimeFieldValue(inputRecord, inputFieldDef); exists {
			projected[outputFieldName] = value
		}
	}

	return projected, nil
}