package engine

import (
	"errors"
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
	"github.com/kozwoj/step2query/pipeline"
)

/*
	executeNavigateFK drives FK navigation for one stage execution.

- Input parameters:
  - input is the current runtime state being iterated (DB-backed or in-memory).
  - outputState is the builder-produced ReturnWorkingSet description for the stage output.
  - plan is the resolved FK navigation plan.
  - stageName is used to prefix execution errors.

- Output:
  - the fully materialized ReturnWorkingSet produced by following the FK for each input row.
  - error when iteration, FK lookup, target-row loading, or projection fails.

The function is responsible for iterating the input state, loading one target record per input row,
projecting the requested return fields, and constructing the final ReturnWorkingSet.
*/
func executeNavigateFK(input State, outputState pipeline.StateDescription, plan pipeline.FKNavigationPlan, stageName string) (State, error) {
	inputDef := input.RecordDef()
	targetDef := buildRecordDefinition(input.DBDef().Tables[input.DBDef().TableIndex[plan.TargetTable]])
	outputDef := outputState.RecordDef

	projectedRecords := make([]map[string]interface{}, 0, input.Size())

	currentRecord, err := input.GetFirstRecord()
	if err != nil {
		if errors.Is(err, ErrNoMoreRecords) {
			return NewReturnWorkingSet(outputState.TableName, outputDef, input.DBDef(), projectedRecords), nil
		}
		return nil, fmt.Errorf("%s failed to get first record from input state: %w", stageName, err)
	}

	for currentRecord != nil {
		targetRecord, err := loadNavigateFKTargetRecord(plan, inputDef, currentRecord, input.DBDef())
		if err != nil {
			return nil, fmt.Errorf("%s failed to load target record: %w", stageName, err)
		}

		outputRecord, err := buildNavigateOutputRecord(plan.ReturnItems, inputDef, targetDef, outputDef, currentRecord, targetRecord)
		if err != nil {
			return nil, fmt.Errorf("%s failed to build output record: %w", stageName, err)
		}
		projectedRecords = append(projectedRecords, outputRecord)

		currentRecord, err = input.GetNextRecord()
		if err != nil {
			if errors.Is(err, ErrNoMoreRecords) {
				break
			}
			return nil, fmt.Errorf("%s failed to get next record from input state: %w", stageName, err)
		}
	}

	return NewReturnWorkingSet(outputState.TableName, outputDef, input.DBDef(), projectedRecords), nil
}

/*
	loadNavigateFKTargetRecord resolves and loads the target row referenced by the current input row.

- Input parameters:
  - plan is the resolved FK navigation plan.
  - inputDef is the runtime record definition of the current input row.
  - inputRecord is the current input row whose FK value will be read.
  - dbDef is the opened STEP2 database definition used for target-table access.

- Output:
  - the materialized target record loaded from the target table.
  - error when the FK field cannot be resolved, the FK value cannot be read, the target primary-key lookup fails, or the target record cannot be loaded.

The function should read the FK value from the input row, use STEP2 primary-index lookup on the target table,
and then load the target record body by record ID.
*/
func loadNavigateFKTargetRecord(plan pipeline.FKNavigationPlan, inputDef RecordDefinition, inputRecord map[string]interface{}, dbDef *stepdb.DBDefinition) (map[string]interface{}, error) {
	// Resolve the FK field definition from the input record schema.
	fkFieldDef, _, exists := inputDef.LookupField(plan.ForeignKey.TableName, plan.ForeignKey.FieldName)
	if !exists {
		return nil, fmt.Errorf("FK field %s.%s not found in input record definition", plan.ForeignKey.TableName, plan.ForeignKey.FieldName)
	}

	// Read the FK value from the input row.
	fkValue, found := lookupRuntimeFieldValue(inputRecord, fkFieldDef)
	if !found {
		return nil, fmt.Errorf("FK field %s.%s has no value in input record", plan.ForeignKey.TableName, plan.ForeignKey.FieldName)
	}

	// Look up the target record ID via the primary index.
	targetRecordID, err := record.GetRecordID(plan.TargetTable, fkValue, dbDef)
	if err != nil {
		return nil, fmt.Errorf("primary-key lookup on %s for FK value %v failed: %w", plan.TargetTable, fkValue, err)
	}

	// Load the full target record.
	targetRecord, err := record.GetRecordByID(plan.TargetTable, targetRecordID, dbDef)
	if err != nil {
		return nil, fmt.Errorf("failed to load record %d from %s: %w", targetRecordID, plan.TargetTable, err)
	}

	return targetRecord, nil
}

/*
	buildNavigateOutputRecord constructs one output row for navigation (FK or set).

- Input parameters:
  - items are the return items in builder-defined output order.
  - inputDef is the runtime record definition of the current input row.
  - targetDef is the runtime record definition of the navigated target row.
  - outputDef is the builder-produced output record definition.
  - inputRecord is the current input row.
  - targetRecord is the row reached by FK navigation.

- Output:
  - one projected output row using the field names defined by outputDef.
  - error when a requested return field cannot be resolved against either the input or target runtime schema.

The function should inspect each return item, decide whether the value comes from the input row or the target row,
read the runtime value, and write it to the qualified output field name at the matching output position.
*/
func buildNavigateOutputRecord(items []pipeline.ReturnFieldRef, inputDef RecordDefinition, targetDef RecordDefinition, outputDef RecordDefinition, inputRecord map[string]interface{}, targetRecord map[string]interface{}) (map[string]interface{}, error) {
	// Validate that the return plan and output schema stay aligned.
	if len(items) != len(outputDef.Fields) {
		return nil, fmt.Errorf("navigate return/output schema length mismatch: %d items, %d fields", len(items), len(outputDef.Fields))
	}

	outputRecord := make(map[string]interface{}, len(items))

	// Resolve each return item against either the input row or the target row.
	for index, item := range items {
		outputFieldDef := outputDef.Fields[index]
		if outputFieldDef == nil {
			return nil, fmt.Errorf("navigate output field definition at position %d is nil", index)
		}

		fieldDef, _, exists := targetDef.LookupField(item.Source.TableName, item.Source.FieldName)
		sourceRecord := targetRecord
		if !exists {
			fieldDef, _, exists = inputDef.LookupField(item.Source.TableName, item.Source.FieldName)
			sourceRecord = inputRecord
		}
		if !exists {
			return nil, fmt.Errorf("navigate source field %s.%s is not present in input or target runtime record definition", item.Source.TableName, item.Source.FieldName)
		}

		// Copy the runtime value to the builder-defined output field name.
		if value, exists := lookupRuntimeFieldValue(sourceRecord, fieldDef); exists {
			outputRecord[outputFieldDef.Name] = value
		}
	}

	return outputRecord, nil
}
