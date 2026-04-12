package engine

import (
	"errors"
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
	"github.com/kozwoj/step2query/pipeline"
)

/*
	executeNavigateSet drives set navigation for one stage execution.

- Input parameters:
  - input is the current runtime state being iterated (DB-backed only).
  - outputState is the builder-produced ReturnWorkingSet description for the stage output.
  - plan is the resolved set navigation plan.
  - stageName is used to prefix execution errors.

- Output:
  - the fully materialized ReturnWorkingSet produced by expanding set members for each input row.
  - error when iteration, set member lookup, target-row loading, or projection fails.

The function iterates the input state. For each input row it loads the set member record IDs,
materializes each member record, projects the requested return fields from the (input, member) pair,
and appends the result to the output. The output grows multiplicatively (1:N per input row).
*/
func executeNavigateSet(input State, outputState pipeline.StateDescription, plan pipeline.SetNavigationPlan, stageName string) (State, error) {
	inputDef := input.RecordDef()
	targetDef := buildRecordDefinition(input.DBDef().Tables[input.DBDef().TableIndex[plan.TargetTable]])
	outputDef := outputState.RecordDef

	projectedRecords := make([]map[string]interface{}, 0)

	currentRecord, err := input.GetFirstRecord()
	if err != nil {
		if errors.Is(err, ErrNoMoreRecords) {
			return NewReturnWorkingSet(outputState.TableName, outputDef, input.DBDef(), projectedRecords), nil
		}
		return nil, fmt.Errorf("%s failed to get first record from input state: %w", stageName, err)
	}

	for currentRecord != nil {
		ownerRecordID := input.CurrentRecordID()

		memberRecords, err := loadNavigateSetTargetRecords(plan, ownerRecordID, input.DBDef())
		if err != nil {
			return nil, fmt.Errorf("%s failed to load set members for owner record %d: %w", stageName, ownerRecordID, err)
		}

		for _, memberRecord := range memberRecords {
			outputRecord, err := buildNavigateOutputRecord(plan.ReturnItems, inputDef, targetDef, outputDef, currentRecord, memberRecord)
			if err != nil {
				return nil, fmt.Errorf("%s failed to build output record: %w", stageName, err)
			}
			projectedRecords = append(projectedRecords, outputRecord)
		}

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
	loadNavigateSetTargetRecords loads all member records for a given owner record and set.

- Input parameters:
  - plan is the resolved set navigation plan (carries SourceTable, TargetTable, SetName).
  - ownerRecordID is the record ID of the current owner row in the source table.
  - dbDef is the opened STEP2 database definition used for set and target-table access.

- Output:
  - a slice of materialized member records from the target table.
  - error when set member lookup fails or a member record cannot be loaded.

The function calls record.GetSetMembers to obtain the member record IDs, then loads each
member record from the target table with record.GetRecordByID.
*/
func loadNavigateSetTargetRecords(plan pipeline.SetNavigationPlan, ownerRecordID uint32, dbDef *stepdb.DBDefinition) ([]map[string]interface{}, error) {
	memberIDs, err := record.GetSetMembers(plan.SourceTable, ownerRecordID, plan.SetName, dbDef)
	if err != nil {
		return nil, fmt.Errorf("set member lookup on %s.%s for owner record %d failed: %w", plan.SourceTable, plan.SetName, ownerRecordID, err)
	}

	members := make([]map[string]interface{}, 0, len(memberIDs))
	for _, memberID := range memberIDs {
		memberRecord, err := record.GetRecordByID(plan.TargetTable, memberID, dbDef)
		if err != nil {
			return nil, fmt.Errorf("failed to load member record %d from %s: %w", memberID, plan.TargetTable, err)
		}
		members = append(members, memberRecord)
	}

	return members, nil
}
