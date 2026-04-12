package builder

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

func buildNavigateSetStageDefinition(stage *parser.NavigateSetStage, currentState pipeline.StateDescription, dbDef *stepdb.DBDefinition) (pipeline.Stage, error) {
	if stage == nil {
		return nil, fmt.Errorf("navigate set stage is nil")
	}
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	if currentState.Kind == pipeline.StateReturnWorkingSet {
		return nil, fmt.Errorf("navigate set is not supported for return working set input")
	}

	if stage.SetRef.Table != currentState.TableName {
		return nil, fmt.Errorf("navigate set owner table %s does not match current state table %s", stage.SetRef.Table, currentState.TableName)
	}

	ownerTableIndex, exists := dbDef.TableIndex[stage.SetRef.Table]
	if !exists {
		return nil, fmt.Errorf("set owner table %s does not exist", stage.SetRef.Table)
	}
	ownerTable := dbDef.Tables[ownerTableIndex]

	setIndex, exists := ownerTable.SetIndex[stage.SetRef.Name]
	if !exists {
		return nil, fmt.Errorf("set %s does not exist in table %s", stage.SetRef.Name, stage.SetRef.Table)
	}
	setDef := ownerTable.Sets[setIndex]

	targetTableIndex, exists := dbDef.TableIndex[setDef.MemberTableName]
	if !exists {
		return nil, fmt.Errorf("set member table %s does not exist", setDef.MemberTableName)
	}
	targetTable := dbDef.Tables[targetTableIndex]

	returnItems, outputState, err := buildProjection(stage.Return, targetTable.Name, func(item parser.ReturnItem) (*pipeline.FieldDef, int, error) {
		return resolveNavigateSetReturnField(item, currentState, targetTable)
	})
	if err != nil {
		return nil, err
	}

	return &pipeline.DBNavigateSetStage{
		StageBase: pipeline.StageBase{
			StageKind:   pipeline.StageDBNavigateSet,
			InputState:  currentState,
			OutputState: outputState,
		},
		Plan: pipeline.SetNavigationPlan{
			SourceTable: currentState.TableName,
			TargetTable: targetTable.Name,
			SetName:     stage.SetRef.Name,
			ReturnItems: returnItems,
		},
	}, nil
}

func resolveNavigateSetReturnField(item parser.ReturnItem, currentState pipeline.StateDescription, targetTable *stepdb.TableDescription) (*pipeline.FieldDef, int, error) {
	if item.Field.Table == currentState.TableName {
		return resolveFieldFromState(currentState, item.Field.Table, item.Field.Name)
	}

	if item.Field.Table != targetTable.Name {
		return nil, 0, fmt.Errorf("field %s.%s is not available for navigate set from %s to %s", item.Field.Table, item.Field.Name, currentState.TableName, targetTable.Name)
	}

	fieldIndex, exists := targetTable.RecordLayout.FieldIndex[item.Field.Name]
	if !exists {
		return nil, 0, fmt.Errorf("field %s does not exist in target table %s", item.Field.Name, targetTable.Name)
	}
	field := targetTable.RecordLayout.Fields[fieldIndex]
	return &pipeline.FieldDef{
		Name:            field.Name,
		SourceTableName: targetTable.Name,
		SourceFieldName: field.Name,
		Type:            field.Type,
		IsForeignKey:    field.IsForeignKey,
		ForeignKeyTable: field.ForeignKeyTable,
	}, fieldIndex, nil
}
