package builder

import (
	"fmt"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

func buildNavigateFKStageDefinition(stage *parser.NavigateFKStage, currentState pipeline.StateDescription, dbDef *stepdb.DBDefinition) (pipeline.Stage, error) {
	if stage == nil {
		return nil, fmt.Errorf("navigate FK stage is nil")
	}
	if dbDef == nil {
		return nil, fmt.Errorf("DBDefinition is nil")
	}

	targetTableIndex, exists := dbDef.TableIndex[stage.TargetTable]
	if !exists {
		return nil, fmt.Errorf("target table %s does not exist", stage.TargetTable)
	}
	targetTable := dbDef.Tables[targetTableIndex]

	sourceRef, targetRef, err := resolveNavigateFKJoin(stage, currentState, dbDef, targetTable)
	if err != nil {
		return nil, err
	}

	returnItems, outputState, err := buildProjection(stage.Return, targetTable.Name, func(item parser.ReturnItem) (*pipeline.FieldDef, int, error) {
		return resolveNavigateFKReturnField(item, currentState, targetTable)
	})
	if err != nil {
		return nil, err
	}

	plan := pipeline.FKNavigationPlan{
		SourceTable: sourceRef.TableName,
		TargetTable: targetTable.Name,
		ForeignKey:  sourceRef,
		PrimaryKey:  targetRef,
		ReturnItems: returnItems,
	}

	switch currentState.Kind {
	case pipeline.StateSourceDBTableSet, pipeline.StateDBTableWorkingSet:
		return &pipeline.DBNavigateFKStage{
			StageBase: pipeline.StageBase{
				StageKind:   pipeline.StageDBNavigateFK,
				InputState:  currentState,
				OutputState: outputState,
			},
			Plan: plan,
		}, nil
	case pipeline.StateReturnWorkingSet:
		return &pipeline.MemoryNavigateFKStage{
			StageBase: pipeline.StageBase{
				StageKind:   pipeline.StageMemoryNavigateFK,
				InputState:  currentState,
				OutputState: outputState,
			},
			Plan: plan,
		}, nil
	default:
		return nil, fmt.Errorf("navigate FK is not supported for state kind %d", currentState.Kind)
	}
}

func resolveNavigateFKJoin(stage *parser.NavigateFKStage, currentState pipeline.StateDescription, dbDef *stepdb.DBDefinition, targetTable *stepdb.TableDescription) (pipeline.QualifiedFieldRef, pipeline.QualifiedFieldRef, error) {
	leftIsTarget := stage.Join.Left.Table == targetTable.Name
	rightIsTarget := stage.Join.Right.Table == targetTable.Name

	leftField, leftIndex, leftExists := currentState.RecordDef.LookupField(stage.Join.Left.Table, stage.Join.Left.Name)
	rightField, rightIndex, rightExists := currentState.RecordDef.LookupField(stage.Join.Right.Table, stage.Join.Right.Name)

	var currentJoin parser.QualifiedIdent
	var currentFieldIndex int
	var currentField *pipeline.FieldDef
	var targetJoin parser.QualifiedIdent
	var targetJoinField *stepdb.FieldDescription

	switch {
	case leftExists && rightIsTarget:
		currentJoin = stage.Join.Left
		currentFieldIndex = leftIndex
		currentField = leftField
		targetJoin = stage.Join.Right
	case rightExists && leftIsTarget:
		currentJoin = stage.Join.Right
		currentFieldIndex = rightIndex
		currentField = rightField
		targetJoin = stage.Join.Left
	default:
		return pipeline.QualifiedFieldRef{}, pipeline.QualifiedFieldRef{}, fmt.Errorf("navigate join must reference one field from current state and one field from target table %s", targetTable.Name)
	}

	targetFieldIndex, exists := targetTable.RecordLayout.FieldIndex[targetJoin.Name]
	if !exists {
		return pipeline.QualifiedFieldRef{}, pipeline.QualifiedFieldRef{}, fmt.Errorf("field %s does not exist in target table %s", targetJoin.Name, targetTable.Name)
	}
	targetJoinField = targetTable.RecordLayout.Fields[targetFieldIndex]

	currentSourceTableIndex, exists := dbDef.TableIndex[currentJoin.Table]
	if !exists {
		return pipeline.QualifiedFieldRef{}, pipeline.QualifiedFieldRef{}, fmt.Errorf("source table %s does not exist", currentJoin.Table)
	}
	currentSourceTable := dbDef.Tables[currentSourceTableIndex]

	currentRef := pipeline.QualifiedFieldRef{
		TableName: currentJoin.Table,
		FieldName: currentJoin.Name,
		Position:  currentFieldIndex,
		Type:      currentField.Type,
	}
	targetRef := pipeline.QualifiedFieldRef{
		TableName: targetJoin.Table,
		FieldName: targetJoin.Name,
		Position:  targetFieldIndex,
		Type:      targetJoinField.Type,
	}

	if currentField.IsForeignKey && currentField.ForeignKeyTable == targetTable.Name && currentSourceTable.Key >= 0 {
		primaryField := targetTable.RecordLayout.Fields[targetTable.Key]
		if primaryField.Name == targetJoinField.Name {
			return currentRef, targetRef, nil
		}
	}

	if targetJoinField.IsForeignKey && targetJoinField.ForeignKeyTable == currentJoin.Table && currentSourceTable.Key >= 0 {
		primaryField := currentSourceTable.RecordLayout.Fields[currentSourceTable.Key]
		if primaryField.Name == currentField.Name {
			return pipeline.QualifiedFieldRef{
					TableName: targetJoin.Table,
					FieldName: targetJoin.Name,
					Position:  targetFieldIndex,
					Type:      targetJoinField.Type,
				}, pipeline.QualifiedFieldRef{
					TableName: currentJoin.Table,
					FieldName: currentJoin.Name,
					Position:  currentFieldIndex,
					Type:      currentField.Type,
				}, nil
		}
	}

	return pipeline.QualifiedFieldRef{}, pipeline.QualifiedFieldRef{}, fmt.Errorf("join condition %s.%s == %s.%s is not a valid foreign-key to primary-key relationship", currentJoin.Table, currentJoin.Name, targetJoin.Table, targetJoin.Name)
}

func resolveNavigateFKReturnField(item parser.ReturnItem, currentState pipeline.StateDescription, targetTable *stepdb.TableDescription) (*pipeline.FieldDef, int, error) {
	if item.Field.Table != targetTable.Name {
		return resolveFieldFromState(currentState, item.Field.Table, item.Field.Name)
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
