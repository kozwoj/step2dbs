package builder

import (
	"fmt"
	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

type projectionFieldResolver func(item parser.ReturnItem) (*pipeline.FieldDef, int, error)

func resolveFieldFromState(state pipeline.StateDescription, tableName string, fieldName string) (*pipeline.FieldDef, int, error) {
	fieldDef, fieldIndex, exists := state.RecordDef.LookupField(tableName, fieldName)
	if !exists {
		return nil, 0, fmt.Errorf("field %s.%s is not available in current state %s", tableName, fieldName, state.TableName)
	}
	return fieldDef, fieldIndex, nil
}

func buildRecordDefinition(table *stepdb.TableDescription) pipeline.RecordDefinition {
	fields := make([]*pipeline.FieldDef, 0, len(table.RecordLayout.Fields))
	fieldIndex := make(map[string]int, len(table.RecordLayout.FieldIndex))
	qualifiedFieldIndex := make(map[string]int, len(table.RecordLayout.FieldIndex))

	for index, field := range table.RecordLayout.Fields {
		fields = append(fields, &pipeline.FieldDef{
			Name:            field.Name,
			SourceTableName: table.Name,
			SourceFieldName: field.Name,
			Type:            field.Type,
			IsForeignKey:    field.IsForeignKey,
			ForeignKeyTable: field.ForeignKeyTable,
		})
		fieldIndex[field.Name] = index
		qualifiedFieldIndex[pipeline.QualifiedFieldKey(table.Name, field.Name)] = index
	}

	return pipeline.RecordDefinition{
		NoFields:            table.RecordLayout.NoFields,
		PrimaryKey:          table.RecordLayout.PrimaryKey,
		Fields:              fields,
		FieldIndex:          fieldIndex,
		QualifiedFieldIndex: qualifiedFieldIndex,
	}
}

func buildProjection(items []parser.ReturnItem, outputTableName string, resolve projectionFieldResolver) ([]pipeline.ReturnFieldRef, pipeline.StateDescription, error) {
	returnItems := make([]pipeline.ReturnFieldRef, 0, len(items))
	outputFields := make([]*pipeline.FieldDef, 0, len(items))
	outputIndex := make(map[string]int, len(items))
	qualifiedOutputIndex := make(map[string]int, len(items))

	for position, item := range items {
		resolvedField, fieldIndex, err := resolve(item)
		if err != nil {
			return nil, pipeline.StateDescription{}, err
		}

		returnItems = append(returnItems, pipeline.ReturnFieldRef{
			Source: pipeline.QualifiedFieldRef{
				TableName: item.Field.Table,
				FieldName: item.Field.Name,
				Position:  fieldIndex,
				Type:      resolvedField.Type,
			},
			Alias: "",
		})

		outputFieldName := pipeline.QualifiedFieldKey(item.Field.Table, item.Field.Name)

		outputFields = append(outputFields, &pipeline.FieldDef{
			Name:            outputFieldName,
			SourceTableName: item.Field.Table,
			SourceFieldName: item.Field.Name,
			Type:            resolvedField.Type,
			IsForeignKey:    resolvedField.IsForeignKey,
			ForeignKeyTable: resolvedField.ForeignKeyTable,
		})
		outputIndex[outputFieldName] = position
		qualifiedOutputIndex[outputFieldName] = position
	}

	return returnItems, pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: outputTableName,
		RecordDef: pipeline.RecordDefinition{
			NoFields:            len(outputFields),
			PrimaryKey:          -1,
			Fields:              outputFields,
			FieldIndex:          outputIndex,
			QualifiedFieldIndex: qualifiedOutputIndex,
		},
	}, nil
}
