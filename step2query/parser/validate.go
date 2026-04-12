package parser

import (
	"fmt"
	"strings"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

type ValidationError struct {
	Span    Span
	Message string
}

func (e *ValidationError) Error() string {
	return fmt.Sprintf("line %d, column %d: %s", e.Span.Start.Line, e.Span.Start.Column, e.Message)
}

func ValidateAST(query *Query, dbDef *stepdb.DBDefinition) error {
	if query == nil {
		return fmt.Errorf("query is nil")
	}
	if dbDef == nil {
		return fmt.Errorf("DBDefinition is nil")
	}

	validator := &validator{dbDef: dbDef}
	return validator.validateQuery(query)
}

type validator struct {
	dbDef *stepdb.DBDefinition
}

type availableField struct {
	table *stepdb.TableDescription
	field *stepdb.FieldDescription
}

type validationState struct {
	fields map[string]availableField
}

type valueClass string

const (
	valueClassNumeric valueClass = "numeric"
	valueClassString  valueClass = "string"
	valueClassBoolean valueClass = "boolean"
	valueClassDate    valueClass = "date"
	valueClassTime    valueClass = "time"
)

func (v *validator) validateQuery(query *Query) error {
	sourceTable, err := v.lookupTable(query.Source.Table, query.Source.Span, "source table")
	if err != nil {
		return err
	}

	state := newValidationStateFromTable(sourceTable)

	for _, stage := range query.Stages {
		nextState, err := v.validateStage(stage, state)
		if err != nil {
			return err
		}
		state = nextState
	}

	if len(query.Stages) > 0 {
		if whereStage, ok := query.Stages[len(query.Stages)-1].(*WhereStage); ok {
			return v.errorf(whereStage.Span, "pipeline cannot end with where; add an explicit return stage or end with navigate")
		}
	}

	return nil
}

func (v *validator) validateStage(stage Stage, state validationState) (validationState, error) {
	switch typed := stage.(type) {
	case *WhereStage:
		if err := v.validateBoolExpr(typed.Expr, state); err != nil {
			return validationState{}, err
		}
		return state, nil
	case *NavigateFKStage:
		return v.validateNavigateFKStage(typed, state)
	case *NavigateSetStage:
		return v.validateNavigateSetStage(typed, state)
	case *ReturnStage:
		return v.validateReturnStage(typed, state)
	default:
		return validationState{}, fmt.Errorf("unsupported stage type %T", stage)
	}
}

func (v *validator) validateNavigateFKStage(stage *NavigateFKStage, state validationState) (validationState, error) {
	targetTable, err := v.lookupTable(stage.TargetTable, stage.Span, "navigate target table")
	if err != nil {
		return validationState{}, err
	}

	leftCurrent, leftCurrentOK := state.lookup(stage.Join.Left.Table, stage.Join.Left.Name)
	rightCurrent, rightCurrentOK := state.lookup(stage.Join.Right.Table, stage.Join.Right.Name)
	leftIsTarget := stage.Join.Left.Table == targetTable.Name
	rightIsTarget := stage.Join.Right.Table == targetTable.Name

	var sourceTable *stepdb.TableDescription
	var sourceField *stepdb.FieldDescription
	var targetJoinTable *stepdb.TableDescription
	var targetJoinField *stepdb.FieldDescription

	switch {
	case leftCurrentOK && rightIsTarget:
		targetJoinTable, targetJoinField, err = v.lookupField(stage.Join.Right.Table, stage.Join.Right.Name, stage.Join.Right.Span)
		if err != nil {
			return validationState{}, err
		}
		sourceTable, sourceField = leftCurrent.table, leftCurrent.field
	case rightCurrentOK && leftIsTarget:
		targetJoinTable, targetJoinField, err = v.lookupField(stage.Join.Left.Table, stage.Join.Left.Name, stage.Join.Left.Span)
		if err != nil {
			return validationState{}, err
		}
		sourceTable, sourceField = rightCurrent.table, rightCurrent.field
	case rightIsTarget:
		return validationState{}, v.unavailableFieldError(stage.Join.Left, state)
	case leftIsTarget:
		return validationState{}, v.unavailableFieldError(stage.Join.Right, state)
	default:
		return validationState{}, v.errorf(stage.Join.Span, "navigate join must reference one field from the current pipeline state and one field from target table %s", targetTable.Name)
	}

	if !classesCompatible(classifyFieldType(sourceField.Type), classifyFieldType(targetJoinField.Type)) {
		return validationState{}, v.errorf(stage.Join.Span, "join fields %s.%s and %s.%s have incompatible types %s and %s",
			sourceTable.Name, sourceField.Name, targetJoinTable.Name, targetJoinField.Name, fieldTypeName(sourceField.Type), fieldTypeName(targetJoinField.Type))
	}

	if !isForeignKeyToPrimaryKey(sourceTable, sourceField, targetJoinTable, targetJoinField) &&
		!isForeignKeyToPrimaryKey(targetJoinTable, targetJoinField, sourceTable, sourceField) {
		return validationState{}, v.errorf(stage.Join.Span, "join condition %s.%s == %s.%s is not a valid foreign-key to primary-key relationship",
			sourceTable.Name, sourceField.Name, targetJoinTable.Name, targetJoinField.Name)
	}

	accessible := state.withTable(targetTable)
	return v.validateProjectedReturnItems(stage.Return, accessible)
}

func (v *validator) validateNavigateSetStage(stage *NavigateSetStage, state validationState) (validationState, error) {
	table, err := v.lookupTable(stage.SetRef.Table, stage.SetRef.Span, "set owner table")
	if err != nil {
		return validationState{}, err
	}

	setIndex, exists := table.SetIndex[stage.SetRef.Name]
	if !exists {
		return validationState{}, v.errorf(stage.SetRef.Span, "set %s does not exist in table %s", stage.SetRef.Name, stage.SetRef.Table)
	}
	set := table.Sets[setIndex]
	memberTable, err := v.lookupTable(set.MemberTableName, stage.SetRef.Span, "set member table")
	if err != nil {
		return validationState{}, err
	}

	accessible := state.withTable(memberTable)
	return v.validateProjectedReturnItems(stage.Return, accessible)
}

func (v *validator) validateReturnStage(stage *ReturnStage, state validationState) (validationState, error) {
	return v.validateProjectedReturnItems(stage.Items, state)
}

func (v *validator) validateProjectedReturnItems(items []ReturnItem, state validationState) (validationState, error) {
	nextState := validationState{fields: make(map[string]availableField, len(items))}
	for _, item := range items {
		available, ok := state.lookup(item.Field.Table, item.Field.Name)
		if !ok {
			return validationState{}, v.unavailableFieldError(item.Field, state)
		}
		nextState.fields[fieldKey(item.Field.Table, item.Field.Name)] = available
	}
	return nextState, nil
}

func (v *validator) validateBoolExpr(expr BoolExpr, state validationState) error {
	switch typed := expr.(type) {
	case *OrExpr:
		if err := v.validateBoolExpr(typed.Left, state); err != nil {
			return err
		}
		return v.validateBoolExpr(typed.Right, state)
	case *AndExpr:
		if err := v.validateBoolExpr(typed.Left, state); err != nil {
			return err
		}
		return v.validateBoolExpr(typed.Right, state)
	case *NotExpr:
		return v.validateBoolExpr(typed.Expr, state)
	case *CompareExpr:
		return v.validateCompareExpr(typed, state)
	default:
		return fmt.Errorf("unsupported boolean expression type %T", expr)
	}
}

func (v *validator) validateCompareExpr(expr *CompareExpr, state validationState) error {
	leftClass, leftType, err := v.resolveValueExprType(expr.Left, state)
	if err != nil {
		return err
	}
	rightClass, rightType, err := v.resolveValueExprType(expr.Right, state)
	if err != nil {
		return err
	}

	if expr.Op == CompareLike {
		if leftClass != valueClassString {
			return v.errorf(expr.Left.NodeSpan(), "operator like requires a string-compatible left operand, found %s", fieldTypeName(leftType))
		}
		if rightClass != valueClassString {
			return v.errorf(expr.Right.NodeSpan(), "operator like requires a string-compatible right operand, found %s", fieldTypeName(rightType))
		}

		literalExpr, ok := expr.Right.(*LiteralExpr)
		if !ok {
			return v.errorf(expr.Right.NodeSpan(), "operator like currently requires a quoted string pattern on the right-hand side")
		}
		stringLiteral, ok := literalExpr.Literal.(*StringLiteral)
		if !ok {
			return v.errorf(expr.Right.NodeSpan(), "operator like currently requires a quoted string pattern on the right-hand side")
		}
		if err := validatePrefixPattern(stringLiteral.Value); err != nil {
			return v.errorf(stringLiteral.Span, "%s", err.Error())
		}
		return nil
	}

	if !classesCompatible(leftClass, rightClass) {
		return v.errorf(expr.Span, "operator %s cannot compare %s to %s", expr.Op, fieldTypeName(leftType), fieldTypeName(rightType))
	}

	if !operatorAllowedForClass(expr.Op, leftClass) {
		return v.errorf(expr.Span, "operator %s is not supported for %s operands", expr.Op, leftClass)
	}

	if err := validateLiteralFormatForField(expr.Left, rightType); err != nil {
		return err
	}
	if err := validateLiteralFormatForField(expr.Right, leftType); err != nil {
		return err
	}

	return nil
}

func validateLiteralFormatForField(expr ValueExpr, oppositeType stepdb.FieldType) error {
	literalExpr, ok := expr.(*LiteralExpr)
	if !ok {
		return nil
	}

	switch literal := literalExpr.Literal.(type) {
	case *NumberLiteral:
		if oppositeType == stepdb.DECIMAL && !record.IsDecimalString(literal.Value) {
			return &ValidationError{Span: literal.Span, Message: fmt.Sprintf("invalid DECIMAL literal format %q", literal.Value)}
		}
	case *DateLiteral:
		if _, err := record.ParseDate(literal.Value); err != nil {
			return &ValidationError{Span: literal.Span, Message: fmt.Sprintf("invalid DATE literal format %q", literal.Value)}
		}
	case *TimeLiteral:
		if _, err := record.ConvertCompactTime(literal.Value); err != nil {
			return &ValidationError{Span: literal.Span, Message: fmt.Sprintf("invalid TIME literal format %q", literal.Value)}
		}
	}

	return nil
}

func (v *validator) resolveValueExprType(expr ValueExpr, state validationState) (valueClass, stepdb.FieldType, error) {
	switch typed := expr.(type) {
	case *FieldRef:
		available, ok := state.lookup(typed.Field.Table, typed.Field.Name)
		if !ok {
			return "", 0, v.unavailableFieldError(typed.Field, state)
		}
		return classifyFieldType(available.field.Type), available.field.Type, nil
	case *LiteralExpr:
		switch literal := typed.Literal.(type) {
		case *StringLiteral:
			return valueClassString, stepdb.STRING, nil
		case *NumberLiteral:
			return valueClassNumeric, stepdb.DECIMAL, nil
		case *BooleanLiteral:
			return valueClassBoolean, stepdb.BOOLEAN, nil
		case *DateLiteral:
			return valueClassDate, stepdb.DATE, nil
		case *TimeLiteral:
			return valueClassTime, stepdb.TIME, nil
		default:
			return "", 0, fmt.Errorf("unsupported literal type %T", literal)
		}
	default:
		return "", 0, fmt.Errorf("unsupported value expression type %T", expr)
	}
}

func (v *validator) lookupTable(tableName string, span Span, description string) (*stepdb.TableDescription, error) {
	index, exists := v.dbDef.TableIndex[tableName]
	if !exists {
		return nil, v.errorf(span, "%s %s does not exist", description, tableName)
	}
	return v.dbDef.Tables[index], nil
}

func (v *validator) lookupField(tableName, fieldName string, span Span) (*stepdb.TableDescription, *stepdb.FieldDescription, error) {
	table, err := v.lookupTable(tableName, span, "table")
	if err != nil {
		return nil, nil, err
	}

	index, exists := table.RecordLayout.FieldIndex[fieldName]
	if !exists {
		return nil, nil, v.errorf(span, "field %s does not exist in table %s", fieldName, tableName)
	}

	return table, table.RecordLayout.Fields[index], nil
}

func (v *validator) unavailableFieldError(field QualifiedIdent, state validationState) error {
	if _, exists := v.dbDef.TableIndex[field.Table]; !exists {
		return v.errorf(field.Span, "table %s does not exist", field.Table)
	}
	if _, _, err := v.lookupField(field.Table, field.Name, field.Span); err != nil {
		return err
	}
	return v.errorf(field.Span, "field %s.%s is not available in the current pipeline state", field.Table, field.Name)
}

func (v *validator) errorf(span Span, format string, args ...any) error {
	return &ValidationError{Span: span, Message: fmt.Sprintf(format, args...)}
}

func classifyFieldType(fieldType stepdb.FieldType) valueClass {
	switch fieldType {
	case stepdb.SMALLINT, stepdb.INT, stepdb.BIGINT, stepdb.DECIMAL, stepdb.FLOAT:
		return valueClassNumeric
	case stepdb.STRING, stepdb.CHAR:
		return valueClassString
	case stepdb.BOOLEAN:
		return valueClassBoolean
	case stepdb.DATE:
		return valueClassDate
	case stepdb.TIME:
		return valueClassTime
	default:
		return valueClass(fmt.Sprintf("unknown(%d)", fieldType))
	}
}

func classesCompatible(left, right valueClass) bool {
	return left == right
}

func operatorAllowedForClass(op CompareOp, class valueClass) bool {
	switch class {
	case valueClassBoolean, valueClassString:
		return op == CompareEq || op == CompareNe
	case valueClassNumeric, valueClassDate, valueClassTime:
		return op == CompareEq || op == CompareNe || op == CompareLt || op == CompareLe || op == CompareGt || op == CompareGe
	default:
		return false
	}
}

func isForeignKeyToPrimaryKey(sourceTable *stepdb.TableDescription, sourceField *stepdb.FieldDescription, targetTable *stepdb.TableDescription, targetField *stepdb.FieldDescription) bool {
	if !sourceField.IsForeignKey || sourceField.ForeignKeyTable != targetTable.Name || targetTable.Key == -1 {
		return false
	}
	primaryKeyField := targetTable.RecordLayout.Fields[targetTable.Key]
	return primaryKeyField.Name == targetField.Name
}

func fieldTypeName(fieldType stepdb.FieldType) string {
	switch fieldType {
	case stepdb.SMALLINT:
		return "SMALLINT"
	case stepdb.INT:
		return "INT"
	case stepdb.BIGINT:
		return "BIGINT"
	case stepdb.DECIMAL:
		return "DECIMAL"
	case stepdb.FLOAT:
		return "FLOAT"
	case stepdb.STRING:
		return "STRING"
	case stepdb.CHAR:
		return "CHAR"
	case stepdb.BOOLEAN:
		return "BOOLEAN"
	case stepdb.DATE:
		return "DATE"
	case stepdb.TIME:
		return "TIME"
	default:
		return fmt.Sprintf("FieldType(%d)", fieldType)
	}
}

func validatePrefixPattern(pattern string) error {
	if !strings.HasSuffix(pattern, "*") {
		return fmt.Errorf("like pattern %q must end with '*'", pattern)
	}
	prefix := strings.TrimSuffix(pattern, "*")
	if len(prefix) == 0 {
		return fmt.Errorf("like pattern %q must contain at least one character before '*'", pattern)
	}
	if len(prefix) > 8 {
		return fmt.Errorf("like pattern %q exceeds the maximum prefix length of 8 characters", pattern)
	}
	return nil
}

func newValidationStateFromTable(table *stepdb.TableDescription) validationState {
	state := validationState{fields: make(map[string]availableField, len(table.RecordLayout.Fields))}
	for _, field := range table.RecordLayout.Fields {
		state.fields[fieldKey(table.Name, field.Name)] = availableField{table: table, field: field}
	}
	return state
}

func (s validationState) lookup(tableName, fieldName string) (availableField, bool) {
	available, ok := s.fields[fieldKey(tableName, fieldName)]
	return available, ok
}

func (s validationState) withTable(table *stepdb.TableDescription) validationState {
	combined := validationState{fields: make(map[string]availableField, len(s.fields)+len(table.RecordLayout.Fields))}
	for key, value := range s.fields {
		combined.fields[key] = value
	}
	for _, field := range table.RecordLayout.Fields {
		combined.fields[fieldKey(table.Name, field.Name)] = availableField{table: table, field: field}
	}
	return combined
}

func fieldKey(tableName, fieldName string) string {
	return tableName + "." + fieldName
}
