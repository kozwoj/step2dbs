package engine

import (
	"fmt"
	"sort"
	"strconv"
	"strings"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

/*
	evaluateBoolExpr evaluates one validated boolean expression against one runtime record. This function does
	not perform DB access, dictionary lookup, index lookup, or any other optimization work. It is pure record-local
	evaluation reused by scan and residual-filter execution paths.

- Input parameters:
  - expr is the validated parser.BoolExpr subtree representing the predicate to evaluate
  - record is the uniform runtime representation of the input record
  - recordDef is the builder-produced definition of the record structure

- Output:
  - boolean result when evaluation succeeds.
  - error only when the expression cannot be evaluated against the supplied record and record definition because of an engine/runtime inconsistency.

note: the function assumes parser validation has already enforced class compatibility and operator legality.
*/
func evaluateBoolExpr(expr parser.BoolExpr, record map[string]interface{}, recordDef RecordDefinition) (bool, error) {
	switch typed := expr.(type) {
	case *parser.AndExpr:
		// for AND, evaluate left expression first and short-circuit if false
		left, err := evaluateBoolExpr(typed.Left, record, recordDef)
		if err != nil {
			return false, err
		}
		if !left {
			return false, nil
		}
		return evaluateBoolExpr(typed.Right, record, recordDef)

	case *parser.OrExpr:
		// for OR, evaluate left expression first and short-circuit if true
		left, err := evaluateBoolExpr(typed.Left, record, recordDef)
		if err != nil {
			return false, err
		}
		if left {
			return true, nil
		}
		return evaluateBoolExpr(typed.Right, record, recordDef)

	case *parser.NotExpr:
		// for NOT, evaluate the child expression and negate the result
		value, err := evaluateBoolExpr(typed.Expr, record, recordDef)
		if err != nil {
			return false, err
		}
		return !value, nil

	case *parser.CompareExpr:
		// for CompareExpr, evaluate left and right value expressions and then apply the operator
		leftValue, leftType, err := evaluateValueExpr(typed.Left, record, recordDef)
		if err != nil {
			return false, err
		}
		rightValue, rightType, err := evaluateValueExpr(typed.Right, record, recordDef)
		if err != nil {
			return false, err
		}

		// Missing runtime values currently behave as non-matching predicates.
		if leftValue == nil || rightValue == nil {
			return false, nil
		}
		// Special case for LIKE operator
		if typed.Op == parser.CompareLike {
			return evaluateLikeComparison(leftValue, rightValue)
		}
		// apply the appropriate comparison logic based on the runtime values and their types
		return compareValues(typed.Op, leftValue, leftType, rightValue, rightType)

	default:
		return false, fmt.Errorf("unsupported boolean expression type %T", expr)
	}
}

/*
	evaluateValueExpr evaluates one validated value expression against one runtime record.

- Input parameters:
  - expr is either a field reference or a literal expression.
  - record is the current runtime record.
  - recordDef is the builder-produced schema of that record.

- Output:
  - the function returns the runtime value together with its engine field type.
  - the function returns an error only when a referenced field cannot be resolved or when the runtime value is inconsistent with the record definition.
*/
func evaluateValueExpr(expr parser.ValueExpr, record map[string]interface{}, recordDef RecordDefinition) (interface{}, FieldType, error) {
	switch typed := expr.(type) {

	case *parser.FieldRef:
		// resolve field reference to runtime value and field type using record definition metadata
		fieldDef, fieldPosition, exists := recordDef.LookupField(typed.Field.Table, typed.Field.Name)
		if !exists {
			return nil, 0, fmt.Errorf("field %s.%s is not present in runtime record definition", typed.Field.Table, typed.Field.Name)
		}
		if fieldPosition < 0 || fieldPosition >= len(recordDef.Fields) {
			return nil, 0, fmt.Errorf("field %s.%s has invalid record-definition position %d", typed.Field.Table, typed.Field.Name, fieldPosition)
		}

		if fieldDef == nil {
			return nil, 0, fmt.Errorf("field %s.%s has nil field definition at position %d", typed.Field.Table, typed.Field.Name, fieldPosition)
		}

		value, exists := lookupRuntimeFieldValue(record, fieldDef)
		if !exists {
			return nil, fieldDef.Type, nil
		}

		return value, fieldDef.Type, nil

	case *parser.LiteralExpr:
		// convert literal expression to runtime value and field type based on literal type
		switch literal := typed.Literal.(type) {
		case *parser.StringLiteral:
			return literal.Value, STRING, nil
		case *parser.NumberLiteral:
			return literal.Value, DECIMAL, nil
		case *parser.BooleanLiteral:
			return literal.Value, BOOLEAN, nil
		case *parser.DateLiteral:
			return literal.Value, DATE, nil
		case *parser.TimeLiteral:
			return literal.Value, TIME, nil
		default:
			return nil, 0, fmt.Errorf("unsupported literal type %T", literal)
		}

	default:
		return nil, 0, fmt.Errorf("unsupported value expression type %T", expr)
	}
}

func lookupRuntimeFieldValue(record map[string]interface{}, fieldDef *FieldDef) (interface{}, bool) {
	if fieldDef == nil {
		return nil, false
	}

	keys := []string{fieldDef.Name, fieldDef.QualifiedName(), fieldDef.SourceFieldName}
	seen := make(map[string]struct{}, len(keys))
	for _, key := range keys {
		if key == "" {
			continue
		}
		if _, exists := seen[key]; exists {
			continue
		}
		seen[key] = struct{}{}

		value, exists := record[key]
		if exists {
			return value, true
		}
	}

	return nil, false
}

/*
	compareValues compares two already-evaluated runtime values using their declared field types.

- Input parameters:
  - op is the comparison operator
  - leftValue the runtime value if the left operand
  - leftType the engine field type of the left operand
  - rightValue the runtime value of the right operand
  - rightType the engine field type of the right operand

- Output:
  - the boolean result of applying the comparison operator to the two values
  - an error if the values cannot be compared because of an engine/runtime inconsistency, such as unsupported field types or invalid runtime values for the declared field types

note: the function assumes parser validation has already enforced class compatibility and operator legality, but still
treats unsupported combinations as engine/runtime inconsistencies.
*/
func compareValues(op parser.CompareOp, leftValue interface{}, leftType FieldType, rightValue interface{}, rightType FieldType) (bool, error) {
	comparisonType, err := resolveComparisonType(leftType, rightType)
	if err != nil {
		return false, err
	}

	switch comparisonType {
	case BOOLEAN:
		leftBool, err := coerceBooleanValue(leftValue)
		if err != nil {
			return false, err
		}
		rightBool, err := coerceBooleanValue(rightValue)
		if err != nil {
			return false, err
		}
		return applyEqualityOperator(op, leftBool == rightBool)

	case STRING, CHAR:
		leftString, err := coerceStringValue(leftValue)
		if err != nil {
			return false, err
		}
		rightString, err := coerceStringValue(rightValue)
		if err != nil {
			return false, err
		}
		return applyEqualityOperator(op, leftString == rightString)

	case DATE:
		leftDays, err := coerceDateValue(leftValue)
		if err != nil {
			return false, err
		}
		rightDays, err := coerceDateValue(rightValue)
		if err != nil {
			return false, err
		}
		return applyOrderedOperator(op, compareUint64(leftDays, rightDays)), nil

	case TIME:
		leftMillis, err := coerceTimeValue(leftValue)
		if err != nil {
			return false, err
		}
		rightMillis, err := coerceTimeValue(rightValue)
		if err != nil {
			return false, err
		}
		return applyOrderedOperator(op, compareUint64(leftMillis, rightMillis)), nil

	case DECIMAL:
		leftDecimal, err := coerceDecimalValue(leftValue, leftType)
		if err != nil {
			return false, err
		}
		rightDecimal, err := coerceDecimalValue(rightValue, rightType)
		if err != nil {
			return false, err
		}
		return applyOrderedOperator(op, compareDecimalValues(leftDecimal, rightDecimal)), nil

	case FLOAT:
		leftFloat, err := coerceFloatValue(leftValue, leftType)
		if err != nil {
			return false, err
		}
		rightFloat, err := coerceFloatValue(rightValue, rightType)
		if err != nil {
			return false, err
		}
		return applyOrderedOperator(op, compareFloat64(leftFloat, rightFloat)), nil

	case SMALLINT, INT, BIGINT:
		leftInt, err := coerceInt64Value(leftValue, leftType)
		if err != nil {
			return false, err
		}
		rightInt, err := coerceInt64Value(rightValue, rightType)
		if err != nil {
			return false, err
		}
		return applyOrderedOperator(op, compareInt64(leftInt, rightInt)), nil

	default:
		return false, fmt.Errorf("unsupported comparison field type %d", comparisonType)
	}
}

func resolveComparisonType(leftType FieldType, rightType FieldType) (FieldType, error) {
	if leftType == rightType {
		return leftType, nil
	}

	if isStringType(leftType) && isStringType(rightType) {
		return STRING, nil
	}
	if isIntegralType(leftType) && isIntegralType(rightType) {
		if leftType == BIGINT || rightType == BIGINT {
			return BIGINT, nil
		}
		return INT, nil
	}
	if isNumericType(leftType) && isNumericType(rightType) {
		if leftType == FLOAT || rightType == FLOAT {
			return FLOAT, nil
		}
		if leftType == DECIMAL || rightType == DECIMAL {
			return DECIMAL, nil
		}
		if leftType == BIGINT || rightType == BIGINT {
			return BIGINT, nil
		}
		return INT, nil
	}

	return 0, fmt.Errorf("cannot resolve comparison type for %d and %d", leftType, rightType)
}

func isStringType(fieldType FieldType) bool {
	return fieldType == STRING || fieldType == CHAR
}

func isIntegralType(fieldType FieldType) bool {
	return fieldType == SMALLINT || fieldType == INT || fieldType == BIGINT
}

func isNumericType(fieldType FieldType) bool {
	return isIntegralType(fieldType) || fieldType == DECIMAL || fieldType == FLOAT
}

func applyEqualityOperator(op parser.CompareOp, equal bool) (bool, error) {
	switch op {
	case parser.CompareEq:
		return equal, nil
	case parser.CompareNe:
		return !equal, nil
	default:
		return false, fmt.Errorf("operator %s is not supported for equality-only comparison", op)
	}
}

func applyOrderedOperator(op parser.CompareOp, comparison int) bool {
	switch op {
	case parser.CompareEq:
		return comparison == 0
	case parser.CompareNe:
		return comparison != 0
	case parser.CompareLt:
		return comparison < 0
	case parser.CompareLe:
		return comparison <= 0
	case parser.CompareGt:
		return comparison > 0
	case parser.CompareGe:
		return comparison >= 0
	default:
		return false
	}
}

func coerceBooleanValue(value interface{}) (bool, error) {
	boolValue, ok := value.(bool)
	if !ok {
		return false, fmt.Errorf("expected BOOLEAN runtime value, got %T", value)
	}
	return boolValue, nil
}

func coerceStringValue(value interface{}) (string, error) {
	stringValue, ok := value.(string)
	if !ok {
		return "", fmt.Errorf("expected string runtime value, got %T", value)
	}
	return stringValue, nil
}

func coerceDateValue(value interface{}) (uint64, error) {
	stringValue, err := coerceStringValue(value)
	if err != nil {
		return 0, err
	}
	parsed, err := record.ParseDate(stringValue)
	if err != nil {
		return 0, fmt.Errorf("invalid DATE runtime value %q: %w", stringValue, err)
	}
	return parsed, nil
}

func coerceTimeValue(value interface{}) (uint64, error) {
	stringValue, err := coerceStringValue(value)
	if err != nil {
		return 0, err
	}
	parsed, err := record.ConvertCompactTime(stringValue)
	if err != nil {
		return 0, fmt.Errorf("invalid TIME runtime value %q: %w", stringValue, err)
	}
	return parsed, nil
}

func coerceDecimalValue(value interface{}, fieldType FieldType) (record.Decimal, error) {
	var decimalString string

	switch fieldType {
	case SMALLINT, INT:
		intValue, ok := value.(int)
		if !ok {
			return record.Decimal{}, fmt.Errorf("expected int runtime value for field type %d, got %T", fieldType, value)
		}
		decimalString = strconv.Itoa(intValue)
	case BIGINT:
		intValue, ok := value.(int64)
		if !ok {
			return record.Decimal{}, fmt.Errorf("expected int64 runtime value for BIGINT, got %T", value)
		}
		decimalString = strconv.FormatInt(intValue, 10)
	case DECIMAL:
		stringValue, err := coerceStringValue(value)
		if err != nil {
			return record.Decimal{}, err
		}
		decimalString = stringValue
	default:
		return record.Decimal{}, fmt.Errorf("cannot coerce field type %d to DECIMAL comparison", fieldType)
	}

	decimalValue, err := record.DecimalFromString(decimalString)
	if err != nil {
		return record.Decimal{}, fmt.Errorf("invalid DECIMAL runtime value %q: %w", decimalString, err)
	}
	return decimalValue, nil
}

func coerceFloatValue(value interface{}, fieldType FieldType) (float64, error) {
	switch fieldType {
	case SMALLINT, INT:
		intValue, ok := value.(int)
		if !ok {
			return 0, fmt.Errorf("expected int runtime value for field type %d, got %T", fieldType, value)
		}
		return float64(intValue), nil
	case BIGINT:
		intValue, ok := value.(int64)
		if !ok {
			return 0, fmt.Errorf("expected int64 runtime value for BIGINT, got %T", value)
		}
		return float64(intValue), nil
	case DECIMAL:
		decimalValue, err := coerceDecimalValue(value, fieldType)
		if err != nil {
			return 0, err
		}
		return decimalValue.Float64(), nil
	case FLOAT:
		floatValue, ok := value.(float64)
		if !ok {
			return 0, fmt.Errorf("expected float64 runtime value for FLOAT, got %T", value)
		}
		return floatValue, nil
	default:
		return 0, fmt.Errorf("cannot coerce field type %d to FLOAT comparison", fieldType)
	}
}

func coerceInt64Value(value interface{}, fieldType FieldType) (int64, error) {
	switch fieldType {
	case SMALLINT, INT:
		intValue, ok := value.(int)
		if !ok {
			return 0, fmt.Errorf("expected int runtime value for field type %d, got %T", fieldType, value)
		}
		return int64(intValue), nil
	case BIGINT:
		intValue, ok := value.(int64)
		if !ok {
			return 0, fmt.Errorf("expected int64 runtime value for BIGINT, got %T", value)
		}
		return intValue, nil
	default:
		return 0, fmt.Errorf("cannot coerce field type %d to integral comparison", fieldType)
	}
}

func compareUint64(left uint64, right uint64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareInt64(left int64, right int64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareFloat64(left float64, right float64) int {
	switch {
	case left < right:
		return -1
	case left > right:
		return 1
	default:
		return 0
	}
}

func compareDecimalValues(left record.Decimal, right record.Decimal) int {
	if left.Neg != right.Neg {
		if left.Neg {
			return -1
		}
		return 1
	}

	magnitude := compareDecimalMagnitude(left, right)
	if left.Neg {
		return -magnitude
	}
	return magnitude
}

func compareDecimalMagnitude(left record.Decimal, right record.Decimal) int {
	if left.IntPart != right.IntPart {
		if left.IntPart < right.IntPart {
			return -1
		}
		return 1
	}

	leftFrac, rightFrac := normalizeDecimalFractions(left, right)
	return strings.Compare(leftFrac, rightFrac)
}

func normalizeDecimalFractions(left record.Decimal, right record.Decimal) (string, string) {
	maxScale := int(left.Scale)
	if int(right.Scale) > maxScale {
		maxScale = int(right.Scale)
	}

	return decimalFractionString(left, maxScale), decimalFractionString(right, maxScale)
}

func decimalFractionString(value record.Decimal, scale int) string {
	if scale == 0 {
		return ""
	}

	fraction := fmt.Sprintf("%0*d", int(value.Scale), value.FracPart)
	if len(fraction) < scale {
		fraction += strings.Repeat("0", scale-len(fraction))
	}
	return fraction
}

func evaluateLikeComparison(leftValue interface{}, rightValue interface{}) (bool, error) {
	leftString, err := coerceStringValue(leftValue)
	if err != nil {
		return false, err
	}
	rightString, err := coerceStringValue(rightValue)
	if err != nil {
		return false, err
	}
	if !strings.HasSuffix(rightString, "*") {
		return false, fmt.Errorf("like pattern %q must end with '*'", rightString)
	}
	prefix := strings.TrimSuffix(rightString, "*")
	return strings.HasPrefix(leftString, prefix), nil
}

/*
executeSearchPlan recursively walks a DBWhereSearchPlan tree and returns the set of matching record IDs.

Leaf nodes perform a single dictionary or primary-key lookup.
AND nodes intersect the candidate sets from their children.
OR nodes union the candidate sets from their children.

When a node carries a Residual predicate, each candidate record is loaded and evaluated against
the residual expression; only records that pass are kept.
*/
func executeSearchPlan(plan *pipeline.DBWhereSearchPlan, tableName string, dbDef *stepdb.DBDefinition, recordDef RecordDefinition) ([]uint32, error) {
	var candidateIDs []uint32
	var err error

	switch plan.Kind {
	case pipeline.DBWhereSearchPrimaryKeyExact:
		candidateIDs, err = executeSearchPrimaryKeyExact(plan, tableName, dbDef)
	case pipeline.DBWhereSearchStringExact:
		candidateIDs, err = executeSearchStringExact(plan, tableName, dbDef)
	case pipeline.DBWhereSearchStringPrefix:
		candidateIDs, err = executeSearchStringPrefix(plan, tableName, dbDef)
	case pipeline.DBWhereSearchAnd:
		candidateIDs, err = executeSearchAnd(plan, tableName, dbDef, recordDef)
	case pipeline.DBWhereSearchOr:
		candidateIDs, err = executeSearchOr(plan, tableName, dbDef, recordDef)
	default:
		return nil, fmt.Errorf("unsupported search plan kind %d", plan.Kind)
	}
	if err != nil {
		return nil, err
	}

	if plan.Residual != nil {
		candidateIDs, err = filterByResidual(candidateIDs, plan.Residual, tableName, dbDef, recordDef)
		if err != nil {
			return nil, err
		}
	}

	return candidateIDs, nil
}

func executeSearchPrimaryKeyExact(plan *pipeline.DBWhereSearchPlan, tableName string, dbDef *stepdb.DBDefinition) ([]uint32, error) {
	literal, ok := plan.Literal.(*parser.StringLiteral)
	if !ok {
		return nil, fmt.Errorf("PrimaryKeyExact search expects a string literal, got %T", plan.Literal)
	}
	recordID, err := record.GetRecordID(tableName, literal.Value, dbDef)
	if err != nil {
		// primary key not found — zero candidates, not an error
		return []uint32{}, nil
	}
	return []uint32{recordID}, nil
}

func executeSearchStringExact(plan *pipeline.DBWhereSearchPlan, tableName string, dbDef *stepdb.DBDefinition) ([]uint32, error) {
	literal, ok := plan.Literal.(*parser.StringLiteral)
	if !ok {
		return nil, fmt.Errorf("StringExact search expects a string literal, got %T", plan.Literal)
	}
	return record.GetRecordsByString(tableName, plan.Field.FieldName, literal.Value, dbDef)
}

func executeSearchStringPrefix(plan *pipeline.DBWhereSearchPlan, tableName string, dbDef *stepdb.DBDefinition) ([]uint32, error) {
	return record.GetRecordsBySubstring(tableName, plan.Field.FieldName, plan.Prefix, dbDef)
}

func executeSearchAnd(plan *pipeline.DBWhereSearchPlan, tableName string, dbDef *stepdb.DBDefinition, recordDef RecordDefinition) ([]uint32, error) {
	leftIDs, err := executeSearchPlan(plan.Left, tableName, dbDef, recordDef)
	if err != nil {
		return nil, err
	}
	if len(leftIDs) == 0 {
		return leftIDs, nil
	}
	rightIDs, err := executeSearchPlan(plan.Right, tableName, dbDef, recordDef)
	if err != nil {
		return nil, err
	}
	return intersectRecordIDs(leftIDs, rightIDs), nil
}

func executeSearchOr(plan *pipeline.DBWhereSearchPlan, tableName string, dbDef *stepdb.DBDefinition, recordDef RecordDefinition) ([]uint32, error) {
	leftIDs, err := executeSearchPlan(plan.Left, tableName, dbDef, recordDef)
	if err != nil {
		return nil, err
	}
	rightIDs, err := executeSearchPlan(plan.Right, tableName, dbDef, recordDef)
	if err != nil {
		return nil, err
	}
	return unionRecordIDs(leftIDs, rightIDs), nil
}

func filterByResidual(candidateIDs []uint32, residual parser.BoolExpr, tableName string, dbDef *stepdb.DBDefinition, recordDef RecordDefinition) ([]uint32, error) {
	var filtered []uint32
	for _, id := range candidateIDs {
		rec, err := record.GetRecordByID(tableName, id, dbDef)
		if err != nil {
			return nil, fmt.Errorf("failed to load record %d for residual evaluation: %w", id, err)
		}
		match, err := evaluateBoolExpr(residual, rec, recordDef)
		if err != nil {
			return nil, fmt.Errorf("residual evaluation failed for record %d: %w", id, err)
		}
		if match {
			filtered = append(filtered, id)
		}
	}
	return filtered, nil
}

func intersectRecordIDs(a, b []uint32) []uint32 {
	set := make(map[uint32]struct{}, len(a))
	for _, id := range a {
		set[id] = struct{}{}
	}
	var result []uint32
	for _, id := range b {
		if _, exists := set[id]; exists {
			result = append(result, id)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}

func unionRecordIDs(a, b []uint32) []uint32 {
	set := make(map[uint32]struct{}, len(a)+len(b))
	for _, id := range a {
		set[id] = struct{}{}
	}
	for _, id := range b {
		set[id] = struct{}{}
	}
	result := make([]uint32, 0, len(set))
	for id := range set {
		result = append(result, id)
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
