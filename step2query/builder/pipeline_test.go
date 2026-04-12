package builder

import (
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/parser"
	"github.com/kozwoj/step2query/pipeline"
)

func TestBuildPipelineProducesCompleteDescriptionForWhereReturn(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseValidatedQuery(t, dbDef, queryExampleWherePrimaryKey)

	builtPipeline, err := BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	if len(builtPipeline.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(builtPipeline.Stages))
	}

	initialState := buildSourceStateDescription(t, dbDef, "Students")
	assertStateDescription(t, builtPipeline.InitialState, initialState)

	whereStage, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	assertStateDescription(t, whereStage.Input(), initialState)
	assertStateDescription(t, whereStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Students",
		RecordDef: initialState.RecordDef,
	})
	assertBoolExprMatches(t, whereStage.Plan.Expr, query.Stages[0].(*parser.WhereStage).Expr)
	if whereStage.Plan.ExecutionMode != "db-scan" {
		t.Fatalf("expected where execution mode db-scan, got %q", whereStage.Plan.ExecutionMode)
	}

	returnStage, ok := builtPipeline.Stages[1].(*pipeline.DBReturnStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBReturnStage, got %T", builtPipeline.Stages[1])
	}
	assertStateDescription(t, returnStage.Input(), whereStage.Output())
	assertStateDescription(t, returnStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Students",
		RecordDef: buildExpectedReturnRecordDef(
			fieldDef("Students", "First_name", pipeline.STRING, false, ""),
			fieldDef("Students", "Last_name", pipeline.STRING, false, ""),
		),
	})
	assertReturnItems(t, returnStage.Plan.Items,
		returnItem("Students", "First_name", initialState.RecordDef.FieldIndex["First_name"], pipeline.STRING),
		returnItem("Students", "Last_name", initialState.RecordDef.FieldIndex["Last_name"], pipeline.STRING),
	)
}

func TestBuildPipelineProducesCompleteDescriptionForWhereNavigateSet(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseValidatedQuery(t, dbDef, queryExampleWhereNavigateSetCourse)

	builtPipeline, err := BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	if len(builtPipeline.Stages) != 2 {
		t.Fatalf("expected 2 stages, got %d", len(builtPipeline.Stages))
	}

	initialState := buildSourceStateDescription(t, dbDef, "Classes")
	assertStateDescription(t, builtPipeline.InitialState, initialState)

	whereStage, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	assertStateDescription(t, whereStage.Input(), initialState)
	assertStateDescription(t, whereStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Classes",
		RecordDef: initialState.RecordDef,
	})
	assertBoolExprMatches(t, whereStage.Plan.Expr, query.Stages[0].(*parser.WhereStage).Expr)

	navigateStage, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateSetStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateSetStage, got %T", builtPipeline.Stages[1])
	}
	assertStateDescription(t, navigateStage.Input(), whereStage.Output())
	assertStateDescription(t, navigateStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Students",
		RecordDef: buildExpectedReturnRecordDef(
			fieldDef("Students", "Student_id", pipeline.CHAR, false, ""),
			fieldDef("Students", "Last_name", pipeline.STRING, false, ""),
			fieldDef("Students", "First_name", pipeline.STRING, false, ""),
		),
	})
	if navigateStage.Plan.SourceTable != "Classes" {
		t.Fatalf("expected source table Classes, got %q", navigateStage.Plan.SourceTable)
	}
	if navigateStage.Plan.TargetTable != "Students" {
		t.Fatalf("expected target table Students, got %q", navigateStage.Plan.TargetTable)
	}
	if navigateStage.Plan.SetName != "Enrollment" {
		t.Fatalf("expected set name Enrollment, got %q", navigateStage.Plan.SetName)
	}
	studentsState := buildSourceStateDescription(t, dbDef, "Students")
	assertReturnItems(t, navigateStage.Plan.ReturnItems,
		returnItem("Students", "Student_id", studentsState.RecordDef.FieldIndex["Student_id"], pipeline.CHAR),
		returnItem("Students", "Last_name", studentsState.RecordDef.FieldIndex["Last_name"], pipeline.STRING),
		returnItem("Students", "First_name", studentsState.RecordDef.FieldIndex["First_name"], pipeline.STRING),
	)
}

func TestBuildPipelineProducesCompleteDescriptionForWhereTwoNavigateFK(t *testing.T) {
	dbDef := loadCollegeDefinition(t)
	query := parseValidatedQuery(t, dbDef, queryExampleWhereTwoNavigateFK)

	builtPipeline, err := BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	if len(builtPipeline.Stages) != 3 {
		t.Fatalf("expected 3 stages, got %d", len(builtPipeline.Stages))
	}

	classesState := buildSourceStateDescription(t, dbDef, "Classes")
	assertStateDescription(t, builtPipeline.InitialState, classesState)

	whereStage, ok := builtPipeline.Stages[0].(*pipeline.DBWhereStage)
	if !ok {
		t.Fatalf("expected first stage to be *pipeline.DBWhereStage, got %T", builtPipeline.Stages[0])
	}
	assertStateDescription(t, whereStage.Input(), classesState)
	assertStateDescription(t, whereStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateDBTableWorkingSet,
		TableName: "Classes",
		RecordDef: classesState.RecordDef,
	})
	assertBoolExprMatches(t, whereStage.Plan.Expr, query.Stages[0].(*parser.WhereStage).Expr)

	dbNavigateStage, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateFKStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateFKStage, got %T", builtPipeline.Stages[1])
	}
	assertStateDescription(t, dbNavigateStage.Input(), whereStage.Output())
	if dbNavigateStage.Plan.SourceTable != "Classes" {
		t.Fatalf("expected source table Classes, got %q", dbNavigateStage.Plan.SourceTable)
	}
	if dbNavigateStage.Plan.TargetTable != "Teachers" {
		t.Fatalf("expected target table Teachers, got %q", dbNavigateStage.Plan.TargetTable)
	}
	assertQualifiedFieldRef(t, dbNavigateStage.Plan.ForeignKey, pipeline.QualifiedFieldRef{
		TableName: "Classes",
		FieldName: "Teacher",
		Position:  classesState.RecordDef.FieldIndex["Teacher"],
		Type:      classesState.RecordDef.Fields[classesState.RecordDef.FieldIndex["Teacher"]].Type,
	})
	teachersState := buildSourceStateDescription(t, dbDef, "Teachers")
	assertQualifiedFieldRef(t, dbNavigateStage.Plan.PrimaryKey, pipeline.QualifiedFieldRef{
		TableName: "Teachers",
		FieldName: "Employee_id",
		Position:  teachersState.RecordDef.FieldIndex["Employee_id"],
		Type:      teachersState.RecordDef.Fields[teachersState.RecordDef.FieldIndex["Employee_id"]].Type,
	})
	assertStateDescription(t, dbNavigateStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Teachers",
		RecordDef: buildExpectedReturnRecordDef(
			fieldDef("Classes", "Class_code", pipeline.CHAR, false, ""),
			fieldDef("Teachers", "Works_for", pipeline.CHAR, true, "Departments"),
			fieldDef("Teachers", "First_name", pipeline.STRING, false, ""),
			fieldDef("Teachers", "Last_name", pipeline.STRING, false, ""),
			fieldDef("Teachers", "Office", pipeline.CHAR, false, ""),
		),
	})
	assertReturnItems(t, dbNavigateStage.Plan.ReturnItems,
		returnItem("Classes", "Class_code", classesState.RecordDef.FieldIndex["Class_code"], pipeline.CHAR),
		returnItem("Teachers", "Works_for", teachersState.RecordDef.FieldIndex["Works_for"], pipeline.CHAR),
		returnItem("Teachers", "First_name", teachersState.RecordDef.FieldIndex["First_name"], pipeline.STRING),
		returnItem("Teachers", "Last_name", teachersState.RecordDef.FieldIndex["Last_name"], pipeline.STRING),
		returnItem("Teachers", "Office", teachersState.RecordDef.FieldIndex["Office"], pipeline.CHAR),
	)

	memoryNavigateStage, ok := builtPipeline.Stages[2].(*pipeline.MemoryNavigateFKStage)
	if !ok {
		t.Fatalf("expected third stage to be *pipeline.MemoryNavigateFKStage, got %T", builtPipeline.Stages[2])
	}
	assertStateDescription(t, memoryNavigateStage.Input(), dbNavigateStage.Output())
	if memoryNavigateStage.Plan.SourceTable != "Teachers" {
		t.Fatalf("expected source table Teachers, got %q", memoryNavigateStage.Plan.SourceTable)
	}
	if memoryNavigateStage.Plan.TargetTable != "Departments" {
		t.Fatalf("expected target table Departments, got %q", memoryNavigateStage.Plan.TargetTable)
	}
	assertQualifiedFieldRef(t, memoryNavigateStage.Plan.ForeignKey, pipeline.QualifiedFieldRef{
		TableName: "Teachers",
		FieldName: "Works_for",
		Position:  dbNavigateStage.Output().RecordDef.FieldIndex[pipeline.QualifiedFieldKey("Teachers", "Works_for")],
		Type:      dbNavigateStage.Output().RecordDef.Fields[dbNavigateStage.Output().RecordDef.FieldIndex[pipeline.QualifiedFieldKey("Teachers", "Works_for")]].Type,
	})
	departmentsState := buildSourceStateDescription(t, dbDef, "Departments")
	assertQualifiedFieldRef(t, memoryNavigateStage.Plan.PrimaryKey, pipeline.QualifiedFieldRef{
		TableName: "Departments",
		FieldName: "Department_code",
		Position:  departmentsState.RecordDef.FieldIndex["Department_code"],
		Type:      departmentsState.RecordDef.Fields[departmentsState.RecordDef.FieldIndex["Department_code"]].Type,
	})
	assertStateDescription(t, memoryNavigateStage.Output(), pipeline.StateDescription{
		Kind:      pipeline.StateReturnWorkingSet,
		TableName: "Departments",
		RecordDef: buildExpectedReturnRecordDef(
			fieldDef("Classes", "Class_code", pipeline.CHAR, false, ""),
			fieldDef("Teachers", "First_name", pipeline.STRING, false, ""),
			fieldDef("Teachers", "Last_name", pipeline.STRING, false, ""),
			fieldDef("Departments", "Building_name", pipeline.STRING, false, ""),
			fieldDef("Teachers", "Office", pipeline.CHAR, false, ""),
		),
	})
	assertReturnItems(t, memoryNavigateStage.Plan.ReturnItems,
		returnItem("Classes", "Class_code", dbNavigateStage.Output().RecordDef.FieldIndex[pipeline.QualifiedFieldKey("Classes", "Class_code")], pipeline.CHAR),
		returnItem("Teachers", "First_name", dbNavigateStage.Output().RecordDef.FieldIndex[pipeline.QualifiedFieldKey("Teachers", "First_name")], pipeline.STRING),
		returnItem("Teachers", "Last_name", dbNavigateStage.Output().RecordDef.FieldIndex[pipeline.QualifiedFieldKey("Teachers", "Last_name")], pipeline.STRING),
		returnItem("Departments", "Building_name", departmentsState.RecordDef.FieldIndex["Building_name"], pipeline.STRING),
		returnItem("Teachers", "Office", dbNavigateStage.Output().RecordDef.FieldIndex[pipeline.QualifiedFieldKey("Teachers", "Office")], pipeline.CHAR),
	)
}

func parseValidatedQuery(t *testing.T, dbDef *stepdb.DBDefinition, input string) *parser.Query {
	t.Helper()
	query, err := parser.Parse(input)
	if err != nil {
		t.Fatalf("Parse returned error: %v", err)
	}
	if err := parser.ValidateAST(query, dbDef); err != nil {
		t.Fatalf("ValidateAST returned error: %v", err)
	}
	return query
}

func assertBoolExprMatches(t *testing.T, got parser.BoolExpr, want parser.BoolExpr) {
	t.Helper()
	if got != want {
		t.Fatalf("expected bool expr %p, got %p", want, got)
	}
}

func assertStateDescription(t *testing.T, got pipeline.StateDescription, want pipeline.StateDescription) {
	t.Helper()
	if got.Kind != want.Kind {
		t.Fatalf("expected state kind %d, got %d", want.Kind, got.Kind)
	}
	if got.TableName != want.TableName {
		t.Fatalf("expected state table %q, got %q", want.TableName, got.TableName)
	}
	assertRecordDefinition(t, got.RecordDef, want.RecordDef)
}

func assertRecordDefinition(t *testing.T, got pipeline.RecordDefinition, want pipeline.RecordDefinition) {
	t.Helper()
	if got.NoFields != want.NoFields {
		t.Fatalf("expected %d fields, got %d", want.NoFields, got.NoFields)
	}
	if got.PrimaryKey != want.PrimaryKey {
		t.Fatalf("expected primary key index %d, got %d", want.PrimaryKey, got.PrimaryKey)
	}
	if len(got.Fields) != len(want.Fields) {
		t.Fatalf("expected %d field definitions, got %d", len(want.Fields), len(got.Fields))
	}
	for i := range want.Fields {
		assertFieldDef(t, got.Fields[i], want.Fields[i], i)
	}
	if len(got.FieldIndex) != len(want.FieldIndex) {
		t.Fatalf("expected %d field index entries, got %d", len(want.FieldIndex), len(got.FieldIndex))
	}
	for name, wantIndex := range want.FieldIndex {
		gotIndex, ok := got.FieldIndex[name]
		if !ok {
			t.Fatalf("expected field index entry for %q", name)
		}
		if gotIndex != wantIndex {
			t.Fatalf("expected field index for %q to be %d, got %d", name, wantIndex, gotIndex)
		}
	}
	for name := range got.FieldIndex {
		if _, ok := want.FieldIndex[name]; !ok {
			t.Fatalf("unexpected field index entry %q", name)
		}
	}
	if len(got.QualifiedFieldIndex) != len(want.QualifiedFieldIndex) {
		t.Fatalf("expected %d qualified field index entries, got %d", len(want.QualifiedFieldIndex), len(got.QualifiedFieldIndex))
	}
	for name, wantIndex := range want.QualifiedFieldIndex {
		gotIndex, ok := got.QualifiedFieldIndex[name]
		if !ok {
			t.Fatalf("expected qualified field index entry for %q", name)
		}
		if gotIndex != wantIndex {
			t.Fatalf("expected qualified field index for %q to be %d, got %d", name, wantIndex, gotIndex)
		}
	}
	for name := range got.QualifiedFieldIndex {
		if _, ok := want.QualifiedFieldIndex[name]; !ok {
			t.Fatalf("unexpected qualified field index entry %q", name)
		}
	}
}

func assertFieldDef(t *testing.T, got *pipeline.FieldDef, want *pipeline.FieldDef, position int) {
	t.Helper()
	if got == nil || want == nil {
		t.Fatalf("field %d definition must not be nil", position)
	}
	if got.Name != want.Name {
		t.Fatalf("expected field %d name %q, got %q", position, want.Name, got.Name)
	}
	if got.SourceTableName != want.SourceTableName {
		t.Fatalf("expected field %d source table %q, got %q", position, want.SourceTableName, got.SourceTableName)
	}
	if got.SourceFieldName != want.SourceFieldName {
		t.Fatalf("expected field %d source field %q, got %q", position, want.SourceFieldName, got.SourceFieldName)
	}
	if got.Type != want.Type {
		t.Fatalf("expected field %d type %d, got %d", position, want.Type, got.Type)
	}
	if got.IsForeignKey != want.IsForeignKey {
		t.Fatalf("expected field %d foreign key flag %t, got %t", position, want.IsForeignKey, got.IsForeignKey)
	}
	if got.ForeignKeyTable != want.ForeignKeyTable {
		t.Fatalf("expected field %d foreign key table %q, got %q", position, want.ForeignKeyTable, got.ForeignKeyTable)
	}
}

func assertReturnItems(t *testing.T, got []pipeline.ReturnFieldRef, want ...pipeline.ReturnFieldRef) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("expected %d return items, got %d", len(want), len(got))
	}
	for i := range want {
		assertQualifiedFieldRef(t, got[i].Source, want[i].Source)
		if got[i].Alias != want[i].Alias {
			t.Fatalf("expected return item %d alias %q, got %q", i, want[i].Alias, got[i].Alias)
		}
	}
}

func assertQualifiedFieldRef(t *testing.T, got pipeline.QualifiedFieldRef, want pipeline.QualifiedFieldRef) {
	t.Helper()
	if got.TableName != want.TableName {
		t.Fatalf("expected qualified field table %q, got %q", want.TableName, got.TableName)
	}
	if got.FieldName != want.FieldName {
		t.Fatalf("expected qualified field name %q, got %q", want.FieldName, got.FieldName)
	}
	if got.Position != want.Position {
		t.Fatalf("expected qualified field position %d, got %d", want.Position, got.Position)
	}
	if got.Type != want.Type {
		t.Fatalf("expected qualified field type %d, got %d", want.Type, got.Type)
	}
}

func buildExpectedReturnRecordDef(fields ...*pipeline.FieldDef) pipeline.RecordDefinition {
	fieldIndex := make(map[string]int, len(fields))
	qualifiedFieldIndex := make(map[string]int, len(fields))
	for i, field := range fields {
		fieldIndex[field.Name] = i
		qualifiedFieldIndex[pipeline.QualifiedFieldKey(field.SourceTableName, field.SourceFieldName)] = i
	}
	return pipeline.RecordDefinition{
		NoFields:            len(fields),
		PrimaryKey:          -1,
		Fields:              fields,
		FieldIndex:          fieldIndex,
		QualifiedFieldIndex: qualifiedFieldIndex,
	}
}

func fieldDef(sourceTableName string, name string, fieldType pipeline.FieldType, isForeignKey bool, foreignKeyTable string) *pipeline.FieldDef {
	return &pipeline.FieldDef{
		Name:            pipeline.QualifiedFieldKey(sourceTableName, name),
		SourceTableName: sourceTableName,
		SourceFieldName: name,
		Type:            fieldType,
		IsForeignKey:    isForeignKey,
		ForeignKeyTable: foreignKeyTable,
	}
}

func returnItem(tableName string, fieldName string, position int, fieldType pipeline.FieldType) pipeline.ReturnFieldRef {
	return pipeline.ReturnFieldRef{
		Source: pipeline.QualifiedFieldRef{
			TableName: tableName,
			FieldName: fieldName,
			Position:  position,
			Type:      fieldType,
		},
	}
}
