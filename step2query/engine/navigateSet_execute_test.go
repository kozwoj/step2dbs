package engine

import (
	"testing"

	stepdb "github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/builder"
	"github.com/kozwoj/step2query/pipeline"
	"github.com/kozwoj/step2query/test_db"
)

const queryExampleWhereNavigateSetByPK = `Classes | where Classes.Class_code == "MATH101-01" | navigate set Classes.Enrollment return Students.Student_id, Students.Advisor`

const queryExampleWhereNavigateSetByCourse = `Classes | where Classes.Course == "MATH202 " | navigate set Classes.Enrollment return Students.Student_id, Students.Last_name, Students.First_name`

func TestExecuteNavigateSetByPrimaryKey(t *testing.T) {
	tempDir := t.TempDir()
	_, _, err := test_db.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(stepdb.CloseDB)

	dbDef := stepdb.Definition()
	if dbDef == nil {
		t.Fatal("expected opened DBDefinition after creating NIP test database")
	}

	query := parseValidatedQuery(t, dbDef, queryExampleWhereNavigateSetByPK)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	if len(builtPipeline.Stages) != 2 {
		t.Fatalf("expected pipeline to contain 2 stages, got %d", len(builtPipeline.Stages))
	}
	navigateStage, ok := builtPipeline.Stages[1].(*pipeline.DBNavigateSetStage)
	if !ok {
		t.Fatalf("expected second stage to be *pipeline.DBNavigateSetStage, got %T", builtPipeline.Stages[1])
	}
	if navigateStage.Plan.SetName != "Enrollment" {
		t.Fatalf("expected set name Enrollment, got %s", navigateStage.Plan.SetName)
	}

	output, err := ExecutePipeline(builtPipeline, dbDef)
	if err != nil {
		t.Fatalf("ExecutePipeline returned error: %v", err)
	}

	returnSet, ok := output.(*ReturnWorkingSet)
	if !ok {
		t.Fatalf("expected pipeline output to be *ReturnWorkingSet, got %T", output)
	}

	// MATH101-01 has 6 enrolled students.
	if returnSet.Size() != 6 {
		t.Fatalf("expected 6 output rows, got %d", returnSet.Size())
	}

	expectedStudentIDs := map[string]bool{
		"NIP2409002": false,
		"NIP2409003": false,
		"NIP2409007": false,
		"NIP2409013": false,
		"NIP2409014": false,
		"NIP2309017": false,
	}

	assertNavigateSetOutputStudentIDs(t, returnSet, expectedStudentIDs, "Students.Student_id")
}

func TestExecuteNavigateSetByCourse(t *testing.T) {
	tempDir := t.TempDir()
	_, _, err := test_db.CreateAndPopulateNIPDatabase(tempDir)
	if err != nil {
		t.Fatalf("CreateAndPopulateNIPDatabase failed: %v", err)
	}
	t.Cleanup(stepdb.CloseDB)

	dbDef := stepdb.Definition()
	if dbDef == nil {
		t.Fatal("expected opened DBDefinition after creating NIP test database")
	}

	query := parseValidatedQuery(t, dbDef, queryExampleWhereNavigateSetByCourse)
	builtPipeline, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		t.Fatalf("BuildPipeline returned error: %v", err)
	}

	output, err := ExecutePipeline(builtPipeline, dbDef)
	if err != nil {
		t.Fatalf("ExecutePipeline returned error: %v", err)
	}

	returnSet, ok := output.(*ReturnWorkingSet)
	if !ok {
		t.Fatalf("expected pipeline output to be *ReturnWorkingSet, got %T", output)
	}

	// MATH202-01 has 6 enrolled students, MATH202-02 has 4. Total = 10.
	if returnSet.Size() != 10 {
		t.Fatalf("expected 10 output rows, got %d", returnSet.Size())
	}

	expectedStudentIDs := map[string]bool{
		"NIP2209001": false,
		"NIP2209008": false,
		"NIP2209014": false,
		"NIP2209017": false,
		"NIP2309008": false,
		"NIP2309009": false,
		"NIP2209009": false,
		"NIP2209012": false,
		"NIP2309011": false,
		"NIP2309013": false,
	}

	assertNavigateSetOutputStudentIDs(t, returnSet, expectedStudentIDs, "Students.Student_id")

	// Spot-check a few name values from known students.
	assertNavigateSetContainsStudent(t, returnSet, "NIP2209001", "Johnson", "Emma")
	assertNavigateSetContainsStudent(t, returnSet, "NIP2209009", "Gonzalez", "Owen")
	assertNavigateSetContainsStudent(t, returnSet, "NIP2309013", "Morris", "Lily")
}

func assertNavigateSetOutputStudentIDs(t *testing.T, returnSet *ReturnWorkingSet, expectedIDs map[string]bool, fieldName string) {
	t.Helper()

	currentRecord, err := returnSet.GetFirstRecord()
	if err != nil {
		t.Fatalf("GetFirstRecord returned error: %v", err)
	}
	for currentRecord != nil {
		studentID, ok := currentRecord[fieldName].(string)
		if !ok {
			t.Fatalf("expected %s to be string, got %T", fieldName, currentRecord[fieldName])
		}
		if _, expected := expectedIDs[studentID]; !expected {
			t.Fatalf("unexpected student ID %s in output", studentID)
		}
		expectedIDs[studentID] = true

		currentRecord, err = returnSet.GetNextRecord()
		if err != nil {
			if err.Error() == ErrNoMoreRecords.Error() {
				break
			}
			t.Fatalf("GetNextRecord returned error: %v", err)
		}
	}

	for id, found := range expectedIDs {
		if !found {
			t.Fatalf("expected student ID %s not found in output", id)
		}
	}
}

func assertNavigateSetContainsStudent(t *testing.T, returnSet *ReturnWorkingSet, studentID string, lastName string, firstName string) {
	t.Helper()

	currentRecord, err := returnSet.GetFirstRecord()
	if err != nil {
		t.Fatalf("GetFirstRecord returned error: %v", err)
	}
	for currentRecord != nil {
		if id, _ := currentRecord["Students.Student_id"].(string); id == studentID {
			assertOutputRecordValue(t, currentRecord, "Students.Last_name", lastName)
			assertOutputRecordValue(t, currentRecord, "Students.First_name", firstName)
			return
		}
		currentRecord, err = returnSet.GetNextRecord()
		if err != nil {
			if err.Error() == ErrNoMoreRecords.Error() {
				break
			}
			t.Fatalf("GetNextRecord returned error: %v", err)
		}
	}

	t.Fatalf("student %s not found in output", studentID)
}
