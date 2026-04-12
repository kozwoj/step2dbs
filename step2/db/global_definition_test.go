package db

import (
	"testing"
	"time"
)

func TestInitDefinition_Singleton(t *testing.T) {
	// Ensure we start with a clean slate
	if DefinitionInitialized() {
		_ = ResetDefinition()
	}

	// Create a test DBDefinition
	testDef := &DBDefinition{
		Name:      "TestDB",
		DirPath:   "/test/path",
		CreatedOn: time.Now(),
		Tables:    make([]*TableDescription, 0),
	}

	// First initialization should succeed
	err := InitDefinition(testDef)
	if err != nil {
		t.Fatalf("First InitDefinition failed: %v", err)
	}

	// Verify it was initialized
	if !DefinitionInitialized() {
		t.Error("DefinitionInitialized should return true after initialization")
	}

	// Verify we can access it
	def := Definition()
	if def == nil {
		t.Fatal("Definition() returned nil after initialization")
	}
	if def.Name != "TestDB" {
		t.Errorf("Expected name 'TestDB', got '%s'", def.Name)
	}

	// Second initialization should fail (singleton enforcement)
	anotherDef := &DBDefinition{
		Name:      "AnotherDB",
		DirPath:   "/another/path",
		CreatedOn: time.Now(),
		Tables:    make([]*TableDescription, 0),
	}
	err = InitDefinition(anotherDef)
	if err == nil {
		t.Fatal("Expected error when calling InitDefinition twice, got nil")
	}
	if err != ErrDefinitionAlreadyInitialized {
		t.Errorf("Expected ErrDefinitionAlreadyInitialized, got %v", err)
	}

	// Verify the original definition is still there
	def = Definition()
	if def.Name != "TestDB" {
		t.Errorf("Definition was changed! Expected 'TestDB', got '%s'", def.Name)
	}

	// Cleanup
	err = ResetDefinition()
	if err != nil {
		t.Errorf("ResetDefinition failed: %v", err)
	}

	// Verify it was reset
	if DefinitionInitialized() {
		t.Error("DefinitionInitialized should return false after reset")
	}
}

func TestResetDefinition_RequiresInitialization(t *testing.T) {
	// Ensure we start with a clean slate
	if DefinitionInitialized() {
		_ = ResetDefinition()
	}

	// Trying to reset when not initialized should fail
	err := ResetDefinition()
	if err == nil {
		t.Fatal("Expected error when calling ResetDefinition on uninitialized definition, got nil")
	}
	if err != ErrDefinitionNotInitialized {
		t.Errorf("Expected ErrDefinitionNotInitialized, got %v", err)
	}
}

func TestDefinition_Lifecycle(t *testing.T) {
	// Ensure we start with a clean slate
	if DefinitionInitialized() {
		_ = ResetDefinition()
	}

	// Initially should not be initialized
	if DefinitionInitialized() {
		t.Error("Definition should not be initialized at start")
	}
	if Definition() != nil {
		t.Error("Definition() should return nil when not initialized")
	}

	// Initialize
	testDef := &DBDefinition{
		Name:      "LifecycleDB",
		DirPath:   "/lifecycle/path",
		CreatedOn: time.Now(),
		Tables:    make([]*TableDescription, 0),
	}
	err := InitDefinition(testDef)
	if err != nil {
		t.Fatalf("InitDefinition failed: %v", err)
	}

	// Should be initialized now
	if !DefinitionInitialized() {
		t.Error("Definition should be initialized after InitDefinition")
	}
	if Definition() == nil {
		t.Error("Definition() should not return nil after initialization")
	}

	// Reset
	err = ResetDefinition()
	if err != nil {
		t.Fatalf("ResetDefinition failed: %v", err)
	}

	// Should not be initialized anymore
	if DefinitionInitialized() {
		t.Error("Definition should not be initialized after reset")
	}
	if Definition() != nil {
		t.Error("Definition() should return nil after reset")
	}

	// Should be able to initialize again with a different definition
	anotherDef := &DBDefinition{
		Name:      "AnotherDB",
		DirPath:   "/another/path",
		CreatedOn: time.Now(),
		Tables:    make([]*TableDescription, 0),
	}
	err = InitDefinition(anotherDef)
	if err != nil {
		t.Fatalf("Second InitDefinition after reset failed: %v", err)
	}

	// Verify the new definition
	def := Definition()
	if def.Name != "AnotherDB" {
		t.Errorf("Expected name 'AnotherDB', got '%s'", def.Name)
	}

	// Cleanup
	_ = ResetDefinition()
}
