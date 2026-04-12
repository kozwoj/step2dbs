package db

import (
	"errors"
	"sync"
)

/*-------------------------------------------------------------------------------------------------
This file defines the global database definition and provides functions to initialize and access it.
The global definition is stored in a resettable singleton pattern using a mutex to ensure thread-safe access.

The definition must be initialized when the DB is opened using:

err := db.InitDefinition(*DBDefinition)

The definition can be accessed from anywhere using:

def := db.Definition()

and can be checked if it has been initialized using:

if !db.DefinitionInitialized() {
	// the DBDefinition has not been initialized yet
	error handling...
}

To close and reset the global definition (only called by CloseDB):

db.ResetDefinition()

Note: InitDefinition can only be called when the definition is nil (not yet initialized or after reset).
ResetDefinition can only be called when the definition is not nil.
------------------------------------------------------------------------------------------------- */

var (
	defMutex  sync.RWMutex
	globalDef *DBDefinition
)

var (
	ErrDefinitionAlreadyInitialized = errors.New("database definition already initialized")
	ErrDefinitionNotInitialized     = errors.New("database definition not initialized")
)

// InitDefinition initializes the global database definition.
// Returns an error if the definition is already initialized.
// This ensures singleton behavior - can only be called once until ResetDefinition is called.
func InitDefinition(def *DBDefinition) error {
	defMutex.Lock()
	defer defMutex.Unlock()

	if globalDef != nil {
		return ErrDefinitionAlreadyInitialized
	}

	globalDef = def
	return nil
}

// Definition returns the global database definition.
// Returns nil if not initialized.
func Definition() *DBDefinition {
	defMutex.RLock()
	defer defMutex.RUnlock()
	return globalDef
}

// DefinitionInitialized returns true if the global definition has been initialized.
func DefinitionInitialized() bool {
	defMutex.RLock()
	defer defMutex.RUnlock()
	return globalDef != nil
}

// ResetDefinition resets the global database definition to nil.
// Returns an error if the definition is not initialized.
// This should only be called by CloseDB.
func ResetDefinition() error {
	defMutex.Lock()
	defer defMutex.Unlock()

	if globalDef == nil {
		return ErrDefinitionNotInitialized
	}

	globalDef = nil
	return nil
}
