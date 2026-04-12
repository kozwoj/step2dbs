package dml

import (
	"encoding/json"
	"fmt"
	"github.com/kozwoj/step2/db"
	"sync"
)

// dmlMutex serializes all mutating DML commands.
// Both individual mutating commands and batch execution acquire this lock.
// Read-only operations do not require the lock.
var dmlMutex sync.Mutex

// LockDML acquires the DML mutex for mutating operations.
func LockDML() {
	dmlMutex.Lock()
}

// UnlockDML releases the DML mutex.
func UnlockDML() {
	dmlMutex.Unlock()
}

// validBatchCommands lists the mutating commands allowed in a batch.
var validBatchCommands = map[string]bool{
	"add":             true,
	"update":          true,
	"delete":          true,
	"addSetMember":    true,
	"removeSetMember": true,
}

// BatchCommand represents a parsed command within a batch request.
type BatchCommand struct {
	Name   string                 // command name (the key: "add", "update", etc.)
	Params map[string]interface{} // command parameters (the value object)
}

// parseBatchCommands parses the key-based input format where each command
// is an object with a single key: {"add": {"tableName": ..., "record": ...}}
func parseBatchCommands(input string) ([]BatchCommand, error) {
	var raw struct {
		Commands []map[string]json.RawMessage `json:"commands"`
	}
	if err := json.Unmarshal([]byte(input), &raw); err != nil {
		return nil, fmt.Errorf("failed to parse batch JSON: %v", err)
	}
	if len(raw.Commands) == 0 {
		return nil, fmt.Errorf("batch must contain at least one command")
	}

	commands := make([]BatchCommand, len(raw.Commands))
	for i, entry := range raw.Commands {
		if len(entry) != 1 {
			return nil, fmt.Errorf("command at index %d: must have exactly one key (the command name), got %d", i, len(entry))
		}
		for name, paramBytes := range entry {
			if !validBatchCommands[name] {
				return nil, fmt.Errorf("command at index %d: unsupported command %q", i, name)
			}
			var params map[string]interface{}
			if err := json.Unmarshal(paramBytes, &params); err != nil {
				return nil, fmt.Errorf("command at index %d (%s): invalid parameters: %v", i, name, err)
			}
			commands[i] = BatchCommand{Name: name, Params: params}
		}
	}
	return commands, nil
}

// validateBatchCommands checks that all parsed commands have their required fields.
func validateBatchCommands(commands []BatchCommand) error {
	for i, cmd := range commands {
		switch cmd.Name {
		case "add":
			if _, ok := cmd.Params["tableName"].(string); !ok {
				return fmt.Errorf("command at index %d (add): missing \"tableName\"", i)
			}
			if _, ok := cmd.Params["record"].(map[string]interface{}); !ok {
				return fmt.Errorf("command at index %d (add): missing \"record\"", i)
			}
		case "update":
			if _, ok := cmd.Params["tableName"].(string); !ok {
				return fmt.Errorf("command at index %d (update): missing \"tableName\"", i)
			}
			if _, ok := cmd.Params["recordID"].(float64); !ok {
				return fmt.Errorf("command at index %d (update): missing or invalid \"recordID\"", i)
			}
			if _, ok := cmd.Params["record"].(map[string]interface{}); !ok {
				return fmt.Errorf("command at index %d (update): missing \"record\"", i)
			}
		case "delete":
			if _, ok := cmd.Params["tableName"].(string); !ok {
				return fmt.Errorf("command at index %d (delete): missing \"tableName\"", i)
			}
			if _, ok := cmd.Params["recordID"].(float64); !ok {
				return fmt.Errorf("command at index %d (delete): missing or invalid \"recordID\"", i)
			}
		case "addSetMember", "removeSetMember":
			if _, ok := cmd.Params["ownerTableName"].(string); !ok {
				return fmt.Errorf("command at index %d (%s): missing \"ownerTableName\"", i, cmd.Name)
			}
			if _, ok := cmd.Params["ownerRecordID"].(float64); !ok {
				return fmt.Errorf("command at index %d (%s): missing or invalid \"ownerRecordID\"", i, cmd.Name)
			}
			if _, ok := cmd.Params["setName"].(string); !ok {
				return fmt.Errorf("command at index %d (%s): missing \"setName\"", i, cmd.Name)
			}
			if _, ok := cmd.Params["memberRecordID"].(float64); !ok {
				return fmt.Errorf("command at index %d (%s): missing or invalid \"memberRecordID\"", i, cmd.Name)
			}
		}
	}
	return nil
}

// commandDMLFunc maps command names to their DML functions.
var commandDMLFunc = map[string]func(string) (string, error){
	"add":             AddNewRecord_DML,
	"update":          UpdateRecord_DML,
	"delete":          DeleteRecord_DML,
	"addSetMember":    AddSetMember_DML,
	"removeSetMember": RemoveSetMember_DML,
}

// undoAction records how to reverse a successfully executed command.
type undoAction struct {
	commandIndex int
	command      string
	undoFunc     func() (string, error) // closure calling the appropriate DML function
}

// captureRecordState calls GetRecordByID_DML to capture the current field values.
// Returns the record map or an error string.
func captureRecordState(tableName string, recordID float64) (map[string]interface{}, error) {
	inputJSON := fmt.Sprintf(`{"tableName":%q,"recordID":%v}`, tableName, recordID)
	resultJSON, err := GetRecordByID_DML(inputJSON)
	if err != nil {
		return nil, fmt.Errorf("failed to capture record state: %v", err)
	}
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		return nil, fmt.Errorf("failed to parse capture result: %v", err)
	}
	if result["status"] != "success" {
		return nil, fmt.Errorf("failed to capture record state: %v", result["errors"])
	}
	rec, _ := result["record"].(map[string]interface{})
	return rec, nil
}

// captureOwnedSets captures all set members for each set that the table owns.
// Returns a map of setName -> []memberRecordID.
func captureOwnedSets(tableName string, recordID float64) (map[string][]float64, error) {
	dbDef := db.Definition()
	if dbDef == nil {
		return nil, fmt.Errorf("database not opened")
	}
	tableIndex, ok := dbDef.TableIndex[tableName]
	if !ok {
		return nil, fmt.Errorf("table %q not found", tableName)
	}
	tableDesc := dbDef.Tables[tableIndex]

	setMembers := make(map[string][]float64)
	for _, setDesc := range tableDesc.Sets {
		inputJSON := fmt.Sprintf(`{"ownerTableName":%q,"ownerRecordID":%v,"setName":%q}`,
			tableName, recordID, setDesc.Name)
		resultJSON, err := GetSetMembers_DML(inputJSON)
		if err != nil {
			return nil, fmt.Errorf("failed to capture set %q: %v", setDesc.Name, err)
		}
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			return nil, fmt.Errorf("failed to parse set members result: %v", err)
		}
		if result["status"] != "success" {
			continue // set may be empty or record has no set — skip
		}
		if members, ok := result["members"].([]interface{}); ok {
			var ids []float64
			for _, m := range members {
				if id, ok := m.(float64); ok {
					ids = append(ids, id)
				}
			}
			if len(ids) > 0 {
				setMembers[setDesc.Name] = ids
			}
		}
	}
	return setMembers, nil
}

// Batch_DML executes a batch of mutating DML commands.
// It validates all commands before acquiring the mutex.
// Returns a JSON result string.
func Batch_DML(input string) (string, error) {
	commands, err := parseBatchCommands(input)
	if err != nil {
		return buildBatchErrorJSON("", -1, err.Error(), nil, nil), nil
	}

	if err := validateBatchCommands(commands); err != nil {
		return buildBatchErrorJSON("", -1, err.Error(), nil, nil), nil
	}

	// Acquire mutex for the entire batch
	dmlMutex.Lock()
	defer dmlMutex.Unlock()

	var results []batchCommandResult
	var undoStack []undoAction

	for i, cmd := range commands {
		// --- State capture before execution ---
		switch cmd.Name {
		case "update":
			// Capture old field values before update
			tableName := cmd.Params["tableName"].(string)
			recordID := cmd.Params["recordID"].(float64)
			oldRecord, err := captureRecordState(tableName, recordID)
			if err != nil {
				undoErrors := executeUndoStack(undoStack)
				return buildBatchErrorJSON(cmd.Name, i, err.Error(), results, undoErrors), nil
			}
			// Undo closure: update back to old values
			capturedTableName := tableName
			capturedRecordID := recordID
			capturedOldRecord := oldRecord
			undoStack = append(undoStack, undoAction{
				commandIndex: i,
				command:      "update",
				undoFunc: func() (string, error) {
					undoParams := map[string]interface{}{
						"tableName": capturedTableName,
						"recordID":  capturedRecordID,
						"record":    capturedOldRecord,
					}
					paramJSON, _ := json.Marshal(undoParams)
					return UpdateRecord_DML(string(paramJSON))
				},
			})

		case "delete":
			// Capture record fields and owned set members before delete
			tableName := cmd.Params["tableName"].(string)
			recordID := cmd.Params["recordID"].(float64)
			oldRecord, err := captureRecordState(tableName, recordID)
			if err != nil {
				undoErrors := executeUndoStack(undoStack)
				return buildBatchErrorJSON(cmd.Name, i, err.Error(), results, undoErrors), nil
			}
			ownedSets, err := captureOwnedSets(tableName, recordID)
			if err != nil {
				undoErrors := executeUndoStack(undoStack)
				return buildBatchErrorJSON(cmd.Name, i, err.Error(), results, undoErrors), nil
			}
			// Undo closure: re-add record then restore set members
			capturedTableName := tableName
			capturedOldRecord := oldRecord
			capturedOwnedSets := ownedSets
			undoStack = append(undoStack, undoAction{
				commandIndex: i,
				command:      "delete",
				undoFunc: func() (string, error) {
					// Re-add the record
					addParams := map[string]interface{}{
						"tableName": capturedTableName,
						"record":    capturedOldRecord,
					}
					paramJSON, _ := json.Marshal(addParams)
					addResultJSON, err := AddNewRecord_DML(string(paramJSON))
					if err != nil {
						return addResultJSON, err
					}
					// Parse to get the new recordID
					var addResult map[string]interface{}
					json.Unmarshal([]byte(addResultJSON), &addResult)
					if addResult["status"] != "success" {
						return addResultJSON, nil
					}
					newRecordID := addResult["recordID"].(float64)

					// Restore set members
					for setName, memberIDs := range capturedOwnedSets {
						for _, memberID := range memberIDs {
							setParams := map[string]interface{}{
								"ownerTableName": capturedTableName,
								"ownerRecordID":  newRecordID,
								"setName":        setName,
								"memberRecordID": memberID,
							}
							setJSON, _ := json.Marshal(setParams)
							AddSetMember_DML(string(setJSON))
						}
					}
					return addResultJSON, nil
				},
			})
		}

		// Marshal params back to JSON for the DML function
		paramJSON, err := json.Marshal(cmd.Params)
		if err != nil {
			undoErrors := executeUndoStack(undoStack)
			return buildBatchErrorJSON(cmd.Name, i,
				fmt.Sprintf("failed to marshal parameters: %v", err),
				results, undoErrors), nil
		}

		// Call the DML function
		dmlFunc := commandDMLFunc[cmd.Name]
		resultJSON, err := dmlFunc(string(paramJSON))
		if err != nil {
			undoErrors := executeUndoStack(undoStack)
			return buildBatchErrorJSON(cmd.Name, i,
				fmt.Sprintf("internal error: %v", err),
				results, undoErrors), nil
		}

		// Parse the DML result to check status
		var dmlResult map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &dmlResult); err != nil {
			undoErrors := executeUndoStack(undoStack)
			return buildBatchErrorJSON(cmd.Name, i,
				fmt.Sprintf("failed to parse DML result: %v", err),
				results, undoErrors), nil
		}

		status, _ := dmlResult["status"].(string)
		if status != "success" {
			// Command failed — collect error message
			failedError := "unknown error"
			if errs, ok := dmlResult["errors"].([]interface{}); ok && len(errs) > 0 {
				if msg, ok := errs[0].(string); ok {
					failedError = msg
				}
			}
			// Undo already-succeeded commands in reverse order
			undoErrors := executeUndoStack(undoStack)
			return buildBatchErrorJSON(cmd.Name, i, failedError, results, undoErrors), nil
		}

		// Build undo closures for commands that capture state AFTER execution
		switch cmd.Name {
		case "add":
			recordID := dmlResult["recordID"].(float64)
			capturedTableName := cmd.Params["tableName"].(string)
			capturedRecordID := recordID
			undoStack = append(undoStack, undoAction{
				commandIndex: i,
				command:      "add",
				undoFunc: func() (string, error) {
					delParams := map[string]interface{}{
						"tableName": capturedTableName,
						"recordID":  capturedRecordID,
					}
					paramJSON, _ := json.Marshal(delParams)
					return DeleteRecord_DML(string(paramJSON))
				},
			})
		case "addSetMember":
			capturedParams := cmd.Params
			undoStack = append(undoStack, undoAction{
				commandIndex: i,
				command:      "addSetMember",
				undoFunc: func() (string, error) {
					paramJSON, _ := json.Marshal(capturedParams)
					return RemoveSetMember_DML(string(paramJSON))
				},
			})
		case "removeSetMember":
			capturedParams := cmd.Params
			undoStack = append(undoStack, undoAction{
				commandIndex: i,
				command:      "removeSetMember",
				undoFunc: func() (string, error) {
					paramJSON, _ := json.Marshal(capturedParams)
					return AddSetMember_DML(string(paramJSON))
				},
			})
		}

		// Build result entry
		entry := batchCommandResult{
			Index:   i,
			Command: cmd.Name,
			Status:  "success",
		}
		if cmd.Name == "add" {
			if rid, ok := dmlResult["recordID"].(float64); ok {
				entry.RecordID = uint32(rid)
			}
		}
		results = append(results, entry)
	}

	return buildBatchSuccessJSON(results), nil
}

// executeUndoStack runs undo closures in reverse order, collecting any errors.
func executeUndoStack(undoStack []undoAction) []string {
	var undoErrors []string
	for i := len(undoStack) - 1; i >= 0; i-- {
		action := undoStack[i]
		resultJSON, err := action.undoFunc()
		if err != nil {
			undoErrors = append(undoErrors, fmt.Sprintf("undo %s (index %d): %v", action.command, action.commandIndex, err))
			continue
		}
		var result map[string]interface{}
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			undoErrors = append(undoErrors, fmt.Sprintf("undo %s (index %d): failed to parse result", action.command, action.commandIndex))
			continue
		}
		if status, _ := result["status"].(string); status != "success" {
			errMsg := "unknown error"
			if errs, ok := result["errors"].([]interface{}); ok && len(errs) > 0 {
				if msg, ok := errs[0].(string); ok {
					errMsg = msg
				}
			}
			undoErrors = append(undoErrors, fmt.Sprintf("undo %s (index %d): %s", action.command, action.commandIndex, errMsg))
		}
	}
	return undoErrors
}

// --- Batch response builders ---

type batchCommandResult struct {
	Index    int    `json:"index"`
	Command  string `json:"command"`
	Status   string `json:"status"`
	RecordID uint32 `json:"recordID,omitempty"`
}

func buildBatchSuccessJSON(results []batchCommandResult) string {
	resp := map[string]interface{}{
		"status":  "success",
		"results": results,
	}
	b, _ := json.Marshal(resp)
	return string(b)
}

func buildBatchErrorJSON(failedCommand string, failedAtIndex int, failedError string, results []batchCommandResult, undoErrors []string) string {
	resp := map[string]interface{}{
		"status":        "error",
		"failedAtIndex": failedAtIndex,
		"failedCommand": failedCommand,
		"failedError":   failedError,
		"results":       results,
		"undoErrors":    undoErrors,
	}
	b, _ := json.Marshal(resp)
	return string(b)
}
