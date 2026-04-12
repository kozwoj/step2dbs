package server

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
)

// batchRoutes registers the batch DML endpoint
func batchRoutes(r chi.Router) {
	r.Get("/", handleBatchHelp)
	r.Post("/", handleBatch)
}

// handleBatch executes a batch of mutating DML commands.
// Batch_DML handles its own mutex locking, so we read the raw body
// and pass it directly rather than using executeMutatingDMLCommand.
func handleBatch(w http.ResponseWriter, r *http.Request) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("failed to read request body: %v", err))
		return
	}

	resultJSON, err := dml.Batch_DML(string(body))
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("batch execution failed: %v", err))
		return
	}

	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("failed to parse batch result: %v", err))
		return
	}

	sendJSON(w, result)
}

// handleBatchHelp returns help information for the batch endpoint
func handleBatchHelp(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"command":     "Batch",
		"description": "Executes a batch of mutating DML commands atomically with best-effort undo on failure",
		"method":      "POST",
		"endpoint":    "/step2/batch",
		"input_format": map[string]interface{}{
			"commands": []map[string]interface{}{
				{"add": map[string]string{"tableName": "...", "record": "{...}"}},
				{"update": map[string]string{"tableName": "...", "recordID": "number", "record": "{...}"}},
				{"delete": map[string]string{"tableName": "...", "recordID": "number"}},
				{"addSetMember": map[string]string{"ownerTableName": "...", "ownerRecordID": "number", "setName": "...", "memberRecordID": "number"}},
				{"removeSetMember": map[string]string{"ownerTableName": "...", "ownerRecordID": "number", "setName": "...", "memberRecordID": "number"}},
			},
		},
		"notes": []string{
			"Commands execute sequentially in order",
			"If any command fails, previously succeeded commands are undone in reverse order",
			"Delete undo restores owned set members but cannot restore member-of references",
		},
	})
}
