package server

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"github.com/kozwoj/step2/dml"

	"github.com/go-chi/chi/v5"
	"github.com/go-chi/chi/v5/middleware"
)

// Start initializes and starts the REST API server on the specified port
func Start(port int) error {
	r := chi.NewRouter()

	// Middleware stack
	r.Use(middleware.Logger)
	r.Use(middleware.Recoverer)
	r.Use(middleware.RequestID)

	// Route groups
	r.Route("/step2", func(r chi.Router) {
		r.Get("/", handleRootDiscovery)
		r.Route("/db", dbRoutes)
		r.Route("/db/info", dbInfoRoutes)
		r.Route("/record", recordRoutes)
		r.Route("/record/get", recordGetRoutes)
		r.Route("/set", setRoutes)
		r.Route("/batch", batchRoutes)
	})

	addr := fmt.Sprintf(":%d", port)
	log.Printf("Starting STEP2 REST server on port %d", port)
	log.Printf("Root endpoint: http://localhost:%d/step2/", port)

	return http.ListenAndServe(addr, r)
}

// executeMutatingDMLCommand wraps executeDMLCommand with the DML mutex
// for individual mutating operations (add, update, delete, set changes).
func executeMutatingDMLCommand(w http.ResponseWriter, r *http.Request, commandFunc func(string) (string, error)) {
	dml.LockDML()
	defer dml.UnlockDML()
	executeDMLCommand(w, r, commandFunc)
}

// executeDMLCommand handles DML commands that take a JSON string parameter
// and return (string, error). It decodes the request body, calls the DML function,
// and sends the JSON response or error.
func executeDMLCommand(w http.ResponseWriter, r *http.Request, commandFunc func(string) (string, error)) {

	// 1. Decode JSON from request body into map
	var input map[string]interface{}
	if err := json.NewDecoder(r.Body).Decode(&input); err != nil {
		sendError(w, http.StatusBadRequest, fmt.Sprintf("Invalid JSON: %v", err))
		return
	}

	// 2. Marshal map back to JSON string
	inputJSON, err := json.Marshal(input)
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to marshal input: %v", err))
		return
	}

	// 3. Call DML command with JSON string
	resultJSON, err := commandFunc(string(inputJSON))
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Command failed: %v", err))
		return
	}

	// 4. Unmarshal result and send as JSON response
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to unmarshal result: %v", err))
		return
	}

	sendJSON(w, result)
}

// executeNoParamCommand handles DML commands that take no parameters
// and return (string, error). It calls the DML function and sends the JSON response or error.
func executeNoParamCommand(w http.ResponseWriter, r *http.Request, commandFunc func() (string, error)) {

	// Call DML command
	resultJSON, err := commandFunc()
	if err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Command failed: %v", err))
		return
	}

	// Unmarshal result and send as JSON response
	var result map[string]interface{}
	if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
		sendError(w, http.StatusInternalServerError, fmt.Sprintf("Failed to unmarshal result: %v", err))
		return
	}

	sendJSON(w, result)
}

// sendJSON sends a JSON response with the given data
func sendJSON(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(data); err != nil {
		log.Printf("Error encoding JSON response: %v", err)
	}
}

// sendError sends an error response with the given status code and message
func sendError(w http.ResponseWriter, statusCode int, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	json.NewEncoder(w).Encode(map[string]string{"error": message})
}

// handleRootDiscovery returns information about available API endpoints
func handleRootDiscovery(w http.ResponseWriter, r *http.Request) {
	sendJSON(w, map[string]interface{}{
		"service":     "STEP2 REST API",
		"version":     "1.0",
		"description": "REST interface for STEP2 database operations",
		"available_domains": []string{
			"/step2/db/",
			"/step2/db/info/",
			"/step2/record/",
			"/step2/record/get/",
			"/step2/set/",
			"/step2/batch",
		},
		"help": "Add trailing slash to domain endpoints for detailed information (e.g., /step2/db/)",
	})
}
