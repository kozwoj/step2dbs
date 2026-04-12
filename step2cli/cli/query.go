package cli

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2query/builder"
	"github.com/kozwoj/step2query/engine"
	"github.com/kozwoj/step2query/parser"
)

// ExecuteQuery opens a database, parses and executes a query pipeline,
// and prints the result as a JSON array of objects.
func ExecuteQuery(dbPath string, pipelineText string) error {
	// Open the database
	if err := db.OpenDB(dbPath); err != nil {
		return printQueryError(fmt.Errorf("failed to open database: %w", err))
	}
	defer db.CloseDB()

	dbDef := db.Definition()

	// Parse the pipeline
	query, err := parser.Parse(pipelineText)
	if err != nil {
		return printQueryError(fmt.Errorf("parse error: %w", err))
	}

	// Validate the AST against the schema
	if err := parser.ValidateAST(query, dbDef); err != nil {
		return printQueryError(fmt.Errorf("validation error: %w", err))
	}

	// Build the execution plan
	plan, err := builder.BuildPipeline(query, dbDef)
	if err != nil {
		return printQueryError(fmt.Errorf("build error: %w", err))
	}

	// Execute the pipeline
	resultState, err := engine.ExecutePipeline(plan, dbDef)
	if err != nil {
		return printQueryError(fmt.Errorf("execution error: %w", err))
	}

	// Collect results into a JSON array
	var records []map[string]interface{}

	rec, err := resultState.GetFirstRecord()
	for err == nil {
		records = append(records, rec)
		rec, err = resultState.GetNextRecord()
	}
	if !errors.Is(err, engine.ErrNoMoreRecords) {
		return printQueryError(fmt.Errorf("error reading results: %w", err))
	}

	if records == nil {
		records = []map[string]interface{}{}
	}

	// Output as JSON
	output, err := json.MarshalIndent(records, "", "  ")
	if err != nil {
		return printQueryError(fmt.Errorf("JSON encoding error: %w", err))
	}

	fmt.Println(string(output))
	return nil
}

// printQueryError outputs the error as a JSON object and returns it.
func printQueryError(err error) error {
	errObj := map[string]string{"error": err.Error()}
	output, _ := json.MarshalIndent(errObj, "", "  ")
	fmt.Println(string(output))
	return err
}
