package main

import (
	"fmt"
	"log"
	"github.com/kozwoj/step2/db"
)

func main() {
	fmt.Println("STEP2 Query Language Prototype")

	// Example: Create a DBDefinition from a schema file
	// You can use any DDL file from the step2/cli/test directory
	schemaFile := `..\step2\docs\testdata\College.ddl`

	dbDef, err := db.NewDBDefinitionFromSchema(schemaFile)
	if err != nil {
		log.Fatalf("Failed to create DB definition: %v", err)
	}
	fmt.Printf("\nDatabase: %s\n", dbDef.Name)
	fmt.Printf("Tables: %d\n", len(dbDef.Tables))

	for _, table := range dbDef.Tables {
		fmt.Printf("\nTable: %s\n", table.Name)
		fmt.Printf("  Fields: %d\n", len(table.RecordLayout.Fields))
		for _, field := range table.RecordLayout.Fields {
			fmt.Printf("    - %s (%v)\n", field.Name, field.Type)
		}
	}

	// Your query parser/planner code goes here
	// Now you have a working DBDefinition to work with!
}
