package main

// go build -o step2.exe .

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strings"

	"github.com/kozwoj/step2/server"
	"github.com/kozwoj/step2cli/cli"
)

func main() {
	// Check if a subcommand was provided
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	subcommand := os.Args[1]

	switch subcommand {
	case "server":
		runServer()
	case "schema":
		runSchema()
	case "query":
		runQuery()
	default:
		fmt.Printf("Unknown command: %s\n\n", subcommand)
		printUsage()
		os.Exit(1)
	}
}

func runServer() {
	// Create flagset for server subcommand
	serverCmd := flag.NewFlagSet("server", flag.ExitOnError)
	port := serverCmd.Int("port", 8080, "Port number for the REST server")

	// Parse flags (skip program name and subcommand)
	serverCmd.Parse(os.Args[2:])

	// Start the REST server
	log.Printf("Starting STEP2 REST server on port %d...\n", *port)
	if err := server.Start(*port); err != nil {
		log.Fatalf("Failed to start server: %v", err)
	}
}

func runSchema() {
	// Create flagset for schema subcommand
	schemaCmd := flag.NewFlagSet("schema", flag.ExitOnError)
	schemaPath := schemaCmd.String("path", "", "Path to the DDL schema file (required)")
	showStorage := schemaCmd.Bool("storage", false, "Display database storage structure")

	// Parse flags (skip program name and subcommand)
	schemaCmd.Parse(os.Args[2:])

	// Validate required flags
	if *schemaPath == "" {
		fmt.Println("Error: -path flag is required for schema command")
		fmt.Println("Usage: step2 schema -path <schema_file> [-storage]")
		os.Exit(1)
	}

	// Analyze and display the schema
	if err := cli.AnalyzeSchema(*schemaPath, *showStorage); err != nil {
		log.Fatalf("Schema analysis failed: %v", err)
	}
}

func runQuery() {
	// Create flagset for query subcommand
	queryCmd := flag.NewFlagSet("query", flag.ExitOnError)
	dbPath := queryCmd.String("dbpath", "", "Path to the existing database (required)")
	queryFile := queryCmd.String("file", "", "Path to a file containing the query pipeline (required)")

	// Parse flags (skip program name and subcommand)
	queryCmd.Parse(os.Args[2:])

	// Validate required flags
	if *dbPath == "" || *queryFile == "" {
		fmt.Println("Error: -dbpath and -file flags are required for query command")
		fmt.Println("\nUsage: step2 query -dbpath <db_path> -file <query_file>")
		os.Exit(1)
	}

	// Read the query file
	data, err := os.ReadFile(*queryFile)
	if err != nil {
		fmt.Printf("Error: failed to read query file: %v\n", err)
		os.Exit(1)
	}

	// Join lines into a single pipeline string
	pipelineText := strings.Join(strings.Fields(strings.TrimSpace(string(data))), " ")

	if err := cli.ExecuteQuery(*dbPath, pipelineText); err != nil {
		os.Exit(1)
	}
}

func printUsage() {
	fmt.Println("STEP2 - Database Management System")
	fmt.Println("\nUsage:")
	fmt.Println("  step2 <command> [options]")
	fmt.Println("\nAvailable Commands:")
	fmt.Println("  server    Start the REST API server")
	fmt.Println("  schema    Parse and analyze a DDL schema file")
	fmt.Println("  query     Execute a query against an existing database")
	fmt.Println("\nServer Command Options:")
	fmt.Println("  step2 server -port <port_number>")
	fmt.Println("    -port    Port number for the REST server (default: 8080)")
	fmt.Println("\nSchema Command Options:")
	fmt.Println("  step2 schema -path <schema_file_path> [-storage]")
	fmt.Println("    -path      Path to the DDL schema file (required)")
	fmt.Println("    -storage   Display database storage directory structure")
	fmt.Println("\nQuery Command Options:")
	fmt.Println("  step2 query -dbpath <db_path> -file <query_file>")
	fmt.Println("    -dbpath    Path to the existing database (required)")
	fmt.Println("    -file      Path to a file containing the query pipeline (required)")
	fmt.Println("\nExamples:")
	fmt.Println("  step2 server -port 8080")
	fmt.Println("  step2 schema -path docs/testdata/College.ddl")
	fmt.Println("  step2 schema -path docs/testdata/College.ddl -storage")
	fmt.Println(`  step2 query -dbpath C:\mydb\College -file query.txt`)
}
