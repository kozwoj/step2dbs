package main

import (
	"flag"
	"fmt"
	"os"
	"github.com/kozwoj/step2cli/cli"
)

func main() {
	// Define command-line flags
	pathFlag := flag.String("path", "", "Path to DDL schema file (required)")
	storageFlag := flag.Bool("storage", false, "Show database directory structure")

	flag.Parse()

	// Validate required flag
	if *pathFlag == "" {
		fmt.Println("Usage: schema_test -path <schema-file.ddl> [-storage]")
		flag.PrintDefaults()
		os.Exit(1)
	}

	// Run schema analysis
	if err := cli.AnalyzeSchema(*pathFlag, *storageFlag); err != nil {
		os.Exit(1)
	}
}
