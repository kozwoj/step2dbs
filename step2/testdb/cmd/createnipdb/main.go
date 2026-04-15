package main

import (
	"fmt"
	"os"

	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/testdb"
)

func main() {
	if len(os.Args) != 2 {
		fmt.Fprintf(os.Stderr, "Usage: createnipdb <target-directory>\n")
		fmt.Fprintf(os.Stderr, "Creates a populated North Idaho Politechnic database in <target-directory>/College\n")
		os.Exit(1)
	}

	targetDir := os.Args[1]

	if err := os.MkdirAll(targetDir, 0o755); err != nil {
		fmt.Fprintf(os.Stderr, "Error creating directory: %v\n", err)
		os.Exit(1)
	}

	dbPath, stats, err := testdb.CreateAndPopulateNIPDatabase(targetDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}
	defer db.CloseDB()

	fmt.Printf("Database created at: %s\n", dbPath)
	fmt.Println("Record counts:")
	for table, count := range stats {
		fmt.Printf("  %-15s: %d\n", table, count)
	}
}
