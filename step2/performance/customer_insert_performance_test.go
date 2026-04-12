package performance

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kozwoj/step2/db"
	"github.com/kozwoj/step2/record"
)

// TestCustomerInsert1K tests inserting 1,000 customer records into a fresh database
func TestCustomerInsert1K(t *testing.T) {
	runCustomerInsertTest(t, 1000, "1K")
}

// TestCustomerInsert10K tests inserting 10,000 customer records into a fresh database
func TestCustomerInsert10K(t *testing.T) {
	runCustomerInsertTest(t, 10000, "10K")
}

// getDirectorySize calculates the total size of all files in a directory recursively
func getDirectorySize(path string) (int64, error) {
	var totalSize int64
	err := filepath.WalkDir(path, func(_ string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if !d.IsDir() {
			info, err := d.Info()
			if err != nil {
				return err
			}
			totalSize += info.Size()
		}
		return nil
	})
	return totalSize, err
}

// formatBytes converts bytes to a human-readable format
func formatBytes(bytes int64) string {
	const unit = 1024
	if bytes < unit {
		return fmt.Sprintf("%d B", bytes)
	}
	div, exp := int64(unit), 0
	for n := bytes / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(bytes)/float64(div), "KMGTPE"[exp])
}

// runCustomerInsertTest is the core performance test function that creates a fresh database
// and inserts the specified number of customer records.
func runCustomerInsertTest(t *testing.T, recordCount int, testLabel string) {
	t.Logf("=======================================================")
	t.Logf("Customer Insert Performance Test - %s Records", testLabel)
	t.Logf("=======================================================")

	// Create temporary directory for test database
	// Set persistDB to true to keep database files for manual inspection
	persistDB := false
	var tempDir string

	if persistDB {
		tempDir = filepath.Join(os.TempDir(), fmt.Sprintf("customer_perf_%s", testLabel))
		t.Logf("Using persistent directory: %s", tempDir)
	} else {
		tempDir = t.TempDir()
		t.Logf("Using temporary directory (will be cleaned up)")
	}

	// Path to the Customer_Employee.ddl schema
	schemaFile := filepath.Join("..", "docs", "testdata", "Customer_Employee.ddl")

	// Time: Database Creation
	t.Log("\n--- Phase 1: Database Creation ---")
	createStart := time.Now()
	err := db.CreateDB(tempDir, schemaFile)
	if err != nil {
		t.Fatalf("CreateDB failed: %v", err)
	}
	createTime := time.Since(createStart)
	t.Logf("Database created in: %v", createTime)

	// Open the database
	dbPath := filepath.Join(tempDir, "TestSchema")
	openStart := time.Now()
	err = db.OpenDB(dbPath)
	if err != nil {
		t.Fatalf("OpenDB failed: %v", err)
	}
	defer db.CloseDB()
	openTime := time.Since(openStart)
	t.Logf("Database opened in: %v", openTime)

	dbDef := db.Definition()
	if dbDef == nil {
		t.Fatal("Database definition is nil")
	}

	// Time: Record Generation and Insertion
	t.Logf("\n--- Phase 2: Generating and Inserting %d Records ---", recordCount)
	gen := NewCustomerGenerator(42) // Fixed seed for reproducibility

	insertStart := time.Now()
	var insertErrors []error
	successCount := 0

	// Track timing for batches to show progress
	batchSize := recordCount / 10
	if batchSize == 0 {
		batchSize = 100
	}
	lastBatchTime := time.Now()

	for i := 1; i <= recordCount; i++ {
		// Generate customer record
		customer := gen.GenerateCustomer(i)

		// Insert record using the record package
		_, err := record.AddNewRecord("Customers", customer, dbDef)
		if err != nil {
			insertErrors = append(insertErrors, fmt.Errorf("record %d: %w", i, err))
			if len(insertErrors) <= 10 { // Only log first 10 errors
				t.Logf("  ERROR inserting record %d: %v", i, err)
			}
		} else {
			successCount++
		}

		// Progress reporting
		if i%batchSize == 0 {
			batchTime := time.Since(lastBatchTime)
			t.Logf("  Inserted %d records (batch time: %v)", i, batchTime)
			lastBatchTime = time.Now()
		}
	}

	insertTime := time.Since(insertStart)

	// Report any errors
	if len(insertErrors) > 0 {
		t.Logf("\n⚠️  Encountered %d errors during insertion (showing first 10)", len(insertErrors))
		if len(insertErrors) > 10 {
			t.Logf("  (... and %d more errors)", len(insertErrors)-10)
		}
	}

	// Time: Get database statistics
	t.Log("\n--- Phase 3: Collecting Statistics ---")
	statsStart := time.Now()
	tableStats, errors, err := db.GetTableStats([]string{"Customers"})
	if err != nil {
		t.Errorf("GetTableStats failed: %v", err)
	}
	if len(errors) > 0 {
		t.Errorf("GetTableStats returned errors: %v", errors)
	}
	statsTime := time.Since(statsStart)
	t.Logf("Statistics collected in: %v", statsTime)

	// Calculate database file sizes
	dbSize, err := getDirectorySize(dbPath)
	if err != nil {
		t.Logf("Warning: Could not calculate database size: %v", err)
	}

	// Analyze individual dictionary sizes to understand storage distribution
	tablePath := filepath.Join(dbPath, "Customers")
	dictSizes := make(map[string]int64)
	if fields, ok := tableStats[0]["dictionaries"].([]map[string]interface{}); ok {
		for _, field := range fields {
			fieldName := field["field_name"].(string)
			dictPath := filepath.Join(tablePath, fieldName)
			if size, err := getDirectorySize(dictPath); err == nil {
				dictSizes[fieldName] = size
			}
		}
	}

	// Print Performance Results
	t.Log("\n=======================================================")
	t.Log("PERFORMANCE RESULTS")
	t.Log("=======================================================")
	t.Logf("Test Scale:           %s records", testLabel)
	t.Logf("Records Attempted:    %d", recordCount)
	t.Logf("Records Succeeded:    %d", successCount)
	t.Logf("Records Failed:       %d", len(insertErrors))
	t.Log("")
	t.Log("--- Timing ---")
	t.Logf("DB Creation:          %v", createTime)
	t.Logf("DB Open:              %v", openTime)
	t.Logf("Insert Total:         %v", insertTime)
	t.Logf("Stats Collection:     %v", statsTime)

	totalTime := createTime + openTime + insertTime
	t.Logf("Total Time:           %v", totalTime)
	t.Log("")

	if successCount > 0 {
		avgPerRecord := insertTime.Microseconds() / int64(successCount)
		recordsPerSecond := float64(successCount) / insertTime.Seconds()

		t.Log("--- Throughput ---")
		t.Logf("Avg per record:       %d μs", avgPerRecord)
		t.Logf("Records/second:       %.2f", recordsPerSecond)
	}

	// Print Table Statistics
	if len(tableStats) > 0 {
		t.Log("")
		t.Log("--- Database Statistics ---")
		t.Logf("Database Path:        %s", dbPath)
		t.Logf("Database Size:        %s (%d bytes)", formatBytes(dbSize), dbSize)
		if successCount > 0 {
			bytesPerRecord := dbSize / int64(successCount)
			t.Logf("Bytes per record:     %s (%d bytes)", formatBytes(bytesPerRecord), bytesPerRecord)
		}
		t.Log("")

		for _, stat := range tableStats {
			tableName := stat["name"].(string)
			allocatedRecords := stat["allocated_records"].(int)
			deletedListLength := stat["deleted_list_length"].(int)
			activeRecords := allocatedRecords - deletedListLength

			t.Logf("Table:                %s", tableName)
			t.Logf("  Allocated:          %d records", allocatedRecords)
			t.Logf("  Deleted:            %d records", deletedListLength)
			t.Logf("  Active:             %d records", activeRecords)

			// Print dictionary statistics
			if dictionaries, ok := stat["dictionaries"].([]map[string]interface{}); ok && len(dictionaries) > 0 {
				t.Log("")
				t.Log("  --- Dictionary Compression ---")
				for _, dict := range dictionaries {
					fieldName := dict["field_name"].(string)
					numStrings := dict["number_of_strings"].(int)

					// Calculate compression ratio
					compressionRatio := 0.0
					if activeRecords > 0 {
						compressionRatio = float64(activeRecords) / float64(numStrings)
					}

					// Add size information if available
					sizeInfo := ""
					if size, ok := dictSizes[fieldName]; ok {
						sizeInfo = fmt.Sprintf(" [%s]", formatBytes(size))
					}

					t.Logf("  %-20s: %5d unique strings (ratio: %.2fx)%s",
						fieldName, numStrings, compressionRatio, sizeInfo)
				}
			}
		}
	}

	t.Log("=======================================================")

	// Verify record count matches
	if successCount != recordCount {
		t.Errorf("Expected %d successful inserts, got %d", recordCount, successCount)
	}

	if len(tableStats) > 0 {
		allocatedRecords := tableStats[0]["allocated_records"].(int)
		deletedListLength := tableStats[0]["deleted_list_length"].(int)
		activeRecords := allocatedRecords - deletedListLength

		if activeRecords != successCount {
			t.Errorf("Expected %d active records in database, got %d", successCount, activeRecords)
		}
	}

	t.Logf("\n✓ Performance test for %s records completed", testLabel)

	if persistDB {
		t.Logf("\n📁 Database files preserved at: %s", tempDir)
		t.Logf("   (Remember to manually delete when done inspecting)")
	}
}
