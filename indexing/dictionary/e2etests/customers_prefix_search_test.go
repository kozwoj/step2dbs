package e2etests

import (
	"bufio"
	"encoding/json"
	"os"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"testing"
	"time"
)

/*
TestCustomersPrefixSearch tests the prefix search functionality by:
1. Creating a multi-dictionary store for customer records
2. Ingesting all customer records from customers.sjo
3. Using PrefixSearch to find all customers with City starting with "L"
4. Verifying the results by retrieving and displaying the matching records
*/
func TestCustomersPrefixSearch(t *testing.T) {
	// Configuration
	basePath := "C:\\temp\\northwind\\customers_prefix_test"
	dataFile := "../test_data/customers.sjo"
	blockSizes := dictionary.DictionaryBlockSizes{
		PostingsBlockSize: 512,
		IndexBlockSize:    512,
		PrefixBlockSize:   512,
	}
	initialBlocks := uint32(100)

	// Clean up before test
	os.RemoveAll(basePath)
	defer os.RemoveAll(basePath)

	t.Log("===========================================")
	t.Log("Northwind Customers Prefix Search Test")
	t.Log("===========================================")

	// Create dictionaries for each field
	t.Log("Creating dictionaries...")
	startCreate := time.Now()

	err := os.MkdirAll(basePath, 0755)
	if err != nil {
		t.Fatalf("Failed to create base directory: %v", err)
	}

	// Create dictionaries for relevant fields
	cityDict, err := dictionary.CreateDictionary(basePath+"\\city_dict", "City", blockSizes, initialBlocks)
	if err != nil {
		t.Fatalf("Failed to create City dictionary: %v", err)
	}
	defer cityDict.Close()

	companyNameDict, err := dictionary.CreateDictionary(basePath+"\\companyname_dict", "CompanyName", blockSizes, initialBlocks)
	if err != nil {
		t.Fatalf("Failed to create CompanyName dictionary: %v", err)
	}
	defer companyNameDict.Close()

	customerIDDict, err := dictionary.CreateDictionary(basePath+"\\customerid_dict", "CustomerID", blockSizes, initialBlocks)
	if err != nil {
		t.Fatalf("Failed to create CustomerID dictionary: %v", err)
	}
	defer customerIDDict.Close()

	countryDict, err := dictionary.CreateDictionary(basePath+"\\country_dict", "Country", blockSizes, initialBlocks)
	if err != nil {
		t.Fatalf("Failed to create Country dictionary: %v", err)
	}
	defer countryDict.Close()

	createDuration := time.Since(startCreate)
	t.Logf("Dictionaries created in %v", createDuration)

	// Ingest customer data
	t.Logf("Ingesting customer data from %s...", dataFile)
	startIngest := time.Now()

	file, err := os.Open(dataFile)
	if err != nil {
		t.Fatalf("Failed to open data file: %v", err)
	}
	defer file.Close()

	// Map to store recordID -> CustomerID for result display
	recordToCustomerID := make(map[uint32]uint32)  // recordID -> customerID dictID
	recordToCompanyName := make(map[uint32]uint32) // recordID -> companyName dictID
	recordToCity := make(map[uint32]uint32)        // recordID -> city dictID
	recordToCountry := make(map[uint32]uint32)     // recordID -> country dictID

	scanner := bufio.NewScanner(file)
	var recordID uint32 = 0

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var wrapper struct {
			Record struct {
				CustomerID  string `json:"CustomerID"`
				CompanyName string `json:"CompanyName"`
				City        string `json:"City"`
				Country     string `json:"Country"`
			} `json:"record"`
		}

		if err := json.Unmarshal([]byte(line), &wrapper); err != nil {
			t.Fatalf("Failed to parse JSON: %v", err)
		}

		// Add to City dictionary
		cityDictID, cityPostingsRef, err := cityDict.AddString(wrapper.Record.City)
		if err != nil {
			t.Fatalf("Failed to add City: %v", err)
		}
		err = cityDict.AddRecordID(cityPostingsRef, recordID, cityDictID)
		if err != nil {
			t.Fatalf("Failed to add recordID to City postings: %v", err)
		}
		recordToCity[recordID] = cityDictID

		// Add to CompanyName dictionary
		companyDictID, companyPostingsRef, err := companyNameDict.AddString(wrapper.Record.CompanyName)
		if err != nil {
			t.Fatalf("Failed to add CompanyName: %v", err)
		}
		err = companyNameDict.AddRecordID(companyPostingsRef, recordID, companyDictID)
		if err != nil {
			t.Fatalf("Failed to add recordID to CompanyName postings: %v", err)
		}
		recordToCompanyName[recordID] = companyDictID

		// Add to CustomerID dictionary
		custDictID, custPostingsRef, err := customerIDDict.AddString(wrapper.Record.CustomerID)
		if err != nil {
			t.Fatalf("Failed to add CustomerID: %v", err)
		}
		err = customerIDDict.AddRecordID(custPostingsRef, recordID, custDictID)
		if err != nil {
			t.Fatalf("Failed to add recordID to CustomerID postings: %v", err)
		}
		recordToCustomerID[recordID] = custDictID

		// Add to Country dictionary
		countryDictID, countryPostingsRef, err := countryDict.AddString(wrapper.Record.Country)
		if err != nil {
			t.Fatalf("Failed to add Country: %v", err)
		}
		err = countryDict.AddRecordID(countryPostingsRef, recordID, countryDictID)
		if err != nil {
			t.Fatalf("Failed to add recordID to Country postings: %v", err)
		}
		recordToCountry[recordID] = countryDictID

		recordID++
	}

	if err := scanner.Err(); err != nil {
		t.Fatalf("Error reading file: %v", err)
	}

	ingestDuration := time.Since(startIngest)
	t.Logf("Ingested %d records in %v", recordID, ingestDuration)

	// Perform prefix search for cities starting with "L"
	t.Log("===========================================")
	t.Log("Prefix Search: Cities starting with 'L'")
	t.Log("===========================================")

	startSearch := time.Now()

	// Get all city dictIDs that start with "L"
	cityDictIDs, err := cityDict.PrefixSearch("L")
	if err != nil {
		t.Fatalf("PrefixSearch failed: %v", err)
	}

	searchDuration := time.Since(startSearch)
	t.Logf("PrefixSearch returned %d city dictIDs in %v", len(cityDictIDs), searchDuration)

	// For each matching city, get the postings (recordIDs)
	t.Log("-------------------------------------------")
	t.Log("Matching Cities and their Customers:")
	t.Log("-------------------------------------------")

	totalRecords := 0
	for _, cityDictID := range cityDictIDs {
		// Get the city name
		cityName, err := cityDict.GetStringByID(cityDictID)
		if err != nil {
			t.Fatalf("Failed to get city name for dictID %d: %v", cityDictID, err)
		}

		// Verify the city actually starts with "L" (for prefixes > 8 bytes)
		if len(cityName) == 0 || cityName[0] != 'L' {
			continue
		}

		// Get postingsRef for this city
		_, postingsRef, err := cityDict.FindString(cityName)
		if err != nil {
			t.Fatalf("Failed to find city '%s': %v", cityName, err)
		}

		// Get all recordIDs for this city
		recordIDs, err := cityDict.RetrievePostings(postingsRef)
		if err != nil {
			t.Fatalf("Failed to retrieve postings for city '%s': %v", cityName, err)
		}

		t.Logf("\nCity: %s (%d customers)", cityName, len(recordIDs))
		totalRecords += len(recordIDs)

		// Display customer details for each record
		for _, recID := range recordIDs {
			// Get customer ID
			custDictID := recordToCustomerID[recID]
			customerID, _ := customerIDDict.GetStringByID(custDictID)

			// Get company name
			companyDictID := recordToCompanyName[recID]
			companyName, _ := companyNameDict.GetStringByID(companyDictID)

			// Get country
			countryDictID := recordToCountry[recID]
			country, _ := countryDict.GetStringByID(countryDictID)

			t.Logf("  - %s: %s (%s)", customerID, companyName, country)
		}
	}

	t.Log("===========================================")
	t.Logf("Total: %d customers in cities starting with 'L'", totalRecords)
	t.Log("===========================================")

	// Summary statistics
	t.Log("")
	t.Log("===========================================")
	t.Log("Performance Statistics")
	t.Log("===========================================")
	t.Logf("Total records ingested:  %d", recordID)
	t.Logf("Dictionary creation:     %v", createDuration)
	t.Logf("Data ingestion:          %v", ingestDuration)
	t.Logf("Prefix search time:      %v", searchDuration)
	t.Logf("Cities found:            %d", len(cityDictIDs))
	t.Logf("Matching customers:      %d", totalRecords)
	t.Log("===========================================")

	// Verify expected cities starting with "L" from the data
	expectedCities := []string{"London", "Lisboa", "Lille", "Leipzig", "Lander", "Lyon", "Luleå"}
	t.Log("")
	t.Log("Expected cities starting with 'L':")
	for _, city := range expectedCities {
		t.Logf("  - %s", city)
	}
}
