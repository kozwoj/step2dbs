package e2etests

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"testing"
	"time"
)

// CustomerJSON represents the JSON structure from the .sjo file
// Format: {"record":{...}, "recordName":"Customer"}
type CustomerJSON struct {
	Record     CustomerData `json:"record"`
	RecordName string       `json:"recordName"`
}

// CustomerData represents the actual customer fields from JSON
type CustomerData struct {
	CustomerID   string `json:"CustomerID"`
	CompanyName  string `json:"CompanyName"`
	ContactName  string `json:"ContactName"`
	ContactTitle string `json:"ContactTitle"`
	Address      string `json:"Address"`
	City         string `json:"City"`
	Region       string `json:"Region"`
	PostalCode   string `json:"PostalCode"`
	Country      string `json:"Country"`
	Phone        string `json:"Phone"`
	Fax          string `json:"Fax"`
}

// CustomerRecord is the fixed-length record stored in memory.
// All string properties are replaced with uint32 dictionary IDs.
type CustomerRecord struct {
	RecordID            uint32
	CustomerID_DictID   uint32
	CompanyName_DictID  uint32
	ContactName_DictID  uint32
	ContactTitle_DictID uint32
	Address_DictID      uint32
	City_DictID         uint32
	Region_DictID       uint32
	PostalCode_DictID   uint32
	Country_DictID      uint32
	Phone_DictID        uint32
	Fax_DictID          uint32
}

// MultiDictStore manages customer records with separate dictionaries for each string property.
type MultiDictStore struct {
	Records           []CustomerRecord
	CustomerID_Dict   *dictionary.Dictionary
	CompanyName_Dict  *dictionary.Dictionary
	ContactName_Dict  *dictionary.Dictionary
	ContactTitle_Dict *dictionary.Dictionary
	Address_Dict      *dictionary.Dictionary
	City_Dict         *dictionary.Dictionary
	Region_Dict       *dictionary.Dictionary
	PostalCode_Dict   *dictionary.Dictionary
	Country_Dict      *dictionary.Dictionary
	Phone_Dict        *dictionary.Dictionary
	Fax_Dict          *dictionary.Dictionary
}

// NewMultiDictStore creates or opens a store with all customer dictionaries.
func NewMultiDictStore(basePath string, cleanExisting bool, blockSizes dictionary.DictionaryBlockSizes, initialBlocks uint32) (*MultiDictStore, error) {
	// Clean existing data if requested
	if cleanExisting {
		if err := os.RemoveAll(basePath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to clean existing directory: %w", err)
		}
	}

	store := &MultiDictStore{
		Records: make([]CustomerRecord, 0),
	}

	// Helper to create or open a dictionary
	openOrCreateDict := func(fieldName string) (*dictionary.Dictionary, error) {
		dirPath := basePath + "\\" + fieldName + "_dict"

		// Try to open existing dictionary first
		dict, err := dictionary.OpenDictionary(dirPath, fieldName)
		if err == nil {
			return dict, nil
		}

		// Create new dictionary if opening failed
		dict, err = dictionary.CreateDictionary(dirPath, fieldName, blockSizes, initialBlocks)
		if err != nil {
			return nil, err
		}
		return dict, nil
	}

	var err error

	// Create/open all 11 dictionaries
	store.CustomerID_Dict, err = openOrCreateDict("CustomerID")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.CompanyName_Dict, err = openOrCreateDict("CompanyName")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.ContactName_Dict, err = openOrCreateDict("ContactName")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.ContactTitle_Dict, err = openOrCreateDict("ContactTitle")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.Address_Dict, err = openOrCreateDict("Address")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.City_Dict, err = openOrCreateDict("City")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.Region_Dict, err = openOrCreateDict("Region")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.PostalCode_Dict, err = openOrCreateDict("PostalCode")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.Country_Dict, err = openOrCreateDict("Country")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.Phone_Dict, err = openOrCreateDict("Phone")
	if err != nil {
		store.Close()
		return nil, err
	}

	store.Fax_Dict, err = openOrCreateDict("Fax")
	if err != nil {
		store.Close()
		return nil, err
	}

	return store, nil
}

// Close closes all dictionary files
func (s *MultiDictStore) Close() error {
	var firstErr error

	dicts := []*dictionary.Dictionary{
		s.CustomerID_Dict, s.CompanyName_Dict, s.ContactName_Dict,
		s.ContactTitle_Dict, s.Address_Dict, s.City_Dict,
		s.Region_Dict, s.PostalCode_Dict, s.Country_Dict,
		s.Phone_Dict, s.Fax_Dict,
	}

	for _, dict := range dicts {
		if dict != nil {
			if err := dict.Close(); err != nil && firstErr == nil {
				firstErr = err
			}
		}
	}

	return firstErr
}

// InsertCustomer adds a new customer record to the store.
func (s *MultiDictStore) InsertCustomer(
	customerID, companyName, contactName, contactTitle,
	address, city, region, postalCode, country, phone, fax string,
) error {
	recordID := uint32(len(s.Records))

	// Helper to add string to dictionary and postings
	addToDictAndPostings := func(dict *dictionary.Dictionary, value string) (uint32, error) {
		dictID, postingsRef, err := dict.AddString(value)
		if err != nil {
			return 0, err
		}
		if err := dict.AddRecordID(postingsRef, recordID, dictID); err != nil {
			return 0, err
		}
		return dictID, nil
	}

	// Add all fields
	customerID_DictID, err := addToDictAndPostings(s.CustomerID_Dict, customerID)
	if err != nil {
		return err
	}

	companyName_DictID, err := addToDictAndPostings(s.CompanyName_Dict, companyName)
	if err != nil {
		return err
	}

	contactName_DictID, err := addToDictAndPostings(s.ContactName_Dict, contactName)
	if err != nil {
		return err
	}

	contactTitle_DictID, err := addToDictAndPostings(s.ContactTitle_Dict, contactTitle)
	if err != nil {
		return err
	}

	address_DictID, err := addToDictAndPostings(s.Address_Dict, address)
	if err != nil {
		return err
	}

	city_DictID, err := addToDictAndPostings(s.City_Dict, city)
	if err != nil {
		return err
	}

	region_DictID, err := addToDictAndPostings(s.Region_Dict, region)
	if err != nil {
		return err
	}

	postalCode_DictID, err := addToDictAndPostings(s.PostalCode_Dict, postalCode)
	if err != nil {
		return err
	}

	country_DictID, err := addToDictAndPostings(s.Country_Dict, country)
	if err != nil {
		return err
	}

	phone_DictID, err := addToDictAndPostings(s.Phone_Dict, phone)
	if err != nil {
		return err
	}

	fax_DictID, err := addToDictAndPostings(s.Fax_Dict, fax)
	if err != nil {
		return err
	}

	// Create and store the record
	record := CustomerRecord{
		RecordID:            recordID,
		CustomerID_DictID:   customerID_DictID,
		CompanyName_DictID:  companyName_DictID,
		ContactName_DictID:  contactName_DictID,
		ContactTitle_DictID: contactTitle_DictID,
		Address_DictID:      address_DictID,
		City_DictID:         city_DictID,
		Region_DictID:       region_DictID,
		PostalCode_DictID:   postalCode_DictID,
		Country_DictID:      country_DictID,
		Phone_DictID:        phone_DictID,
		Fax_DictID:          fax_DictID,
	}

	s.Records = append(s.Records, record)
	return nil
}

// IngestCustomersFromFile reads a .sjo file and inserts all customer records.
func (s *MultiDictStore) IngestCustomersFromFile(filePath string) error {
	file, err := os.Open(filePath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	lineNumber := 0

	for scanner.Scan() {
		lineNumber++
		line := scanner.Text()

		if len(line) == 0 {
			continue
		}

		var customerJSON CustomerJSON
		if err := json.Unmarshal([]byte(line), &customerJSON); err != nil {
			return fmt.Errorf("failed to parse JSON at line %d: %w", lineNumber, err)
		}

		customer := customerJSON.Record

		err := s.InsertCustomer(
			customer.CustomerID, customer.CompanyName, customer.ContactName,
			customer.ContactTitle, customer.Address, customer.City,
			customer.Region, customer.PostalCode, customer.Country,
			customer.Phone, customer.Fax,
		)
		if err != nil {
			return fmt.Errorf("failed to insert customer at line %d: %w", lineNumber, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading file: %w", err)
	}

	return nil
}

// TestCustomersMultiDictionary tests ingesting Northwind customers with separate dictionaries per field
func TestCustomersMultiDictionary(t *testing.T) {
	basePath := filepath.Join(t.TempDir(), "customers_multidict_test")
	blockSizes := dictionary.DictionaryBlockSizes{
		PostingsBlockSize: 512,
		IndexBlockSize:    512,
		PrefixBlockSize:   512,
	}
	initialBlocks := uint32(100)

	t.Log("===========================================")
	t.Log("Northwind Customers Multi-Dictionary Test")
	t.Log("===========================================")

	// Start total timer
	totalStart := time.Now()

	// Create store with clean directory
	t.Log("Creating store with 11 dictionaries...")
	storeStart := time.Now()
	store, err := NewMultiDictStore(basePath, true, blockSizes, initialBlocks)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()
	storeTime := time.Since(storeStart)
	t.Logf("Store created in %v", storeTime)

	// Ingest customer data
	t.Log("Ingesting customer data from ../test_data/customers.sjo...")
	ingestStart := time.Now()
	err = store.IngestCustomersFromFile("../test_data/customers.sjo")
	if err != nil {
		t.Fatalf("Failed to ingest customers: %v", err)
	}
	ingestTime := time.Since(ingestStart)

	// Calculate total time
	totalTime := time.Since(totalStart)

	// Verify record count
	expectedRecords := 91
	if len(store.Records) != expectedRecords {
		t.Errorf("Expected %d records, got %d", expectedRecords, len(store.Records))
	}

	// Print statistics
	t.Log("===========================================")
	t.Log("Performance Statistics")
	t.Log("===========================================")
	t.Logf("Total records:        %d", len(store.Records))
	t.Logf("Store creation time:  %v", storeTime)
	t.Logf("Ingestion time:       %v", ingestTime)
	t.Logf("Total time:           %v", totalTime)

	if len(store.Records) > 0 {
		avgPerRecord := ingestTime.Microseconds() / int64(len(store.Records))
		recordsPerSec := float64(len(store.Records)) / ingestTime.Seconds()
		t.Logf("Avg time per record:  %d μs", avgPerRecord)
		t.Logf("Records per second:   %.2f", recordsPerSec)
	}

	t.Log("===========================================")
	t.Logf("Dictionary Storage: %s", basePath)
	t.Log("===========================================")
}
