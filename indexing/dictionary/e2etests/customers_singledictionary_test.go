package e2etests

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"github.com/kozwoj/indexing/dictionary/dictionary"
	"testing"
	"time"
)

// SingleDictCustomerRecord is the fixed-length record stored in memory.
// All string properties are replaced with uint32 dictionary IDs from a single shared dictionary.
type SingleDictCustomerRecord struct {
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

// SingleDictStore manages customer records with a single shared dictionary for all string properties.
type SingleDictStore struct {
	Records    []SingleDictCustomerRecord
	Dictionary *dictionary.Dictionary
}

// NewSingleDictStore creates or opens a store with a single dictionary for all customer strings.
func NewSingleDictStore(basePath string, cleanExisting bool, blockSizes dictionary.DictionaryBlockSizes, initialBlocks uint32) (*SingleDictStore, error) {
	// Clean existing data if requested
	if cleanExisting {
		if err := os.RemoveAll(basePath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to clean existing directory: %w", err)
		}
	}

	store := &SingleDictStore{
		Records: make([]SingleDictCustomerRecord, 0),
	}

	dictPath := basePath + "\\customer_strings_dict"

	// Try to open existing dictionary first
	dict, err := dictionary.OpenDictionary(dictPath, "CustomerStrings")
	if err != nil {
		// Create new dictionary if opening failed
		dict, err = dictionary.CreateDictionary(dictPath, "CustomerStrings", blockSizes, initialBlocks)
		if err != nil {
			return nil, err
		}
	}

	store.Dictionary = dict
	return store, nil
}

// Close closes the dictionary file
func (s *SingleDictStore) Close() error {
	if s.Dictionary != nil {
		return s.Dictionary.Close()
	}
	return nil
}

// InsertCustomer adds a new customer record to the store using a single dictionary.
func (s *SingleDictStore) InsertCustomer(
	customerID, companyName, contactName, contactTitle,
	address, city, region, postalCode, country, phone, fax string,
) error {
	recordID := uint32(len(s.Records))

	// Helper to add string to dictionary and postings
	addToDictAndPostings := func(value string) (uint32, error) {
		dictID, postingsRef, err := s.Dictionary.AddString(value)
		if err != nil {
			return 0, err
		}
		if err := s.Dictionary.AddRecordID(postingsRef, recordID, dictID); err != nil {
			return 0, err
		}
		return dictID, nil
	}

	// Add all fields to the single dictionary
	customerID_DictID, err := addToDictAndPostings(customerID)
	if err != nil {
		return err
	}

	companyName_DictID, err := addToDictAndPostings(companyName)
	if err != nil {
		return err
	}

	contactName_DictID, err := addToDictAndPostings(contactName)
	if err != nil {
		return err
	}

	contactTitle_DictID, err := addToDictAndPostings(contactTitle)
	if err != nil {
		return err
	}

	address_DictID, err := addToDictAndPostings(address)
	if err != nil {
		return err
	}

	city_DictID, err := addToDictAndPostings(city)
	if err != nil {
		return err
	}

	region_DictID, err := addToDictAndPostings(region)
	if err != nil {
		return err
	}

	postalCode_DictID, err := addToDictAndPostings(postalCode)
	if err != nil {
		return err
	}

	country_DictID, err := addToDictAndPostings(country)
	if err != nil {
		return err
	}

	phone_DictID, err := addToDictAndPostings(phone)
	if err != nil {
		return err
	}

	fax_DictID, err := addToDictAndPostings(fax)
	if err != nil {
		return err
	}

	// Create and store the record
	record := SingleDictCustomerRecord{
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
func (s *SingleDictStore) IngestCustomersFromFile(filePath string) error {
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

// TestCustomersSingleDictionary tests ingesting Northwind customers with a single shared dictionary
func TestCustomersSingleDictionary(t *testing.T) {
	basePath := "C:\\temp\\northwind\\customers_singledict_test"
	blockSizes := dictionary.DictionaryBlockSizes{
		PostingsBlockSize: 512,
		IndexBlockSize:    512,
		PrefixBlockSize:   512,
	}
	initialBlocks := uint32(100)

	t.Log("===========================================")
	t.Log("Northwind Customers Single Dictionary Test")
	t.Log("===========================================")

	// Start total timer
	totalStart := time.Now()

	// Create store with clean directory
	t.Log("Creating store with single shared dictionary...")
	storeStart := time.Now()
	store, err := NewSingleDictStore(basePath, true, blockSizes, initialBlocks)
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
