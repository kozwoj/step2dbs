package e2etests

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/kozwoj/indexing/dictionary/dictionary"
	"github.com/kozwoj/indexing/dictionary/postings"
)

// BitmapMultiDictStore manages customer records with separate dictionaries using bitmap format.
type BitmapMultiDictStore struct {
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

// NewBitmapMultiDictStore creates or opens a store with all customer dictionaries using bitmap format.
func NewBitmapMultiDictStore(basePath string, cleanExisting bool, blockSizes dictionary.DictionaryBlockSizes, initialBlocks uint32) (*BitmapMultiDictStore, error) {
	// Clean existing data if requested
	if cleanExisting {
		if err := os.RemoveAll(basePath); err != nil && !os.IsNotExist(err) {
			return nil, fmt.Errorf("failed to clean existing directory: %w", err)
		}
	}

	store := &BitmapMultiDictStore{
		Records: make([]CustomerRecord, 0),
	}

	// Helper to create or open a dictionary with bitmap format
	openOrCreateDict := func(fieldName string) (*dictionary.Dictionary, error) {
		dirPath := filepath.Join(basePath, fieldName+"_dict")

		// Try to open existing dictionary first
		dict, err := dictionary.OpenDictionary(dirPath, fieldName)
		if err == nil {
			return dict, nil
		}

		// Create new dictionary if opening failed - using CreateDictionaryWithFormat for bitmap
		dict, err = dictionary.CreateDictionaryWithFormat(dirPath, fieldName, blockSizes, initialBlocks, postings.FormatBitmap)
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
func (s *BitmapMultiDictStore) Close() error {
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
func (s *BitmapMultiDictStore) InsertCustomer(
	customerID, companyName, contactName, contactTitle,
	address, city, region, postalCode, country, phone, fax string,
) error {
	recordID := uint32(len(s.Records))

	// Helper to add string to dictionary and postings
	addField := func(dict *dictionary.Dictionary, value string) (uint32, error) {
		dictID, postingsRef, err := dict.AddString(value)
		if err != nil {
			return 0, fmt.Errorf("failed to add string: %w", err)
		}
		err = dict.AddRecordID(postingsRef, recordID, dictID)
		if err != nil {
			return 0, fmt.Errorf("failed to add record ID: %w", err)
		}
		return dictID, nil
	}

	// Process all fields
	record := CustomerRecord{RecordID: recordID}

	var err error
	record.CustomerID_DictID, err = addField(s.CustomerID_Dict, customerID)
	if err != nil {
		return err
	}

	record.CompanyName_DictID, err = addField(s.CompanyName_Dict, companyName)
	if err != nil {
		return err
	}

	record.ContactName_DictID, err = addField(s.ContactName_Dict, contactName)
	if err != nil {
		return err
	}

	record.ContactTitle_DictID, err = addField(s.ContactTitle_Dict, contactTitle)
	if err != nil {
		return err
	}

	record.Address_DictID, err = addField(s.Address_Dict, address)
	if err != nil {
		return err
	}

	record.City_DictID, err = addField(s.City_Dict, city)
	if err != nil {
		return err
	}

	record.Region_DictID, err = addField(s.Region_Dict, region)
	if err != nil {
		return err
	}

	record.PostalCode_DictID, err = addField(s.PostalCode_Dict, postalCode)
	if err != nil {
		return err
	}

	record.Country_DictID, err = addField(s.Country_Dict, country)
	if err != nil {
		return err
	}

	record.Phone_DictID, err = addField(s.Phone_Dict, phone)
	if err != nil {
		return err
	}

	record.Fax_DictID, err = addField(s.Fax_Dict, fax)
	if err != nil {
		return err
	}

	s.Records = append(s.Records, record)
	return nil
}

// IngestCustomersFromFile reads customers from a .sjo file and inserts them into the store.
func (s *BitmapMultiDictStore) IngestCustomersFromFile(filePath string) (int, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return 0, fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	count := 0

	for scanner.Scan() {
		line := scanner.Text()
		if line == "" {
			continue
		}

		var custJSON CustomerJSON
		err := json.Unmarshal([]byte(line), &custJSON)
		if err != nil {
			return count, fmt.Errorf("failed to parse JSON at line %d: %w", count+1, err)
		}

		cust := custJSON.Record
		err = s.InsertCustomer(
			cust.CustomerID, cust.CompanyName, cust.ContactName, cust.ContactTitle,
			cust.Address, cust.City, cust.Region, cust.PostalCode,
			cust.Country, cust.Phone, cust.Fax,
		)
		if err != nil {
			return count, fmt.Errorf("failed to insert customer at line %d: %w", count+1, err)
		}
		count++
	}

	if err := scanner.Err(); err != nil {
		return count, fmt.Errorf("error reading file: %w", err)
	}

	return count, nil
}

func TestCustomersBitmapMultiDictionary(t *testing.T) {
	t.Log("===========================================")
	t.Log("Northwind Customers Multi-Dictionary Test (BITMAP FORMAT)")
	t.Log("===========================================")

	basePath := filepath.Join(t.TempDir(), "customers_bitmap_test")
	blockSizes := dictionary.DictionaryBlockSizes{
		PostingsBlockSize: 512,
		IndexBlockSize:    512,
		PrefixBlockSize:   512,
	}
	initialBlocks := uint32(100)

	t.Log("Creating store with 11 dictionaries using BITMAP format...")
	storeStart := time.Now()
	store, err := NewBitmapMultiDictStore(basePath, true, blockSizes, initialBlocks)
	if err != nil {
		t.Fatalf("Failed to create store: %v", err)
	}
	defer store.Close()
	storeCreationTime := time.Since(storeStart)

	t.Logf("Store created in %v", storeCreationTime)

	t.Log("Ingesting customer data from ../test_data/customers.sjo...")
	ingestStart := time.Now()
	count, err := store.IngestCustomersFromFile("../test_data/customers.sjo")
	if err != nil {
		t.Fatalf("Failed to ingest customers: %v", err)
	}
	ingestionTime := time.Since(ingestStart)

	totalTime := time.Since(storeStart)

	t.Log("===========================================")
	t.Log("Performance Statistics (BITMAP FORMAT)")
	t.Log("===========================================")
	t.Logf("Total records:        %d", count)
	t.Logf("Store creation time:  %v", storeCreationTime)
	t.Logf("Ingestion time:       %v", ingestionTime)
	t.Logf("Total time:           %v", totalTime)

	if count > 0 {
		avgTimePerRecord := ingestionTime.Microseconds() / int64(count)
		recordsPerSecond := float64(count) / ingestionTime.Seconds()
		t.Logf("Avg time per record:  %d μs", avgTimePerRecord)
		t.Logf("Records per second:   %.2f", recordsPerSecond)
	}

	t.Log("===========================================")
	t.Logf("Dictionary Storage: %s", basePath)
	t.Log("===========================================")
}
