package performance

import (
	"encoding/json"
	"testing"
)

// TestCustomerGenerator verifies that the generator creates valid customer records
func TestCustomerGenerator(t *testing.T) {
	gen := NewCustomerGenerator(12345) // Fixed seed for reproducibility

	t.Run("Generate single customer", func(t *testing.T) {
		customer := gen.GenerateCustomer(1)

		// Verify all required fields are present
		requiredFields := []string{
			"Customer_id", "Company_name", "Contact_name", "Contact_title",
			"Address", "City", "Region", "Postal_code", "Country", "Phone", "Fax",
		}

		for _, field := range requiredFields {
			if _, ok := customer[field]; !ok {
				t.Errorf("Missing required field: %s", field)
			}
		}

		// Verify Customer_id format
		customerId, ok := customer["Customer_id"].(string)
		if !ok {
			t.Errorf("Customer_id is not a string")
		}
		if len(customerId) != 10 {
			t.Errorf("Customer_id length should be 10, got %d: %s", len(customerId), customerId)
		}
	})

	t.Run("Generate multiple customers - uniqueness check", func(t *testing.T) {
		count := 100
		customerIds := make(map[string]bool)
		companyNames := make(map[string]bool)
		addresses := make(map[string]bool)
		phones := make(map[string]bool)

		for i := 1; i <= count; i++ {
			customer := gen.GenerateCustomer(i)

			// Collect unique values
			customerIds[customer["Customer_id"].(string)] = true
			companyNames[customer["Company_name"].(string)] = true
			addresses[customer["Address"].(string)] = true
			phones[customer["Phone"].(string)] = true
		}

		// Verify high cardinality for fields that should be unique
		if len(customerIds) != count {
			t.Errorf("Expected %d unique Customer_ids, got %d", count, len(customerIds))
		}
		if len(companyNames) != count {
			t.Errorf("Expected %d unique Company_names, got %d", count, len(companyNames))
		}
		if len(addresses) != count {
			t.Errorf("Expected %d unique Addresses, got %d", count, len(addresses))
		}
		if len(phones) != count {
			t.Errorf("Expected %d unique Phones, got %d", count, len(phones))
		}

		t.Logf("Cardinality out of %d records:", count)
		t.Logf("  Customer IDs: %d (%.0f%%)", len(customerIds), float64(len(customerIds))/float64(count)*100)
		t.Logf("  Company Names: %d (%.0f%%)", len(companyNames), float64(len(companyNames))/float64(count)*100)
		t.Logf("  Addresses: %d (%.0f%%)", len(addresses), float64(len(addresses))/float64(count)*100)
		t.Logf("  Phones: %d (%.0f%%)", len(phones), float64(len(phones))/float64(count)*100)
	})

	t.Run("Print sample customers", func(t *testing.T) {
		t.Log("\nSample generated customers:")
		t.Log("========================================")

		for i := 1; i <= 5; i++ {
			customer := gen.GenerateCustomer(i)

			t.Logf("\nCustomer #%d:", i)
			jsonBytes, _ := json.MarshalIndent(customer, "  ", "  ")
			t.Logf("  %s", string(jsonBytes))
		}

		// Also show customer 500 to demonstrate variety
		t.Log("\nCustomer #500 (to show variety):")
		customer500 := gen.GenerateCustomer(500)
		jsonBytes, _ := json.MarshalIndent(customer500, "  ", "  ")
		t.Logf("  %s", string(jsonBytes))
	})

	t.Run("Field length validation", func(t *testing.T) {
		// Generate several customers and check field lengths match schema constraints
		for i := 1; i <= 100; i++ {
			customer := gen.GenerateCustomer(i)

			// Schema constraints from Customer_Employee.ddl:
			// Customer_id CHAR[10] - exactly 10
			// Company_name STRING(40) - max 40
			// Contact_name STRING(30) - max 30
			// Contact_title STRING(30) - max 30
			// Address STRING(60) - max 60
			// City STRING(15) - max 15
			// Region STRING(15) - max 15
			// Postal_code STRING(10) - max 10
			// Country STRING(15) - max 15
			// Phone STRING(15) - max 15
			// Fax STRING(24) - max 24

			if len(customer["Customer_id"].(string)) != 10 {
				t.Errorf("Customer_id length must be exactly 10, got %d at index %d",
					len(customer["Customer_id"].(string)), i)
			}
			if len(customer["Company_name"].(string)) > 40 {
				t.Errorf("Company_name exceeds 40 chars at index %d", i)
			}
			if len(customer["Contact_name"].(string)) > 30 {
				t.Errorf("Contact_name exceeds 30 chars at index %d", i)
			}
			if len(customer["Contact_title"].(string)) > 30 {
				t.Errorf("Contact_title exceeds 30 chars at index %d", i)
			}
			if len(customer["Address"].(string)) > 60 {
				t.Errorf("Address exceeds 60 chars at index %d", i)
			}
			if len(customer["City"].(string)) > 15 {
				t.Errorf("City exceeds 15 chars at index %d", i)
			}
			if len(customer["Region"].(string)) > 15 {
				t.Errorf("Region exceeds 15 chars at index %d", i)
			}
			if len(customer["Postal_code"].(string)) > 10 {
				t.Errorf("Postal_code exceeds 10 chars at index %d", i)
			}
			if len(customer["Country"].(string)) > 15 {
				t.Errorf("Country exceeds 15 chars at index %d", i)
			}
			if len(customer["Phone"].(string)) > 15 {
				t.Errorf("Phone exceeds 15 chars at index %d", i)
			}
			if len(customer["Fax"].(string)) > 24 {
				t.Errorf("Fax exceeds 24 chars at index %d", i)
			}
		}
	})
}
