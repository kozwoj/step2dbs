package performance

import (
	"fmt"
	"math/rand"
	"time"
)

// CustomerGenerator provides functions to generate customer records with extreme
// cardinality (minimal string repetition) for performance testing.
type CustomerGenerator struct {
	rand *rand.Rand
}

// NewCustomerGenerator creates a new customer generator with a seeded random source.
func NewCustomerGenerator(seed int64) *CustomerGenerator {
	return &CustomerGenerator{
		rand: rand.New(rand.NewSource(seed)),
	}
}

// GenerateCustomer creates a customer record with highly unique values to simulate
// extreme conditions where dictionary compression has minimal benefit.
//
// Parameters:
//   - index: sequential index (1-based) for the customer
//
// Returns:
//   - map[string]interface{} containing all customer fields
func (g *CustomerGenerator) GenerateCustomer(index int) map[string]interface{} {
	return map[string]interface{}{
		"Customer_id":   g.generateCustomerID(index),
		"Company_name":  g.generateCompanyName(index),
		"Contact_name":  g.generateContactName(index),
		"Contact_title": g.generateContactTitle(index),
		"Address":       g.generateAddress(index),
		"City":          g.generateCity(index),
		"Region":        g.generateRegion(index),
		"Postal_code":   g.generatePostalCode(index),
		"Country":       g.generateCountry(index),
		"Phone":         g.generatePhone(index),
		"Fax":           g.generateFax(index),
	}
}

// generateCustomerID creates a unique 10-character customer ID.
// Format: CUST000001, CUST000002, etc.
func (g *CustomerGenerator) generateCustomerID(index int) string {
	return fmt.Sprintf("CUST%06d", index)
}

// generateCompanyName creates a unique company name with minimal repetition.
// Format: Company_{index}_{randomSuffix}
func (g *CustomerGenerator) generateCompanyName(index int) string {
	suffixes := []string{"Inc", "LLC", "Corp", "Ltd", "GmbH", "SA", "Co", "Group", "Enterprises", "Solutions"}
	suffix := suffixes[g.rand.Intn(len(suffixes))]
	randomNum := g.rand.Intn(9999)
	return fmt.Sprintf("Company_%d_%s_%d", index, suffix, randomNum)
}

// generateContactName creates a unique contact name.
// Format: FirstName_{index % pool} LastName_{index}
func (g *CustomerGenerator) generateContactName(index int) string {
	firstNames := []string{"John", "Jane", "Michael", "Sarah", "David", "Emily", "Robert", "Lisa",
		"William", "Maria", "James", "Jennifer", "Richard", "Patricia", "Charles", "Linda",
		"Joseph", "Barbara", "Thomas", "Elizabeth", "Christopher", "Susan", "Daniel", "Jessica"}

	lastNames := []string{"Smith", "Johnson", "Williams", "Brown", "Jones", "Garcia", "Miller", "Davis",
		"Rodriguez", "Martinez", "Hernandez", "Lopez", "Gonzalez", "Wilson", "Anderson", "Thomas",
		"Taylor", "Moore", "Jackson", "Martin", "Lee", "Perez", "Thompson", "White"}

	firstName := firstNames[index%len(firstNames)]
	lastName := lastNames[index%len(lastNames)]

	// Add suffix to make unique
	return fmt.Sprintf("%s_%d %s_%d", firstName, index/len(firstNames), lastName, index)
}

// generateContactTitle creates contact titles with some repetition (realistic).
// About 20-30 different titles for variety.
func (g *CustomerGenerator) generateContactTitle(index int) string {
	titles := []string{
		"Sales Representative", "Marketing Manager", "Owner", "Sales Manager",
		"Marketing Director", "Accounting Manager", "Sales Associate", "Order Administrator",
		"CEO", "CFO", "CTO", "VP Sales", "VP Marketing", "Account Manager",
		"Business Development Manager", "Regional Manager", "Sales Director",
		"Customer Service Manager", "Operations Manager", "General Manager",
		"Assistant Sales Agent", "Marketing Coordinator", "Sales Coordinator",
		"Export Administrator", "Sales Agent", "Marketing Assistant",
		"Account Executive", "Business Manager", "District Manager", "Purchasing Manager",
	}
	// Some repetition but still varied
	return titles[index%len(titles)]
}

// generateAddress creates a unique street address.
// Format: {number} {street_name} {street_type}
func (g *CustomerGenerator) generateAddress(index int) string {
	streetNames := []string{"Main", "Oak", "Maple", "Cedar", "Elm", "Washington", "Park", "Lake",
		"Hill", "Church", "Market", "Union", "River", "Center", "High", "School", "State", "Broad"}

	streetTypes := []string{"St", "Ave", "Blvd", "Dr", "Ln", "Rd", "Way", "Ct", "Pl", "Ter"}

	streetNum := 100 + (index * 17) % 9900 // Unique street numbers
	streetName := streetNames[index%len(streetNames)]
	streetType := streetTypes[g.rand.Intn(len(streetTypes))]

	// Add suffix for uniqueness
	return fmt.Sprintf("%d %s %s Unit_%d", streetNum, streetName, streetType, index)
}

// generateCity creates city names with moderate repetition.
// About 50-100 different cities for 1000 records. All names <= 15 chars.
func (g *CustomerGenerator) generateCity(index int) string {
	cities := []string{
		"New York", "Los Angeles", "Chicago", "Houston", "Phoenix", "Philadelphia", "San Antonio",
		"San Diego", "Dallas", "San Jose", "Austin", "Jacksonville", "Columbus",
		"Charlotte", "San Francisco", "Indianapolis", "Seattle", "Denver", "Washington",
		"Boston", "El Paso", "Nashville", "Detroit", "Oklahoma", "Portland", "Las Vegas",
		"Memphis", "Louisville", "Baltimore", "Milwaukee", "Albuquerque", "Tucson", "Fresno",
		"Sacramento", "Kansas City", "Mesa", "Atlanta", "Omaha", "Raleigh",
		"Miami", "Long Beach", "Oakland", "Minneapolis", "Tulsa", "Tampa",
		"Arlington", "New Orleans", "Wichita", "Cleveland", "Bakersfield", "Aurora", "Anaheim",
		"Honolulu", "Santa Ana", "Riverside", "Lexington", "Henderson",
		"Stockton", "Saint Paul", "Cincinnati", "Greensboro", "Pittsburgh", "Irvine", "Lincoln",
		"Orlando", "Toledo", "Fort Wayne", "Jersey City", "Chandler", "Buffalo",
		"Durham", "Reno", "Madison", "Lubbock", "Gilbert", "Winston", "Glendale", "Hialeah",
		"Garland", "Scottsdale", "Boise", "Norfolk", "Spokane", "Fremont", "Richmond", "Irving",
		"Baton Rouge", "Birmingham", "Rochester", "Anchorage", "Des Moines",
	}
	// Moderate repetition - cycle through cities
	return cities[index%len(cities)]
}

// generateRegion creates region names with moderate repetition.
// About 30 different regions.
func (g *CustomerGenerator) generateRegion(index int) string {
	regions := []string{
		"Northeast", "Southeast", "Midwest", "Southwest", "West Coast", "Northwest",
		"Central", "Eastern", "Western", "Northern", "Southern", "Mid-Atlantic",
		"New England", "Pacific", "Mountain", "Great Lakes", "Gulf Coast", "Atlantic",
		"Plains", "Deep South", "Upper Midwest", "Lower Midwest", "Far West", "Sunbelt",
		"Rust Belt", "Bible Belt", "Corn Belt", "Silicon Valley", "Wine Country", "Delta",
	}
	// Some regions can be empty (realistic)
	if index%7 == 0 {
		return ""
	}
	return regions[index%len(regions)]
}

// generatePostalCode creates unique postal codes.
func (g *CustomerGenerator) generatePostalCode(index int) string {
	// US-style ZIP codes with some variation
	base := 10000 + (index*37)%89999
	return fmt.Sprintf("%05d", base)
}

// generateCountry creates country names with moderate repetition.
// About 15-20 different countries (realistic global distribution).
func (g *CustomerGenerator) generateCountry(index int) string {
	countries := []string{
		"USA", "Canada", "Mexico", "UK", "Germany", "France", "Italy", "Spain",
		"Brazil", "Argentina", "Japan", "China", "India", "Australia", "Netherlands",
		"Belgium", "Switzerland", "Sweden", "Norway", "Denmark",
	}
	return countries[index%len(countries)]
}

// generatePhone creates unique phone numbers.
// Format: (###) ###-#### where # are digits
func (g *CustomerGenerator) generatePhone(index int) string {
	areaCode := 200 + (index%800)
	exchange := 200 + ((index*7)%800)
	line := (index * 13) % 10000
	return fmt.Sprintf("(%03d) %03d-%04d", areaCode, exchange, line)
}

// generateFax creates unique fax numbers.
// Some records may have empty fax (realistic).
func (g *CustomerGenerator) generateFax(index int) string {
	// 20% of records have no fax
	if index%5 == 0 {
		return ""
	}

	areaCode := 200 + ((index*3)%800)
	exchange := 200 + ((index*11)%800)
	line := (index * 17) % 10000
	return fmt.Sprintf("(%03d) %03d-%04d", areaCode, exchange, line)
}

// init seeds the global random number generator
func init() {
	rand.Seed(time.Now().UnixNano())
}
