# Customer Insert Performance Test Results

## Test Environment
- **Package**: step2/performance
- **Schema**: Customer_Employee.ddl (11 string fields per record)
- **Test Type**: Fresh database inserts (empty DB at start)
- **Generator**: High cardinality data (minimal string repetition)

## Performance Results Summary

### 1K Records Test
```
Records Attempted:    1,000
Records Succeeded:    1,000
Records Failed:       0

--- Timing ---
DB Creation:          195.53 ms
DB Open:              110.00 ms
Insert Total:         928.85 ms
Total Time:           1.23 s

--- Throughput ---
Avg per record:       928 μs
Records/second:       1,076.60
```

### 10K Records Test
```
Records Attempted:    10,000
Records Succeeded:    10,000
Records Failed:       0

--- Timing ---
DB Creation:          194.13 ms
DB Open:              112.04 ms
Insert Total:         10.35 s
Total Time:           10.66 s

--- Throughput ---
Avg per record:       1,035 μs
Records/second:       966.03
```

## Performance Analysis

### Scaling Characteristics
- **10x data volume** (1K → 10K records)
- **11.4% slower per record** (929 μs → 1,035 μs)
- **10.2% lower throughput** (1,076 → 966 records/sec)
- **Conclusion**: Good linear scaling with moderate degradation

### Performance Degradation Factors
As the database grows:
1. B-tree indexes get deeper (more nodes to traverse)
2. Dictionary lookups take longer (more strings to search)
3. More data structures in memory

## Dictionary Compression Analysis

### High Cardinality Fields (1.00x - No Compression)
These fields are unique per record by design:
- Customer_id, Company_name, Contact_name, Address, Phone, Postal_code

| Field         | 1K Test | 10K Test | Compression |
|---------------|---------|----------|-------------|
| Company_name  | 1,000   | 10,000   | 1.00x       |
| Contact_name  | 1,000   | 10,000   | 1.00x       |
| Address       | 1,000   | 10,000   | 1.00x       |
| Phone         | 1,000   | 10,000   | 1.00x       |
| Postal_code   | 1,000   | 10,000   | 1.00x       |
| Fax           | 801     | 8,001    | 1.25x       |

### Moderate Repetition Fields (Good Compression)
These fields have realistic repetition patterns:

| Field         | 1K Test | 10K Test | Unique | 1K Ratio | 10K Ratio |
|---------------|---------|----------|--------|----------|-----------|
| Contact_title | 1,000   | 10,000   | 30     | 33.33x   | 333.33x   |
| City          | 1,000   | 10,000   | 91     | 10.99x   | 109.89x   |
| Region        | 1,000   | 10,000   | 31     | 32.26x   | 322.58x   |
| Country       | 1,000   | 10,000   | 20     | 50.00x   | 500.00x   |

**Key Insight**: Compression ratios scale linearly with record count for fields with limited unique values. This demonstrates effective dictionary compression for real-world data patterns.

## Next Steps for Testing

### Planned Tests
1. ✅ Fresh DB insert - 1K records
2. ✅ Fresh DB insert - 10K records
3. ⏳ Incremental insert to pre-populated DB (measure degradation)
4. ⏳ 100K record test (large-scale performance)
5. ⏳ Memory usage profiling
6. ⏳ Query performance on populated databases

### Questions to Explore
- How does performance degrade when adding to a pre-populated database?
- What is the performance at 100K records?
- What are the memory usage patterns?
- How do dictionary compression ratios affect query performance?

## Running the Tests

```powershell
# Run 1K test
go test -v step2/performance -run TestCustomerInsert1K -timeout 5m

# Run 10K test
go test -v step2/performance -run TestCustomerInsert10K -timeout 5m

# Run both
go test -v step2/performance -run TestCustomerInsert -timeout 10m
```
