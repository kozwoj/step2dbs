# College Database Testing Scripts - Design Document

**Date:** March 4, 2026
**Purpose:** Test data generation and performance testing for STEP2 College database
**Context:** Following successful REST server implementation and basic CRUD testing

## Overview

After completing the REST server implementation (17 DML endpoints) and manual testing with basic records, the next phase involves:
1. Generate realistic College test data at scale
2. Implement batch loading with performance metrics
3. Measure database sizes and insertion rates
4. Analyze performance characteristics across different scales

## Current State

### Completed Work
- ✅ REST server operational on port 8080 (17 endpoints)
- ✅ PowerShell testing module: `scripts/step2.ps1` (20 functions)
- ✅ Basic CRUD testing: Departments and Teachers tables
- ✅ Foreign key validation confirmed working
- ✅ Primary key lookup verified (byID and byKey)
- ✅ CHAR field padding requirement documented

### Key Learnings
1. **CHAR Field Padding**: CHAR[N] fields must be exactly N characters
   - Example: `Department_code` CHAR[8] requires "CS      " not "CS"
   - Example: `Student_id` CHAR[10] requires "S00000001 " (with trailing space)
   - Padding with spaces to exact length is mandatory
   - Validation errors occur if length doesn't match schema

2. **Foreign Key Validation**: Automatic and immediate during Add-Record
3. **JSON Depth**: Use `-Depth 5` or higher for nested structures

## Test Data Generation Strategy

### 1. Dependency-Ordered Loading

Tables must be loaded in dependency order based on foreign key relationships:

**Level 1** (No dependencies):
- Departments
- Courses

**Level 2** (Depends on Level 1):
- Teachers (→ Departments)

**Level 3** (Depends on Levels 1-2):
- Majors (→ Departments)
- Students (→ Teachers as advisors)
- Classes (→ Teachers, Courses)

**Level 4** (Depends on Levels 1-3):
- Grades (→ Students, Classes)

**Level 5** (Set relationships):
- Teachers.Teaches → Classes
- Teachers.Advises → Students
- Students.TakesClasses → Classes
- Students.CompletedClasses → Grades

### 2. Realistic Data Sources

Predefined data arrays for realistic generation:

```powershell
# Names (30-50 each for good distribution)
$script:firstNames = @("Alice", "Bob", "Charlie", "Diana", "Emma", "Frank",
    "Grace", "Henry", "Iris", "Jack", "Karen", "Leo", "Maria", "Nathan",
    "Olivia", "Paul", "Quinn", "Rachel", "Sam", "Tina", "Uma", "Victor",
    "Wendy", "Xavier", "Yara", "Zack")

$script:lastNames = @("Anderson", "Brown", "Chen", "Davis", "Evans",
    "Fisher", "Garcia", "Harris", "Ibrahim", "Johnson", "Kim", "Lee",
    "Martinez", "Nguyen", "O'Brien", "Patel", "Quinn", "Rodriguez",
    "Smith", "Taylor", "Upton", "Vasquez", "Williams", "Xu", "Yang", "Zhang")

# Department data with building assignments
$script:deptData = @(
    @{Code="CS      "; Name="Computer Science"; Building="ENGR    "; BuildingName="Engineering Building"}
    @{Code="ENGL    "; Name="English"; Building="ARTS    "; BuildingName="Arts Building"}
    @{Code="MATH    "; Name="Mathematics"; Building="SCI     "; BuildingName="Science Building"}
    @{Code="PHYS    "; Name="Physics"; Building="SCI     "; BuildingName="Science Building"}
    @{Code="HIST    "; Name="History"; Building="ARTS    "; BuildingName="Arts Building"}
    @{Code="CHEM    "; Name="Chemistry"; Building="SCI     "; BuildingName="Science Building"}
    @{Code="PSYC    "; Name="Psychology"; Building="SOCSCI  "; BuildingName="Social Sciences"}
    @{Code="ECON    "; Name="Economics"; Building="SOCSCI  "; BuildingName="Social Sciences"}
    @{Code="BIO     "; Name="Biology"; Building="SCI     "; BuildingName="Science Building"}
    @{Code="ART     "; Name="Art"; Building="ARTS    "; BuildingName="Arts Building"}
)

# Grade distribution (weighted)
$script:gradeWeights = @{
    "A" = 15   # 15%
    "B" = 35   # 35%
    "C" = 30   # 30%
    "D" = 15   # 15%
    "F" = 5    # 5%
}
```

### 3. Size Profiles

Three test profiles for different scale testing:

**Small Profile** (Quick testing, ~10MB database):
- 10 Departments
- 50 Teachers (5 per dept average)
- 500 Students (50 per dept average)
- 100 Courses (10 per dept average)
- 200 Classes (2 per course average)
- ~2000 Grades (4 per student average)

**Medium Profile** (Realistic college, ~100MB database):
- 20 Departments
- 200 Teachers (10 per dept average)
- 5,000 Students (250 per dept average)
- 500 Courses (25 per dept average)
- 1,000 Classes (2 per course average)
- ~25,000 Grades (5 per student average)

**Large Profile** (Major university, ~1GB database):
- 50 Departments
- 1,000 Teachers (20 per dept average)
- 50,000 Students (1000 per dept average)
- 2,000 Courses (40 per dept average)
- 5,000 Classes (2.5 per course average)
- ~250,000 Grades (5 per student average)

### 4. Key Generation Patterns

Sequential generation with proper CHAR padding:

```powershell
function New-EmployeeID {
    param([int]$index)
    $numStr = $index.ToString().PadLeft(6, '0')
    return ("T" + $numStr + " ")  # CHAR[8]: "T000001 "
}

function New-StudentID {
    param([int]$index)
    $numStr = $index.ToString().PadLeft(8, '0')
    return ("S" + $numStr + " ")  # CHAR[10]: "S00000001 "
}

function New-CourseCode {
    param([string]$deptCode, [int]$level)
    $deptClean = $deptCode.Trim()
    $courseNum = (Get-Random -Minimum 100 -Maximum 499).ToString()
    return ($deptClean + $courseNum).PadRight(8)  # CHAR[8]: "CS101   "
}

function New-ClassCode {
    param([string]$courseCode, [int]$section)
    $courseClean = $courseCode.Trim()
    $sectionStr = $section.ToString().PadLeft(2, '0')
    return ($courseClean + "-" + $sectionStr).PadRight(10)  # CHAR[10]: "CS101-01  "
}
```

### 5. Distribution Logic

Weighted randomization for realistic proportions:

```powershell
function Get-WeightedDepartment {
    param([array]$departments)
    # Weight CS and ENGL higher (more popular majors)
    $weights = @{
        "CS      " = 25
        "ENGL    " = 20
        "MATH    " = 15
        "PSYC    " = 15
        "ECON    " = 10
        "default" = 5
    }

    $totalWeight = ($weights.Values | Measure-Object -Sum).Sum
    $random = Get-Random -Minimum 0 -Maximum $totalWeight

    $cumulative = 0
    foreach ($dept in $departments) {
        $weight = if ($weights.ContainsKey($dept.Code)) { $weights[$dept.Code] } else { $weights["default"] }
        $cumulative += $weight
        if ($random -lt $cumulative) {
            return $dept
        }
    }
    return $departments[-1]
}

function Get-WeightedGrade {
    # Returns A, B, C, D, or F based on realistic distribution
    $rand = Get-Random -Minimum 0 -Maximum 100
    if ($rand -lt 15) { return "A" }
    elseif ($rand -lt 50) { return "B" }  # 15-50 = 35%
    elseif ($rand -lt 80) { return "C" }  # 50-80 = 30%
    elseif ($rand -lt 95) { return "D" }  # 80-95 = 15%
    else { return "F" }  # 95-100 = 5%
}
```

### 6. Generator Functions

One function per table type:

```powershell
function New-Department {
    param([int]$index, [hashtable]$template)
    return @{
        Name = $template.Name
        Department_code = $template.Code
        Building_name = $template.BuildingName
        Building_code = $template.Building
    }
}

function New-Teacher {
    param([int]$index, [array]$departments)
    $dept = Get-WeightedDepartment -departments $departments
    $firstName = $script:firstNames | Get-Random
    $lastName = $script:lastNames | Get-Random
    $officeNum = (Get-Random -Minimum 100 -Maximum 999).ToString()

    return @{
        Employee_id = New-EmployeeID -index $index
        First_name = $firstName
        Last_name = $lastName
        Building_code = $dept.Building
        Office = $officeNum.PadRight(10)
        Works_for = $dept.Code
    }
}

function New-Student {
    param([int]$index, [array]$teachers, [array]$majors)
    $advisor = $teachers | Get-Random
    $major = $majors | Get-Random
    $firstName = $script:firstNames | Get-Random
    $lastName = $script:lastNames | Get-Random
    $year = Get-WeightedYear  # 1-4 with more upperclassmen
    $credits = Get-Random -Minimum (($year-1)*24) -Maximum ($year*32)

    return @{
        Student_id = New-StudentID -index $index
        First_name = $firstName
        Last_name = $lastName
        Advisor = $advisor.Employee_id
        Major = $major.Department_code
        Year = $year
        Credits = $credits
    }
}

# Similar functions for: New-Course, New-Class, New-Grade
```

### 7. Batch Loading with Metrics

Performance-focused loading infrastructure:

```powershell
function Load-DataBatch {
    param(
        [string]$tableName,
        [array]$records,
        [int]$batchSize = 100
    )

    $total = $records.Count
    $loaded = 0
    $errors = @()
    $recordIDs = @()

    $sw = [System.Diagnostics.Stopwatch]::StartNew()

    for ($i = 0; $i -lt $total; $i += $batchSize) {
        $end = [Math]::Min($i + $batchSize - 1, $total - 1)
        $batch = $records[$i..$end]

        foreach ($record in $batch) {
            try {
                $result = Add-Record -TableName $tableName -Record $record
                if ($result.status -eq "success") {
                    $loaded++
                    $recordIDs += $result.recordID
                } else {
                    $errors += "Record $i: $($result.errors)"
                }
            }
            catch {
                $errors += "Record $i: $_"
            }
        }

        # Progress bar
        $pct = [int](($loaded / $total) * 100)
        $rate = if ($sw.Elapsed.TotalSeconds -gt 0) {
            [int]($loaded / $sw.Elapsed.TotalSeconds)
        } else { 0 }

        Write-Progress -Activity "Loading $tableName" `
                       -Status "$loaded/$total records" `
                       -PercentComplete $pct `
                       -CurrentOperation "$rate records/sec"
    }

    Write-Progress -Activity "Loading $tableName" -Completed
    $sw.Stop()

    return @{
        Loaded = $loaded
        Failed = $errors.Count
        Errors = $errors
        Duration = $sw.Elapsed
        Rate = if ($sw.Elapsed.TotalSeconds -gt 0) { $loaded / $sw.Elapsed.TotalSeconds } else { 0 }
        RecordIDs = $recordIDs
    }
}
```

### 8. ID Lookup Cache

Critical for FK resolution performance:

```powershell
# Cache structure: PrimaryKey → RecordID mapping
$script:deptCache = @{}      # "CS      " → 1
$script:teacherCache = @{}   # "T000001 " → 5
$script:studentCache = @{}   # "S00000001" → 123
$script:courseCache = @{}    # "CS101   " → 42
$script:classCache = @{}     # "CS101-01  " → 87

# Build cache from load results
function Build-DepartmentCache {
    param([array]$departments, [array]$recordIDs)
    for ($i = 0; $i -lt $departments.Count; $i++) {
        $key = $departments[$i].Department_code
        $script:deptCache[$key] = $recordIDs[$i]
    }
}

# Use cache for FK lookup
function Get-CachedRecordID {
    param([string]$cacheType, [string]$key)
    $cache = switch ($cacheType) {
        "dept" { $script:deptCache }
        "teacher" { $script:teacherCache }
        "student" { $script:studentCache }
        "course" { $script:courseCache }
        "class" { $script:classCache }
    }
    return $cache[$key]
}
```

### 9. Main Generator Script Structure

```powershell
# Generate-CollegeData.ps1

param(
    [Parameter(Mandatory=$true)]
    [ValidateSet("Small", "Medium", "Large")]
    [string]$Profile,

    [Parameter(Mandatory=$false)]
    [int]$BatchSize = 100,

    [Parameter(Mandatory=$false)]
    [string]$DatabasePath = "C:\temp\step2-test\College"
)

# Import step2.ps1 functions
. "$PSScriptRoot\step2.ps1"

# Define size profiles
$profiles = @{
    Small = @{Depts=10; Teachers=50; Students=500; Courses=100; Classes=200}
    Medium = @{Depts=20; Teachers=200; Students=5000; Courses=500; Classes=1000}
    Large = @{Depts=50; Teachers=1000; Students=50000; Courses=2000; Classes=5000}
}

$config = $profiles[$Profile]
Write-Host "Generating $Profile profile: $($config.Students) students" -ForegroundColor Cyan

# Initialize caches
$script:deptCache = @{}
$script:teacherCache = @{}
$script:studentCache = @{}
$script:courseCache = @{}
$script:classCache = @{}

# Results tracking
$results = @{}

# === LEVEL 1: Departments ===
Write-Host "`nGenerating Departments..." -ForegroundColor Yellow
$depts = @()
for ($i = 0; $i -lt $config.Depts; $i++) {
    $template = $script:deptData[$i % $script:deptData.Count]
    $depts += New-Department -index $i -template $template
}

Write-Host "Loading Departments..." -ForegroundColor Yellow
$results["Departments"] = Load-DataBatch -tableName "Departments" -records $depts -batchSize $BatchSize
Build-DepartmentCache -departments $depts -recordIDs $results["Departments"].RecordIDs

# === LEVEL 2: Teachers ===
Write-Host "`nGenerating Teachers..." -ForegroundColor Yellow
$teachers = @()
for ($i = 1; $i -le $config.Teachers; $i++) {
    $teachers += New-Teacher -index $i -departments $depts
}

Write-Host "Loading Teachers..." -ForegroundColor Yellow
$results["Teachers"] = Load-DataBatch -tableName "Teachers" -records $teachers -batchSize $BatchSize
Build-TeacherCache -teachers $teachers -recordIDs $results["Teachers"].RecordIDs

# === LEVEL 3: Students, Classes, etc. ===
# ... similar pattern for remaining tables ...

# === LEVEL 5: Set Relationships ===
Write-Host "`nAssigning Set Relationships..." -ForegroundColor Yellow
$setResults = Assign-SetRelationships

# === Generate Report ===
Generate-PerformanceReport -loadResults $results -dbPath $DatabasePath -setResults $setResults
```

## Performance Metrics

### Expected Rates
- **Baseline**: 200-500 records/second over REST API
- **Small profile**: ~3-5 seconds total load time
- **Medium profile**: ~30-60 seconds total load time
- **Large profile**: ~5-10 minutes total load time

### Measurements to Collect
1. **Per-table metrics**:
   - Total records loaded
   - Failed records
   - Duration (seconds)
   - Rate (records/second)

2. **Database metrics**:
   - Total database size (MB)
   - Per-table file sizes
   - Index file sizes
   - Dictionary sizes

3. **Set relationship metrics**:
   - Total relationships created
   - Duration
   - Success/failure rates

## Implementation Plan

### Phase 1: Core Infrastructure (Day 1)
1. Create `Generate-CollegeData.ps1` script
2. Implement predefined data arrays
3. Implement key generation functions (padding helpers)
4. Implement Load-DataBatch function
5. Test with Small profile (Departments + Teachers only)

### Phase 2: Full Generation (Day 2)
1. Implement all generator functions (7 tables)
2. Implement cache building functions
3. Implement dependency-ordered loading
4. Test complete Small profile
5. Verify record counts and FK integrity

### Phase 3: Set Relationships (Day 3)
1. Implement Assign-SetRelationships function
2. Test Teacher.Teaches assignments
3. Test Student.TakesClasses assignments
4. Verify set member retrieval
5. Test complete workflow end-to-end

### Phase 4: Performance Testing (Day 4)
1. Test Medium profile
2. Test Large profile
3. Compare batch sizes (50, 100, 200, 500)
4. Analyze performance bottlenecks
5. Generate comparison reports

### Phase 5: Analysis & Optimization (Day 5)
1. Identify slow operations
2. Test parallel loading (if supported)
3. Optimize CHAR padding performance
4. Document findings
5. Create performance baseline documentation

## Testing Checklist

- [ ] Small profile generates successfully
- [ ] All FK relationships valid
- [ ] Primary key lookups work
- [ ] Set relationships created correctly
- [ ] Performance metrics collected
- [ ] Medium profile tested
- [ ] Large profile tested
- [ ] Database size measurements collected
- [ ] Rate comparisons documented
- [ ] Error handling tested (invalid records)

## Success Criteria

1. **Correctness**: All records load without validation errors
2. **Referential Integrity**: All FK relationships valid
3. **Performance**: Achieve 200+ records/second baseline
4. **Scalability**: Large profile completes in <15 minutes
5. **Metrics**: Complete performance report generated
6. **Reproducibility**: Script runs consistently across profiles

## Next Steps (Tomorrow)

1. Start with Phase 1 implementation
2. Create `Generate-CollegeData.ps1` in `scripts/` directory
3. Test Small profile with Departments and Teachers only
4. Verify caching mechanism works correctly
5. Expand to full Small profile (all 7 tables)
6. Run complete test and collect initial metrics

## Notes

- CHAR field padding is crucial - implement helper function early
- Cache lookups are essential for performance with large datasets
- Progress bars provide good UX for long-running loads
- Error collection important for debugging at scale
- Consider implementing dry-run mode for testing generation logic
