# This module provides PowerShell functions to interact with the STEP2 REST server.

# Base URL for the STEP2 REST API server
$script:Step2ServerUrl = "http://localhost:8080"

<# ========================================== SERVER Management ========================================== #>

<#
.SYNOPSIS
    Start-SRV starts the STEP2 REST API server.
.DESCRIPTION
    Starts the STEP2 REST API server in a separate process.
.PARAMETER Step2ExePath
    Full path to the step2.exe executable.
.PARAMETER Port
    The port number for the server (default: 8080).
.EXAMPLE
    Start-SRV -Step2ExePath ".\step2.exe" -Port 8080
#>
function Start-SRV {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$Step2ExePath,

        [Parameter(Mandatory=$false)]
        [int]$Port = 8080
    )

    # Check if server is already running
    $existing = Get-Process -Name "step2" -ErrorAction SilentlyContinue
    if ($existing) {
        Write-Host "STEP2 server is already running (PID: $($existing.Id))" -ForegroundColor Yellow
        return
    }

    # Update the server URL if custom port is specified
    if ($Port -ne 8080) {
        $script:Step2ServerUrl = "http://localhost:$Port"
    }

    # Start the server
    try {
        Start-Process -FilePath $Step2ExePath -ArgumentList "server", "-port", $Port -WindowStyle Normal
        Start-Sleep -Seconds 2
        Write-Host "STEP2 server started on port $Port" -ForegroundColor Green
    }
    catch {
        Write-Host "Failed to start STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Stop-SRV stops the STEP2 REST API server.
.DESCRIPTION
    Stops all running STEP2 REST API server processes.
.EXAMPLE
    Stop-SRV
#>
function Stop-SRV {
    [CmdletBinding()]
    param()

    $processes = Get-Process -Name "step2" -ErrorAction SilentlyContinue

    if (-not $processes) {
        Write-Host "No STEP2 server processes found" -ForegroundColor Yellow
        return
    }

    foreach ($proc in $processes) {
        try {
            Stop-Process -Id $proc.Id -Force
            Write-Host "Stopped STEP2 server (PID: $($proc.Id))" -ForegroundColor Green
        }
        catch {
            Write-Host "Failed to stop STEP2 server (PID: $($proc.Id)): $_" -ForegroundColor Red
        }
    }
}

<#
.SYNOPSIS
    Get-SRVStatus gets the status of the STEP2 REST API server.
.DESCRIPTION
    Checks if the STEP2 REST API server is running and responds to requests.
.EXAMPLE
    Get-SRVStatus
#>
function Get-SRVStatus {
    [CmdletBinding()]
    param()

    # Check if process is running
    $process = Get-Process -Name "step2" -ErrorAction SilentlyContinue

    if (-not $process) {
        Write-Host "STEP2 server is not running" -ForegroundColor Red
        return @{ Running = $false }
    }

    # Check if server responds
    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/" -Method Get -TimeoutSec 2
        Write-Host "STEP2 server is running and responding (PID: $($process.Id))" -ForegroundColor Green
        return @{ Running = $true; PID = $process.Id; Responding = $true }
    }
    catch {
        Write-Host "STEP2 server is running but not responding (PID: $($process.Id))" -ForegroundColor Yellow
        return @{ Running = $true; PID = $process.Id; Responding = $false }
    }
}

<# ========================================== DATABASE Operations ========================================== #>

<#
.SYNOPSIS
    Create-DB creates a new STEP2 database.
.DESCRIPTION
    Creates a new STEP2 database based on a DDL schema file in the specified directory.
.PARAMETER SchemaPath
    Full path to the DDL schema file.
.PARAMETER DirectoryPath
    Full path to the directory where the database will be created.
.EXAMPLE
    Create-DB -SchemaPath ".\docs\testdata\College.ddl" -DirectoryPath ".\mydb"
#>
function Create-DB {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$SchemaPath,

        [Parameter(Mandatory=$true)]
        [string]$DirectoryPath
    )

    $body = @{
        schemaPath = $SchemaPath
        dirPath = $DirectoryPath
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/db/create" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Database created successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error creating database: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Open-DB opens a STEP2 database.
.DESCRIPTION
    Opens an existing STEP2 database from the specified directory path.
.PARAMETER DirectoryPath
    Full path to the directory containing the database.
.EXAMPLE
    Open-DB -DirectoryPath ".\mydb\College"
#>
function Open-DB {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$DirectoryPath
    )

    $body = @{
        dirPath = $DirectoryPath
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/db/open" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Database opened successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error opening database: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Close-DB closes the currently opened STEP2 database.
.DESCRIPTION
    Closes the currently opened STEP2 database on the server.
.EXAMPLE
    Close-DB
#>
function Close-DB {
    [CmdletBinding()]
    param()

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/db/close" `
                                       -Method Post `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Database closed successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error closing database: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<# ========================================== DATABASE INFO Operations ========================================== #>

<#
.SYNOPSIS
    Get-Schema retrieves the schema of the currently opened database.
.DESCRIPTION
    Retrieves the complete database schema including tables, fields, constraints, and sets.
.EXAMPLE
    $schema = Get-Schema
    $schema.schema.tables | ForEach-Object { Write-Host $_.name }
#>
function Get-Schema {
    [CmdletBinding()]
    param()

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/db/info/schema" `
                                       -Method Get `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Schema retrieved successfully" -ForegroundColor Green
            Write-Host "Database: $($response.schema.name)" -ForegroundColor Cyan
            Write-Host "Tables: $($response.schema.tables.Count)" -ForegroundColor Cyan
            return $response
        } else {
            Write-Host "Error retrieving schema: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-TableStats retrieves statistics for one or more tables in the currently opened database.
.DESCRIPTION
    Retrieves statistics including allocated records, deleted records, and dictionary sizes
    for the specified tables.
.PARAMETER TableNames
    Array of table names to retrieve statistics for.
.EXAMPLE
    $stats = Get-TableStats -TableNames @("Students", "Teachers")
    $stats.tables | ForEach-Object {
        $active = $_.allocated_records - $_.deleted_list_length
        Write-Host "$($_.name): $active active records"
    }
.EXAMPLE
    # Get stats for all tables
    $schema = Get-Schema
    $allTables = $schema.schema.tables | ForEach-Object { $_.name }
    $stats = Get-TableStats -TableNames $allTables
#>
function Get-TableStats {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string[]]$TableNames
    )

    $body = @{
        tables = $TableNames
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/db/info/tables" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success" -or ($response.tables -and $response.tables.Count -gt 0)) {
            if ($response.errors -and $response.errors.Count -gt 0) {
                Write-Host "Warning: $($response.errors -join ', ')" -ForegroundColor Yellow
            }
            Write-Host "Retrieved statistics for $($response.tables.Count) table(s)" -ForegroundColor Green

            # Display summary
            foreach ($table in $response.tables) {
                $active = $table.allocated_records - $table.deleted_list_length
                $dictCount = if ($table.dictionaries) { $table.dictionaries.Count } else { 0 }
                Write-Host "  $($table.name): $active active records ($($table.allocated_records) allocated, $($table.deleted_list_length) deleted), $dictCount dictionaries" -ForegroundColor Cyan
            }

            return $response
        } else {
            Write-Host "Error retrieving table statistics: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<# ========================================== RECORD Operations ========================================== #>

<#
.SYNOPSIS
    Add-Record adds a new record to a table in the database.
.DESCRIPTION
    Adds a new record to the specified table and returns the assigned recordID.
.PARAMETER TableName
    The name of the table.
.PARAMETER Record
    A hashtable containing the field values for the record.
.EXAMPLE
    $student = @{
        First_name = "John"
        Last_name = "Doe"
        Student_id = "S001"
        Major = "CS"
        Year = 3
        Credits = 90
    }
    $result = Add-Record -TableName "Students" -Record $student
    Write-Host "Record ID: $($result.recordID)"
#>
function Add-Record {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [hashtable]$Record
    )

    $body = @{
        tableName = $TableName
        record = $Record
    } | ConvertTo-Json -Depth 10

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/add" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Record added successfully (ID: $($response.recordID))" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error adding record: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Update-Record updates an existing record in the database.
.DESCRIPTION
    Updates an existing record identified by its recordID. Only the fields provided
    in the Record parameter will be updated.
.PARAMETER TableName
    The name of the table.
.PARAMETER RecordID
    The internal record ID.
.PARAMETER Record
    A hashtable containing the fields to update. Does not need to be complete.
.EXAMPLE
    $updates = @{
        Year = 4
        Credits = 120
    }
    Update-Record -TableName "Students" -RecordID 1 -Record $updates
#>
function Update-Record {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [int]$RecordID,

        [Parameter(Mandatory=$true)]
        [hashtable]$Record
    )

    $body = @{
        tableName = $TableName
        recordID = $RecordID
        record = $Record
    } | ConvertTo-Json -Depth 10

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/update" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Record updated successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error updating record: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Delete-Record deletes a record from the database.
.DESCRIPTION
    Deletes a record identified by its recordID from the specified table.
.PARAMETER TableName
    The name of the table.
.PARAMETER RecordID
    The internal record ID.
.EXAMPLE
    Delete-Record -TableName "Students" -RecordID 1
#>
function Delete-Record {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [int]$RecordID
    )

    $body = @{
        tableName = $TableName
        recordID = $RecordID
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/delete" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Record deleted successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error deleting record: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<# ========================================== RECORD GET Operations ========================================== #>

<#
.SYNOPSIS
    Get-RecordByID retrieves a record by its internal record ID.
.DESCRIPTION
    Retrieves a record from the specified table using its internal record ID.
.PARAMETER TableName
    The name of the table.
.PARAMETER RecordID
    The internal record ID.
.EXAMPLE
    $record = Get-RecordByID -TableName "Students" -RecordID 1
#>
function Get-RecordByID {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [int]$RecordID
    )

    $body = @{
        tableName = $TableName
        recordID = $RecordID
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/get/byid" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Record retrieved successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error retrieving record: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-RecordID retrieves the internal record ID for a primary key value.
.DESCRIPTION
    Looks up the internal record ID using the primary key value.
.PARAMETER TableName
    The name of the table.
.PARAMETER PrimeKey
    The primary key value (type depends on the table's primary key definition).
.EXAMPLE
    $result = Get-RecordID -TableName "Students" -PrimeKey "S001"
    Write-Host "Record ID: $($result.recordID)"
#>
function Get-RecordID {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        $PrimeKey
    )

    $body = @{
        tableName = $TableName
        primeKey = $PrimeKey
    } | ConvertTo-Json -Depth 10

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/get/id" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Record ID retrieved successfully: $($response.recordID)" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error retrieving record ID: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-NextRecord retrieves the next record in sequential order.
.DESCRIPTION
    Retrieves the next record after the specified record ID in the table's sequential order.
.PARAMETER TableName
    The name of the table.
.PARAMETER CurrentRecordID
    The current record ID.
.EXAMPLE
    $next = Get-NextRecord -TableName "Students" -CurrentRecordID 1
#>
function Get-NextRecord {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [int]$CurrentRecordID
    )

    $body = @{
        tableName = $TableName
        currentRecordID = $CurrentRecordID
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/get/next" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Next record retrieved successfully (ID: $($response.nextRecordID))" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error retrieving next record: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-RecordByKey retrieves a record by its primary key value.
.DESCRIPTION
    Convenience function that combines Get-RecordID and Get-RecordByID to retrieve
    a record directly using its primary key value.
.PARAMETER TableName
    The name of the table.
.PARAMETER PrimeKey
    The primary key value.
.EXAMPLE
    $student = Get-RecordByKey -TableName "Students" -PrimeKey "S001"
#>
function Get-RecordByKey {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        $PrimeKey
    )

    $body = @{
        tableName = $TableName
        primeKey = $PrimeKey
    } | ConvertTo-Json -Depth 10

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/get/bykey" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Record retrieved successfully by key" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error retrieving record by key: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-RecordsByString searches for records with an exact string match.
.DESCRIPTION
    Performs an exact match search on a string property and returns matching record IDs.
.PARAMETER TableName
    The name of the table.
.PARAMETER PropertyName
    The name of the property to search.
.PARAMETER PropertyValue
    The exact value to match.
.EXAMPLE
    $results = Get-RecordsByString -TableName "Students" -PropertyName "Last_name" -PropertyValue "Doe"
#>
function Get-RecordsByString {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [string]$PropertyName,

        [Parameter(Mandatory=$true)]
        [string]$PropertyValue
    )

    $body = @{
        tableName = $TableName
        propertyName = $PropertyName
        propertyValue = $PropertyValue
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/get/bystring" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            $count = if ($response.recordIDs) { $response.recordIDs.Count } else { 0 }
            Write-Host "Found $count matching records" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error searching records: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-RecordsBySubstring searches for records with a prefix match.
.DESCRIPTION
    Performs a prefix search on a string property and returns matching record IDs.
.PARAMETER TableName
    The name of the table.
.PARAMETER PropertyName
    The name of the property to search.
.PARAMETER Substring
    The substring prefix to match.
.EXAMPLE
    $results = Get-RecordsBySubstring -TableName "Students" -PropertyName "Last_name" -Substring "Do"
#>
function Get-RecordsBySubstring {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$TableName,

        [Parameter(Mandatory=$true)]
        [string]$PropertyName,

        [Parameter(Mandatory=$true)]
        [string]$Substring
    )

    $body = @{
        tableName = $TableName
        propertyName = $PropertyName
        substring = $Substring
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/record/get/bysubstring" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            $count = if ($response.recordIDs) { $response.recordIDs.Count } else { 0 }
            Write-Host "Found $count matching records" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error searching records: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<# ========================================== SET Operations ========================================== #>

<#
.SYNOPSIS
    Add-SetMember adds a member record to an owner's set.
.DESCRIPTION
    Adds a member record to a set owned by the specified owner record.
.PARAMETER OwnerTableName
    The name of the owner table.
.PARAMETER OwnerRecordID
    The internal record ID of the owner.
.PARAMETER SetName
    The name of the set.
.PARAMETER MemberRecordID
    The internal record ID of the member to add.
.EXAMPLE
    Add-SetMember -OwnerTableName "Teachers" -OwnerRecordID 1 -SetName "Teaches" -MemberRecordID 5
#>
function Add-SetMember {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$OwnerTableName,

        [Parameter(Mandatory=$true)]
        [int]$OwnerRecordID,

        [Parameter(Mandatory=$true)]
        [string]$SetName,

        [Parameter(Mandatory=$true)]
        [int]$MemberRecordID
    )

    $body = @{
        ownerTableName = $OwnerTableName
        ownerRecordID = $OwnerRecordID
        setName = $SetName
        memberRecordID = $MemberRecordID
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/set/addmember" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Member added to set successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error adding member to set: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Get-SetMembers retrieves all members of an owner's set.
.DESCRIPTION
    Retrieves all member record IDs in a set owned by the specified owner record.
.PARAMETER OwnerTableName
    The name of the owner table.
.PARAMETER OwnerRecordID
    The internal record ID of the owner.
.PARAMETER SetName
    The name of the set.
.EXAMPLE
    $members = Get-SetMembers -OwnerTableName "Teachers" -OwnerRecordID 1 -SetName "Teaches"
#>
function Get-SetMembers {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$OwnerTableName,

        [Parameter(Mandatory=$true)]
        [int]$OwnerRecordID,

        [Parameter(Mandatory=$true)]
        [string]$SetName
    )

    $body = @{
        ownerTableName = $OwnerTableName
        ownerRecordID = $OwnerRecordID
        setName = $SetName
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/set/getmembers" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            $count = if ($response.members) { $response.members.Count } else { 0 }
            Write-Host "Retrieved $count set members" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error retrieving set members: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Remove-SetMember removes a member from an owner's set.
.DESCRIPTION
    Removes a member record from a set owned by the specified owner record.
.PARAMETER OwnerTableName
    The name of the owner table.
.PARAMETER OwnerRecordID
    The internal record ID of the owner.
.PARAMETER SetName
    The name of the set.
.PARAMETER MemberRecordID
    The internal record ID of the member to remove.
.EXAMPLE
    Remove-SetMember -OwnerTableName "Teachers" -OwnerRecordID 1 -SetName "Teaches" -MemberRecordID 5
#>
function Remove-SetMember {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$OwnerTableName,

        [Parameter(Mandatory=$true)]
        [int]$OwnerRecordID,

        [Parameter(Mandatory=$true)]
        [string]$SetName,

        [Parameter(Mandatory=$true)]
        [int]$MemberRecordID
    )

    $body = @{
        ownerTableName = $OwnerTableName
        ownerRecordID = $OwnerRecordID
        setName = $SetName
        memberRecordID = $MemberRecordID
    } | ConvertTo-Json

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/set/removemember" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            Write-Host "Member removed from set successfully" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Error removing member from set: $($response.errors -join ', ')" -ForegroundColor Red
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<# ========================================== BATCH Operations ========================================== #>

<#
.SYNOPSIS
    Process-Batch executes a batch of mutating DML commands atomically.
.DESCRIPTION
    Executes a sequence of mutating commands (add, update, delete, addSetMember, removeSetMember)
    as a single batch. If any command fails, all previously succeeded commands are undone
    in reverse order (best-effort).
.PARAMETER Commands
    An array of hashtables, each with a single key (the command name) whose value is a hashtable
    of command parameters.
.EXAMPLE
    # Add two departments in a batch
    $commands = @(
        @{ add = @{ tableName = "Departments"; record = @{ Name = "Physics"; Department_code = "PHYSICSD"; Building_name = "Science Hall"; Building_code = "SCHL0001" } } },
        @{ add = @{ tableName = "Departments"; record = @{ Name = "Chemistry"; Department_code = "CHEMISTR"; Building_name = "Science Hall"; Building_code = "SCHL0002" } } }
    )
    $result = Process-Batch -Commands $commands
.EXAMPLE
    # Add a record and connect it to a set
    $commands = @(
        @{ add = @{ tableName = "Classes"; record = @{ Class_name = "Physics 101"; Class_id = "PHY101F26"; Semester = "Fall 2026"; Building_name = "Science Hall"; Room = "101" } } },
        @{ addSetMember = @{ ownerTableName = "Teachers"; ownerRecordID = 1; setName = "Teaches"; memberRecordID = 50 } }
    )
    Process-Batch -Commands $commands
#>
function Process-Batch {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [hashtable[]]$Commands
    )

    $body = @{
        commands = $Commands
    } | ConvertTo-Json -Depth 10

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/batch" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            $count = if ($response.results) { $response.results.Count } else { 0 }
            Write-Host "Batch executed successfully ($count commands)" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Batch failed at command $($response.failedAtIndex) ($($response.failedCommand)): $($response.failedError)" -ForegroundColor Red
            if ($response.undoErrors -and $response.undoErrors.Count -gt 0) {
                Write-Host "Undo errors:" -ForegroundColor Yellow
                foreach ($undoErr in $response.undoErrors) {
                    Write-Host "  $undoErr" -ForegroundColor Yellow
                }
            }
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}

<#
.SYNOPSIS
    Process-BatchFile executes a batch of mutating DML commands from a JSON file.
.DESCRIPTION
    Reads a JSON file containing a batch of mutating commands and executes them atomically.
    The JSON file should have the same format as the batch endpoint input:
    { "commands": [ {"add": {...}}, {"update": {...}}, ... ] }
.PARAMETER FilePath
    Full path to the JSON file containing the batch commands.
.EXAMPLE
    # Given a file enroll_student.json:
    # {
    #   "commands": [
    #     {"add": {"tableName": "Students", "record": {"First_name": "Jane", "Last_name": "Smith", "Student_id": "NIP2609001", "Major": "COMPSCIE", "Year": 1, "Credits": 0}}},
    #     {"addSetMember": {"ownerTableName": "Classes", "ownerRecordID": 3, "setName": "Enrollment", "memberRecordID": 301}}
    #   ]
    # }
    Process-BatchFile -FilePath ".\enroll_student.json"
#>
function Process-BatchFile {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory=$true)]
        [string]$FilePath
    )

    if (-not (Test-Path $FilePath)) {
        Write-Host "File not found: $FilePath" -ForegroundColor Red
        return
    }

    $body = Get-Content -Path $FilePath -Raw

    # Validate it's valid JSON before sending
    try {
        $null = $body | ConvertFrom-Json
    }
    catch {
        Write-Host "Invalid JSON in file: $_" -ForegroundColor Red
        return
    }

    try {
        $response = Invoke-RestMethod -Uri "$script:Step2ServerUrl/step2/batch" `
                                       -Method Post `
                                       -Body $body `
                                       -ContentType "application/json"

        if ($response.status -eq "success") {
            $count = if ($response.results) { $response.results.Count } else { 0 }
            Write-Host "Batch executed successfully ($count commands)" -ForegroundColor Green
            return $response
        } else {
            Write-Host "Batch failed at command $($response.failedAtIndex) ($($response.failedCommand)): $($response.failedError)" -ForegroundColor Red
            if ($response.undoErrors -and $response.undoErrors.Count -gt 0) {
                Write-Host "Undo errors:" -ForegroundColor Yellow
                foreach ($undoErr in $response.undoErrors) {
                    Write-Host "  $undoErr" -ForegroundColor Yellow
                }
            }
            return $response
        }
    }
    catch {
        Write-Host "Failed to connect to STEP2 server: $_" -ForegroundColor Red
        throw
    }
}
