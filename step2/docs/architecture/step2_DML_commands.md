# Data Manipulation Language (DML) Commands

## Database Management Commands

### CreateDB

**Description**:
Creates a new subdirector with the same name as the schema. The operation then creates a DB object from the schema, serializes it to a read-only `schema.json` file, and stores it in the created subdirectory. Then it creates files and subdirectories described in the Overview.

The `dirPath` is fully specified (absolute) path to the directory in which the new database director is created.

**Input Parameters**:
```json
{
  "dirPath": string,
  "schemaPath": string
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...]
}
```

### OpenDB

**Description**:
Reads the `schema.json` file, validates DB directories and files against the schema, and creates the DB singleton object for subsequent operations.

The `dirPath` is fully specified (absolute) path to the directory of the database.

**Input Parameters**:
```json
{
  "dirPath": string
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...]
}
```

### CloseDB

**Description**:
Closes the opened database by setting the DB record pointer to null, releasing resources, and ensuring all pending operations are completed.

**Input Parameters**:
None (closes the current database)

**Response**:
```json
{
  "status": string,
  "errors": [string, ...]
}
```
### GetSchema

**Description**
Get the DB schema of the currently opened DB in the form of a JSON object.

**Input Parameters**:
None (closes the current database)

**Response**:
```json
{
  "status": string,
  "schema": {schema_object}
}
```
The `schema_object` is a JSON object corresponding to the content of the DB .ddl definition. The command is mainly used for discovery and scripting.

### GetTableStats

**Description**:
Gests information about size of table(s) in the currently open DB. For each selected table it returns the table stats in the form the following JSON object:
``` json
{"allocated_records": int, "deleted_list_length": int, "dictionaries": [{"<string_prop_name>": string, "number_of_strings": int}, ... ]}
```
The `allocated_records` tells us how many records have been allocated, including the deleted records. The `deleted_list_length` tells us how many reusable record spaces are available. The number of active records is therefore `allocated_records - deleted_list_length`.

**Input Parameters**:
```json
{
  "tables": [string, ...]
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "tables": [{table_stats}, ...]
}
```

## Record Commands

### AddRecord

**Description**:
Creates a new record in the given table. The operation will fail if a record with the same primary key already exists. If table `records.dat` file has deleted/reusable record spaces, the first such space will be reused. This means that new records is not necessarily placed at the end of the records file and will have arbitrary recordID.

**Input Parameters**:
```json
{
  "tableName": string,
  "record": { record_object }
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "recordID": uint32
}
```

### UpdateRecord

**Description**:
Updates an existing record in a table identified by its recordID. The input record object does not need to be complete, but must include the primary key property, which cannot be changed. Only the specified fields will be updated. The reason the commands takes recordID and not primary key, is that it is possible to define tables without primary keys and it is possible to read records directly bases on their positions in `records.dat` file.

**Input Parameters**:
```json
{
  "tableName": string,
  "recordID": uint32,
  "record": { record_object }
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...]
}
```

### DeleteRecord

**Description**:
Deletes a record in a table identified by its recordID. If a record is an owner of set(s), the set(s) is (are) also deleted.

**Input Parameters**:
```json
{
  "tableName": string,
  "recordID": uint32
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...]
}
```

### GetRecordID

**Description**
This function maps records primary keys to their record IDs, and can be used as the first step in updating a record.
**Input Parameters**:
```json
{
  "tableName": string,
  "primeKey": int || string
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "recordID": uint32
}
```
There are multiple ways of retrieving/reding records from a table, bases on
- record ID
- primary key
- value of a string property
- beginning substring of a string property, and
- set membership (described in Set Commands section)

### GetRecordByID
**Purpose**: Reads a record directly using its recordID.

**Description**:
Reads a record directly based on its record ID. The only validation performed is that the recordID points to an active (not deleted) record. This command can be used to read the first record in record file by specifying recordID = 0.

**Input Parameters**:
```json
{
  "tableName" : string,
  "recordID": uint32
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "tableName" : string,
  "record": { record_object }
}
```

### GetNextRecord
**Purpose**: Reads the next record in sequential order within a records file.

**Description**:
Reads the next, non-deleted record following the given recordID. The command can be used for sequential traversal of active (non-deleted) records.

**Input Parameters**:
```json
{
  "tableName" : string,
  "recordID": uint32
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "record": { record_object },
  "tableName": string,
  "recordID": uint32
}
```

### GetRecordByKey

**Description**:
Retrieves an record using its table name and primary key value (SMALLINT, INT, BIGINT, CHAR[4 - 32]).

**Input Parameters**:
```json
{
  "tableName": string,
  "primeKey": int || string
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "record": { record_object },
  "recordID": int || string
}
```

### GetRecordsByString

**Description**:
Retrieves records IDs of one or more record that have a string property value equal to the given string

**Input Parameters**:
```json
{
  "tableName": string,
  "propertyName": string,
  "propertyValue": string
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "recordIDs": [uint32, ...]
}
```

### GetRecordsBySubstring

**Description**:
Retrieves records IDs of one or more record that have a string property value starting with the given substring. STEP2 limits the length of the substring to 8 characters. If the substring given in the commands is longer than 8 characters, it will be truncated. If the substring is less than 8 characters, it will be padded with zeros.

**Input Parameters**:
```json
{
  "tableName": string,
  "propertyName": string,
  "substring": string
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "recordIDs": [uint32, ...]
}
```

## Set Commands

### AddSetMember

**Description**:
The command adds the recordID of the named set of the set-owner record. The command verifies that the owner exists in the given table, the member exists in its table (implied by set definition), and that the member is not already in the owner's set.

**Input Parameters**:
```json
{
  "tableName": string,
  "setName" : string,
  "ownerRecordID": uint32,
  "memberRecordID": uint32
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
}
```

### GetSetMembers

**Description**:
Retrieves record IDs of all members of owner's records named set.

**Input Parameters**:
```json
{
  "tableName": string,
  "setName" : string,
  "ownerRecordID": uint32,
}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
  "memberIDs": [uint32, ...]
}
```

### RemoveSetMember

**Description**:
Removes set member with the given member ID from the set of the given owner in the given table.

**Input Parameters**:
```json
{
  "ownerTableName": string,
  "ownerRecordID": uint32,
  "setName": string,
  "memberRecordID": uint32}
```

**Response**:
```json
{
  "status": string,
  "errors": [string, ...],
}
```

If the command removes the last member from the set, it makes the set empty (stored as NoSet)

## Batch Command

**Description**:
The command groups together two or more state changing commands `add`, `update`, `delete`, `addSetMember`, `removeSetMember`. The command are either all executed, or if one of the fails mid way, the already executed commands are undone.
The URL endpoint of the command is `POST /step2/batch`

**Input JSON:**
```json
{
  "commands": [
    {"add": {"tableName": string, "record": { record_object }}},
    {"update": {"tableName": string, "recordID": uint32, "record": { record_object }}},
    {"delete": {"tableName": string, "recordID": uint32}},
    {"addSetMember": {"ownerTableName": string, "ownerRecordID": uint32, "setName": string, "memberRecordID": uint32}},
    {"removeSetMember": {"ownerTableName": string, "ownerRecordID": uint32, "setName": string, "memberRecordID": uint32}}
  ]
}
```

**Output JSON (all succeeded):**
```json
{
  "status": "success",
  "results": [
    {"index": 0, "command": "add", "status": "success", "recordID": uint32},
    {"index": 1, "command": "update", "status": "success"},
    {"index": 2, "command": "delete", "status": "success"},
    {"index": 3, "command": "addSetMember", "status": "success"},
    {"index": 4, "command": "removeSetMember", "status": "success"}
  ]
}
```

If the batch fails, the output indicates which command failed and which where undone.

**Output JSON (failure with undo):**
```json
{
  "status": "error",
  "failedAtIndex": 2,
  "failedCommand": "delete",
  "failedError": "record not found",
  "results": [
    {"index": 0, "command": "add", "status": "undone"},
    {"index": 1, "command": "update", "status": "undone"}
  ],
  "undoErrors": []
}
```

Undo does not guarantee that the un-deleted record will have the same recordID as the deleted original.

### Error Handling

All DML command return `status` with should be either "success" or "error". It is possible that in the future
some commands may return some other status value.

All DML commands implement error handling:

- **Validation Errors**: Invalid input parameters or data
- **Constraint Violations**: Referential integrity violations
- **Not Found Errors**: Requested records do not exist
- **Duplicate Key Errors**: Attempt to create records with existing keys
- **I/O Errors**: File system or storage-related failures

