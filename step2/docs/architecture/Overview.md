# Overview of the STEP2 DBMS Design

STEP2 is a successor (and evolution) of STEP, which was a learning exercise of implementing an early CODASYL DBMS database system in Go. STEP was originally designed and implemented in early 1980-ties at the `Institute of Organization and Management` at the `Technical University of Wroclaw` for mini computer MERA 400, which had an architecture and assembler similar to the PDP.

STEP2 removes many of the STEP limitations, historically imposed by the constrains of MERA 400 storage and programming environments, and implements a more complete CODASYL data model, but keeps the concept of SETS, which are described in the following sections. However, and most importantly, it is still an learning exercise of implementing a complete, although functionally limited, DBMS in Go.

## DB Schema

STEP2 DDL is defined in `step2\docs\architecture\step2_DDL_grammar.md`. STEP2 DB is a collection of Tables. A Table is a collection of records (or rows), which contain fields (or columns) defined in their Table definition. A Field can be of one of the following types
- SMALLINT - stored as int16
- INT - stored as int32
- BIGINT - stored as int64
- DECIMAL - stored as three unsigned integers (int part, fraction part, scale) and a boolean (true if value is negative)
- FLOAT - stored is float64
- STRING - an arbitrary string, which is truncated if definition contains size constraint, and stored as a reference to a dictionary
- CHAR[S] - a character array of the given size S
- BOOLEAN - stored as uint8
- DATE - stored as uint64 representing number of days from a reference date of 2000-01-01 (Postgres Epoch)
- TIME - stored as uint64 representing number of milliseconds from midnight.

If a Field is a primary key or a foreign key, this is described by Column Constraints.

A Table definition may also contain one or more definitions of Sets. A set is a mechanism to implement light-weight relationship between a record in one table and 1-to-N records in another table. Records in a set are note identified by their primary keys, but by their record identifiers (db keys) described in Storage Model section below.

## Input Data

Records stored in their respective tables are input into STEP2 in the form of JSON objects with properties, which should correspond to record fields. So for the table Students

TABLE Students (
    First_name          STRING(20),
    Last_name           STRING(30),
    Preferred_name      STRING(20) OPTIONAL,
    Gender              CHAR[1],
    Start_date          DATE,
    Student_id          CHAR[10] PRIMARY KEY,
    Major               CHAR[25],
    Advisor             CHAR[8] FOREIGN KEY Teachers,
    Year                SMALLINT,
    Credits             SMALLINT
)
SETS (
    TakesClasses        Classes,
    CompletedClasses    Grades
)

an input record may look like this

``` JSON
{
    "table": "Students",
    "record": {
        "First_name": "Aisha",
        "Last_name": "Khan",
        "Student_id": "Y21-045601",
        "Gender": "F",
        "Major": "Computer Science",
        "Year": 2,
        "Credits": 45
    }
}
```

- Fields in the input JSON record can be in any order. If a field is defined as OPTIONAL, it can be omitted and will be flagged in storage as no-value and will be omitted on output.

- Input STRING fields longer than the max size, e.g. Last_name above the above limit to 30 characters, will be truncated.

- Input CHAR[] arrays shorter than the defined size will be padded with zeros. Input CHAR[] arrays longer than the defined size will be flagged as errors.

- Input DATE fields should be strings of the format "YYYY-MM-DD"

- Input TIME fields should be strings of the format "HH:MM or HH:MM:SS or HH:MM:SS.mmm" where the hour part is in the 24-hour clock system where, for example, 6pm is given as 18 (6 + 12).

- Input DECIMAL numbers should be represented as stings with three parts: sign, integer part, and fraction part e.g. "-1285.00876". If sign is omitted, it is assumed to be +.

## Storage Model

### Record Representation and Storage

All STEP2 record are serialized to fixed-length byte slices with two parts (1) the header, and (2) the body. The body is a sequence of filed values serialized in the following way

- SMALINT, INT, BIGINT and FLOAT values are serialized into 2, 4, 8 and 8 byte slices respectively.
- DECIMAL vales are first converted into Decimal structure
``` Go
type Decimal struct {
	IntPart  uint64 // digits before decimal point
	FracPart uint64 // digits after decimal point (no leading zeros)
	Scale    uint8  // number of digits in FracPart including leading zeros
	Neg      bool   // true = negative
}
```
and then serialized as 8+8+2+1 bytes sequence, where the last byte represents boolean (`0x00` for false, `0x01` for true, false → []byte{0} and true → []byte{1})
- CHAR[] arrays are stored as byte slices of the array size. This implies that CHAR[] arrays should only contain ASCII characters.
- STRING values are stored as uint32 string IDs in the corresponding dictionaries (there is a dictionary for every STRING property)
- BOOLEAN values are stored as a single byte (`0x00` for false, `0x01` for true, false)
- DATE values are stored as 8 bytes representing int64, which is the number of days from the reference date 2000-01-01, which is know as Postgres Epoch. Negative values represent days before January 1 2000.
- TIME values are stored as 8 bytes representation uint64 of the number of milliseconds from midnight.

Record field values are stored in the sequence they are defined in their Table - the record layout definition stores the offset + length of each field. There is one byte in front of every filed, which is a boolean flag indicating if the following bytes represent a meaningful field value. Hence the offset of the first field in a record is headerSize + 1, not + 0. This HasValue flag is mainly used for optional fields, but can be applied to any field in the future.

The record header has three parts
- one byte boolean flag indicating if the record has been deleted and its space can be reused
- uint32 pointer (record ID) of the next deleted record in the file. This mechanism is used to create a linked list of reusable record spaces, with the first pointer to deleted record in the file header. If there is not next deleted record the pointer value is set to 0xFFFF (sentinel value)
- an array of uint32 representing Sets. Sets are stored using the same mechanism as dictionary postings, so each of the uint32 is block a numbers in a postings-like file for the set.

### Records Indexing

STEP2 supports single field based primary indexing, where the key filed must be one of the following types
- SMALLINT
- INT
- BIGINT, or
- CHAR[4-32] - a character array of the size from 4 to 32 characters/bytes

Character arrays are meant to support character bases identifies such as car license plates, employee ids, driver license numbers, vin numbers, etc.
Most of integer-based keys will be positive integers, but the STEP2 does not support unsigned integer field types leaving it to the application to allowing only positive integers.

### DB Schema and Files

A DatabaseSchema is data structure with descriptions of table record layouts and sets. It also includes string->int maps mapping record and filed names into their sequential positions in schema/table definitions. The DatabaseSchema is serialized into a JSON object and stored in the root directory of the database. The schema is created by validating and using the Schema structure create by the step2DDLparser.

The database itself is stored as a collection of files and subdirectories rooted in a directory named schema name in the DDL description (the SCHEMA <name> clause). The directory/file tree is as follows

<SCHEMA_name> dir
    schema.json - file storing DatabaseSchema serialized into a JSON object
    <Table_name0> dir - directory with data in the first defined table
        records.dat - file with table records
        primindex.dat - file with primary index of the table
        <Set_name0>.dat - file with postings for the first set defined in the table
        ...
        <Set_nameN>.dat - file with postings for the last set defined in the table
        <Sting_property0_name> dir - directory containing the files of the dictionary for the first string property of the table records
            strings.dat
	        offsets.dat
	        postings.dat
	        index.dat
	        prefix.dat
        ...
        <String_propertyM_name> dir - directory containing the files of the dictionary for the last string property of the table records
            ...
    ...
    <Table_nameK> dir - directory with data in the last defined table
        ...

    In summary
    - in the root directory there is one subdirectory for each Table defined in the schema
    - in a table subdirectory there is one subdirectory for every string field/column defined in the table for the corresponding string dictionary
    - names of root directory, table subdirectories, and dictionary subdirectories follow schema, table, and field names respectively.
    - records and dictionary files have fixed names
    - set postings files names have set names.

## Data Manipulation Language (DML) commands

The STEP2 DML commands are defined in `step2\docs\architecture\DML-commands.md`. All commands take JSON record as input and return JSON record as output.

If record type is not implied by the command, the record instance is passed as follows

```json
{
    "tableName": string,
    "record": { /* field data */ }
}
```

If a command returns a record instance, the instance is also returned in the above format.

Every record in a table has a `recordID`, which is its sequential number in the `records.dat` file (each record type is of fixed size, so recordID implies offset of the record). Record IDs are unsigned uint32. If a command returns a list of record IDs, the list is returned as follows

```json
"recordIDs": [unit32, ...]
```

Commands may not guarantee that the list is in recordID sorted order.

All commands return `status` property, which is a string indicating if the command succeeded **"ok"** or or failed **"error"**. In case of failure, errors are returned as an array of strings of te form.
``` json
"errors" : [string, ...]
```
