This document contains examples of queries used to test the query language, and discussion of `where` boolean expressions. All queries are against the College DB schema.

``` text
SCHEMA College

 * represents departments at a hypothetical  college
TABLE Departments (
    Name                STRING(50),
    Department_code     CHAR[8] PRIMARY KEY,    * something like "COMPSCIE"
    Building_name       STRING,
    Building_code       CHAR[8]
)
SETS (
    Faculty Teachers
);

TABLE Teachers (
    Employee_id         CHAR[8] PRIMARY KEY,
    First_name          STRING(20),
    Last_name           STRING(30),
    Building_code       CHAR[8],
    Office              CHAR[10],
    Works_for           CHAR[8] FOREIGN KEY Departments
)
SETS (
    Teaches Classes,
    Advises Students
);

TABLE Students (
    First_name          STRING(20),
    Last_name           STRING(30),
    Preferred_name      STRING(20) OPTIONAL,
    Gender              CHAR[1],
    Birth_date          DATE,
    State_or_Country    STRING(30),             * US state or country of ID document
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
);

TABLE Courses (
    Course_code     CHAR[8] PRIMARY KEY,
    Name            STRING(40),
    Credits         SMALLINT,
    Description     STRING
);

TABLE Classes (
    Class_code      CHAR[10] PRIMARY KEY,
    Course          CHAR[8] FOREIGN KEY Courses,
    Teacher         CHAR[8] FOREIGN KEY Teachers,
    Place_times     STRING,
    Capacity        SMALLINT,
    Start_date      DATE,
    End_date        DATE
)
SETS (
    Enrollment      Students
);

TABLE Grades (
    Student_id      CHAR[10] FOREIGN KEY Students,
    Course_code     CHAR[8] FOREIGN KEY Courses,
    In_major        BOOLEAN,
    Grade           CHAR[2]
);

TABLE Majors (
    Major_code      CHAR[25] PRIMARY KEY,
    Description     STRING(80),
    Department      CHAR[8] FOREIGN KEY Departments,
    Coordinator     CHAR[8] FOREIGN KEY Teachers,
    Credits_req     SMALLINT
);
```
## `where` followed by `return`

Selection based on primary key
    Students
        | where Students.Student_id == "NIP2409002" 
        | return Students.First_name, Students.Last_name

Selection based on full string value
    Students
        | where Students.Last_name == "Perry" 
        | return Students.First_name, Students.Last_name

Selection based on prefix
    Students
        | where Students.Last_name like "Je*" 
        | return Students.First_name, Students.Last_name, Students.Year

Selection with string-based search `and` residual
    Students
        | where Students.State_or_Country == "Colorado" and Students.Year > 2
        | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits

Selection with string-based search `and` and residual with `or` on non-indexed field
    Students
        | where Students.State_or_Country == "Colorado" and (Students.Year == 2 or Students.Year == 3)
        | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits

Note: the `where` above can be optimized using the `State_or_Country` dictionary. The `where` below cannot.

    Students
        | where Students.State_or_Country == "Colorado" and Students.Year == 2 or Students.Year == 3 
        | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits

## `where` followed by `navigate set`

    Classes
        | where Classes.Class_code == "MATH101-01"
        | navigate set Classes.Enrollment
            return Students.Student_id, Students.Advisor

Note: the `where` clause above selects one record by primary key. The `where` below filters `Classes` records for the `MATH202 ` course value. The trailing space below is intentional because `Classes.Course` is `CHAR[8]`.

    Classes
        | where Classes.Course == "MATH202 "
        | navigate set Classes.Enrollment
            return Students.Student_id, Students.Last_name, Students.First_name

## `where` followed by two `navigate`

Note: in the case of two FK->PK navigates following each other, the first one needs to return the FK needed by the second navigate. In the example below that field is `Teachers.Works_for`.

    Classes
        | where Classes.Course == "MATH202 "
        | navigate Teachers on Classes.Teacher == Teachers.Employee_id
            return Classes.Class_code, Teachers.Works_for, Teachers.First_name, Teachers.Last_name, Teachers.Office
        | navigate Departments on Teachers.Works_for == Departments.Department_code
            return Classes.Class_code, Teachers.First_name, Teachers.Last_name, Departments.Building_name, Teachers.Office

## query for testing field name collisions 

    Teachers
        | where Teachers.Employee_id == "..."
        | navigate Departments on Teachers.Works_for == Departments.Department_code
            return Teachers.Building_code, Departments.Building_code

## `where` optimization test examples

The examples below exercise the three DB-backed where execution paths based on the `DBWhereAnalysisPlan` produced by the builder. Each example states the expected root classification and the search plan shape. FilterOnInput search nodes carry a Residual predicate that must be evaluated record-by-record on the candidate set produced by the search.

Classification rules (from where-expression-analysis.md):
- Searchable leaf: PK exact, STRING exact, STRING prefix
- Non-searchable leaf (FilterOnInput): CHAR equality, SMALLINT comparisons, etc.
- AND: if either side is Searchable, use that side's candidates; never produces NeedsFullInput
- OR: if both sides carry search plans, produces Or node (Searchable when both sides are pure-Searchable, FilterOnInput when either side carries a Residual); NeedsFullInput when either side lacks a search plan
- NOT: always NeedsFullInput

### Searchable — single PK lookup

    Students
        | where Students.Student_id == "NIP2409002"
        | return Students.First_name, Students.Last_name

Root: Searchable. Search: PrimaryKeyExact on Student_id. Residual: none.

### Searchable — single STRING exact lookup

    Students
        | where Students.Last_name == "Perry"
        | return Students.First_name, Students.Last_name

Root: Searchable. Search: StringExact on Last_name. Residual: none.

### Searchable — single STRING prefix lookup

    Students
        | where Students.Last_name like "Je*"
        | return Students.First_name, Students.Last_name, Students.Year

Root: Searchable. Search: StringPrefix on Last_name, prefix "Je". Residual: none.

### Searchable — AND intersection of two searchable leaves

    Students
        | where Students.State_or_Country == "Colorado" and Students.Last_name == "Johnson"
        | return Students.Student_id, Students.First_name, Students.Last_name

Root: Searchable. Search: And(StringExact State_or_Country, StringExact Last_name). Residual: none.

### Searchable — OR union of two searchable leaves

    Students
        | where Students.State_or_Country == "Colorado" or Students.State_or_Country == "Idaho"
        | return Students.Student_id, Students.Last_name, Students.State_or_Country

Root: Searchable. Search: Or(StringExact State_or_Country "Colorado", StringExact State_or_Country "Idaho"). Residual: none.

### Searchable — nested AND of (OR union) and searchable leaf

    Students
        | where (Students.State_or_Country == "Colorado" or Students.State_or_Country == "Idaho") and Students.Last_name == "Johnson"
        | return Students.Student_id, Students.First_name, Students.State_or_Country

Root: Searchable. Search: And(Or(StringExact "Colorado", StringExact "Idaho"), StringExact Last_name). Residual: none.

### FilterOnInput — searchable AND non-searchable residual

    Students
        | where Students.State_or_Country == "Colorado" and Students.Year > 2
        | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits

Root: FilterOnInput. Search: StringExact State_or_Country "Colorado", Residual: Year > 2.

### FilterOnInput — PK searchable AND non-searchable residual

    Students
        | where Students.Student_id == "NIP2409002" and Students.Year > 1
        | return Students.Student_id, Students.First_name, Students.Year

Root: FilterOnInput. Search: PrimaryKeyExact Student_id, Residual: Year > 1.

### FilterOnInput — (OR union) AND non-searchable residual

    Students
        | where (Students.State_or_Country == "Colorado" or Students.State_or_Country == "Idaho") and Students.Year == 2
        | return Students.Student_id, Students.Last_name, Students.Year, Students.State_or_Country

Root: FilterOnInput. Search: Or(StringExact "Colorado", StringExact "Idaho"), Residual: Year == 2.

### FilterOnInput — searchable AND nested non-searchable (OR of non-searchable)

    Students
        | where Students.Last_name == "Johnson" and (Students.Year == 2 or Students.Year == 3)
        | return Students.Student_id, Students.First_name, Students.Year

Root: FilterOnInput. Search: StringExact Last_name "Johnson", Residual: (Year == 2 or Year == 3). Note: inner OR classifies as NeedsFullInput (neither side has a search plan), but the outer AND with a searchable left side keeps root at FilterOnInput.

### FilterOnInput — AND intersection of searchable leaves AND non-searchable

    Students
        | where Students.State_or_Country == "Colorado" and Students.Last_name == "Johnson" and Students.Year > 1
        | return Students.Student_id, Students.First_name, Students.Year

Root: FilterOnInput. Search: And(StringExact State_or_Country, StringExact Last_name), Residual: Year > 1. The search narrows to the intersection of two dictionary lookups, then the residual filters for Year > 1.

### FilterOnInput — OR of two search-with-residual branches

    Students
        | where Students.State_or_Country == "Colorado" and Students.Year > 2 or Students.State_or_Country == "Nevada" and Students.Year == 1
        | return Students.Student_id, Students.Last_name, Students.Year, Students.State_or_Country

Root: FilterOnInput. Search:

    Or
    ├── StringExact(State_or_Country, "Colorado") + Residual: Year > 2
    └── StringExact(State_or_Country, "Nevada")   + Residual: Year == 1

Each OR branch independently narrows via dictionary lookup, then filters by its own residual; results are unioned.

### NeedsFullInput — OR with one non-searchable side (already in earlier section)

    Students
        | where Students.State_or_Country == "Colorado" and Students.Year == 2 or Students.Year == 3
        | return Students.Student_id, Students.Last_name, Students.Year, Students.Credits

Root: NeedsFullInput. Search: none. Parsed as (Colorado AND Year==2) OR Year==3; the right side (Year==3) has no search plan, so the OR cannot build a candidate set.