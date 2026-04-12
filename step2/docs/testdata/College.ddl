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
