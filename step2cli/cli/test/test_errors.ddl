SCHEMA TestErrors

TABLE Student (
    student_id INT PRIMARY KEY,
    student_id INT,
    name STRING
);

TABLE Course (
    course_id STRING PRIMARY KEY,
    title STRING,
    instructor INT FOREIGN KEY Student
);

TABLE DuplicateCourse (
    id INT PRIMARY KEY
);

TABLE DuplicateCourse (
    code CHAR[5] PRIMARY KEY
);
