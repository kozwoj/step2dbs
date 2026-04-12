SCHEMA TestSchema

TABLE Customers (
    Customer_id CHAR[10] PRIMARY KEY,
    Company_name STRING(40),
    Contact_name STRING(30),
    Contact_title STRING(30),
    Address STRING(60),
    City STRING(15),
    Region STRING(15),
    Postal_code STRING(10),
    Country STRING(15),
    Phone STRING(15),
    Fax STRING(24) 
)
SETS (
    Reps Employees
);

TABLE Employees (
    Employee_id SMALLINT PRIMARY KEY,
    Last_name STRING(20),
    First_name STRING(10),
    Title STRING,
    Birth_date DATE,
    Hire_date DATE,
    Address STRING(60),
    Phone STRING(15),
    Reports_to SMALLINT FOREIGN KEY Employees
);