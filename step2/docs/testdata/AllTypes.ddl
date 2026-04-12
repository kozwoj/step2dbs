SCHEMA AllTypesTes

TABLE AllTypes (
    Small_int_value SMALLINT,
    Integer_value INT PRIMARY KEY,
    Big_int_value BIGINT,
    Decimal_value DECIMAL,
    Float_value FLOAT,
    String_size_value STRING(60),
    String_no_size_value STRING,
    Char_array_value CHAR[15],
    Boolean_value BOOLEAN,
    Date_value DATE,
    Time_value TIME
);
