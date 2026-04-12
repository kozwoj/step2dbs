schema_definition = "SCHEMA" schema_name table_definition { table_definition } ;

table_definition = "TABLE" table_name "(" column_list ")" [ set_defs ] ";" ;

column_list = column_def { "," column_def } ;

column_def = column_name data_type [ column_constraint ] ;

column_constraint = "PRIMARY" "KEY" | "FOREIGN" "KEY" table_name | "OPTIONAL" ;

set_defs = "SETS" "(" set_definition { "," set_definition} ")" ;

set_definition = set_name table_name ;

data_type = numeric_type | string_type | char_array | temporal_type | boolean_type ;

numeric_type = "SMALLINT" | "INT" | "BIGINT" | "DECIMAL" | "FLOAT" ;

string_type = "STRING" [ "(" integer ")" ] ;

char_array = "CHAR" "[" integer "]" ;

boolean_type = "BOOLEAN" ;

temporal_type = "DATE" | "TIME" ;

schema_name = identifier ; 

table_name = identifier ;

column_name = identifier ;

set_name = identifier ;

identifier = letter { letter | digit | "_" } ;
