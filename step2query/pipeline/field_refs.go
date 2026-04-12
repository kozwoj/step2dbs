package pipeline

/*
QualifiedFieldRef identifies a field after AST validation and name resolution.
For DB-backed states, TableName and FieldName are sufficient.
For ReturnWorkingSet states, Position can be used to avoid repeated name lookup at execution time.
*/
type QualifiedFieldRef struct {
	TableName string
	FieldName string
	Position  int
	Type      FieldType
}

/*
ReturnFieldRef identifies one projected field in a return clause.
*/
type ReturnFieldRef struct {
	Source QualifiedFieldRef
	Alias  string
}