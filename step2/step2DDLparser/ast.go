package step2DDLparser

import "strconv"

// ========== AST TYPES ==========

type Schema struct {
	Name   string
	Tables []Table
}

type Table struct {
	Name    string
	Columns []Column
	Sets    []Set
}

type Column struct {
	Name        string
	Type        uint8
	SizeLimit   int // for CHAR/STRING types only. if 0 for STRING it means no limit (unlimited string)
	Constraints []ColumnConstraint
}

type ColumnConstraint struct {
	IsPrimaryKey bool
	IsForeignKey bool
	IsOptional   bool
	TableName    string // for foreign key constraint only
}

type Set struct {
	Name      string
	TableName string
}

// ========== HELPERS TO BUILD AST FROM PARSER VALUES ==========

func asToken(v any) Token {
	return v.(Token)
}

func asTokens(v any) []Token {
	return v.([]Token)
}

func asAnySlice(v any) []any {
	if v == nil {
		return nil
	}
	return v.([]any)
}

func makeNameFromToken(tok Token) string {
	return tok.Text
}

func makeIntFromToken(tok Token) int {
	n, _ := strconv.Atoi(tok.Text)
	return n
}
