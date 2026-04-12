package step2DDLparser

import (
	"fmt"
	"strings"
)

// ========== ERROR HANDLING ==========

// ParseError represents a parsing error with detailed location and context information
type ParseError struct {
	Line        int    // Line number where error occurred (1-based)
	Col         int    // Column number where error occurred (1-based)
	Expected    string // What token was expected
	Found       string // What was actually found (token text or type)
	Explanation string // Optional additional explanation/context
}

// formatExpected formats the Expected string with proper quotes
// Single-char tokens like ), (, ,, ; get quoted: ')' '(' ',' ';'
// Keywords like SCHEMA, TABLE stay unquoted
// Pre-formatted strings like "'SETS' or ';'" pass through unchanged
func formatExpected(expected string) string {
	// If already contains quotes, assume it's pre-formatted
	if strings.Contains(expected, "'") {
		return expected
	}

	// Single character tokens (punctuation) need quotes
	if len(expected) == 1 {
		return fmt.Sprintf("'%s'", expected)
	}

	// Multi-character tokens (keywords, identifiers) don't need quotes
	return expected
}

func (e *ParseError) Error() string {
	msg := fmt.Sprintf("line %d, col %d: expected %s, found '%s'",
		e.Line, e.Col, formatExpected(e.Expected), e.Found)
	if e.Explanation != "" {
		msg += " (" + e.Explanation + ")"
	}
	return msg
}

// newParseError creates a ParseError from a token with optional explanation
func newParseError(expected string, foundToken Token, explanation string) *ParseError {
	return &ParseError{
		Line:        foundToken.Line,
		Col:         foundToken.Col,
		Expected:    expected,
		Found:       foundToken.Text,
		Explanation: explanation,
	}
}

// newParseErrorEOF creates a ParseError for unexpected end of input
func newParseErrorEOF(expected string) *ParseError {
	return &ParseError{
		Line:        0,
		Col:         0,
		Expected:    expected,
		Found:       "EOF",
		Explanation: "unexpected end of input",
	}
}
