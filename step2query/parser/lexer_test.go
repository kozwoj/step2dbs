package parser

import "testing"

func TestTokenizePipelineWithDateAndTime(t *testing.T) {
	input := "Orders | where Orders.OrderDate == 2026-03-11 and Orders.OrderTime >= 09:30:00 | return Orders.OrderID"

	tokens, err := Tokenize(input)
	if err != nil {
		t.Fatalf("Tokenize returned error: %v", err)
	}

	assertTokenKinds(t, tokens,
		TokenIdent,
		TokenPipe,
		TokenWhere,
		TokenIdent,
		TokenDot,
		TokenIdent,
		TokenEq,
		TokenDate,
		TokenAnd,
		TokenIdent,
		TokenDot,
		TokenIdent,
		TokenGe,
		TokenTime,
		TokenPipe,
		TokenReturn,
		TokenIdent,
		TokenDot,
		TokenIdent,
		TokenEOF,
	)

	if tokens[7].Lexeme != "2026-03-11" {
		t.Fatalf("expected DATE lexeme 2026-03-11, got %q", tokens[7].Lexeme)
	}
	if tokens[13].Lexeme != "09:30:00" {
		t.Fatalf("expected TIME lexeme 09:30:00, got %q", tokens[13].Lexeme)
	}
}

func TestQuotedDateRemainsString(t *testing.T) {
	input := `Orders | where Orders.Note == "2026-03-11"`

	tokens, err := Tokenize(input)
	if err != nil {
		t.Fatalf("Tokenize returned error: %v", err)
	}

	if tokens[7].Kind != TokenString {
		t.Fatalf("expected quoted date-like literal to be STRING, got %s", tokens[7].Kind)
	}
}

func TestTokenizeLikePrefixSearch(t *testing.T) {
	input := `Students | where Students.LastName like "Koza*" | return Students.StudentID`

	tokens, err := Tokenize(input)
	if err != nil {
		t.Fatalf("Tokenize returned error: %v", err)
	}

	assertTokenKinds(t, tokens,
		TokenIdent,
		TokenPipe,
		TokenWhere,
		TokenIdent,
		TokenDot,
		TokenIdent,
		TokenLike,
		TokenString,
		TokenPipe,
		TokenReturn,
		TokenIdent,
		TokenDot,
		TokenIdent,
		TokenEOF,
	)

	if tokens[7].Lexeme != `"Koza*"` {
		t.Fatalf("expected quoted prefix pattern \"Koza*\", got %q", tokens[7].Lexeme)
	}
}

func TestUnquotedPrefixPatternIsRejected(t *testing.T) {
	_, err := Tokenize("Students | where Students.LastName like Koza*")
	if err == nil {
		t.Fatal("expected error for unquoted prefix pattern")
	}
}

func TestInvalidCharacterProducesError(t *testing.T) {
	_, err := Tokenize("Orders @ where Orders.ID == 1")
	if err == nil {
		t.Fatal("expected error for invalid character")
	}
}

func assertTokenKinds(t *testing.T, tokens []Token, want ...TokenKind) {
	t.Helper()
	if len(tokens) != len(want) {
		t.Fatalf("expected %d tokens, got %d", len(want), len(tokens))
	}
	for index, kind := range want {
		if tokens[index].Kind != kind {
			t.Fatalf("token %d: expected %s, got %s (%q)", index, kind, tokens[index].Kind, tokens[index].Lexeme)
		}
	}
}
