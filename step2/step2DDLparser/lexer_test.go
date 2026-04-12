package step2DDLparser

import (
	"testing"
)

func TestLexer_Keywords(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected TokenKind
	}{
		{"SCHEMA keyword", "SCHEMA", TokenSCHEMA},
		{"TABLE keyword", "TABLE", TokenTABLE},
		{"SETS keyword", "SETS", TokenSETS},
		{"PRIMARY keyword", "PRIMARY", TokenPRIMARY},
		{"KEY keyword", "KEY", TokenKEY},
		{"FOREIGN keyword", "FOREIGN", TokenFOREIGN},
		{"SMALLINT type", "SMALLINT", TokenSMALLINT},
		{"INT type", "INT", TokenINT},
		{"BIGINT type", "BIGINT", TokenBIGINT},
		{"DECIMAL type", "DECIMAL", TokenDECIMAL},
		{"FLOAT type", "FLOAT", TokenFLOAT},
		{"STRING type", "STRING", TokenSTRING},
		{"CHAR type", "CHAR", TokenCHAR},
		{"BOOLEAN type", "BOOLEAN", TokenBOOLEAN},
		{"DATE type", "DATE", TokenDATE},
		{"TIME type", "TIME", TokenTIME},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Kind != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, tok.Kind)
			}
			if tok.Text != tt.input {
				t.Errorf("expected text '%s', got '%s'", tt.input, tok.Text)
			}
		})
	}
}

func TestLexer_CaseInsensitiveKeywords(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenKind
	}{
		{"schema", TokenSCHEMA},
		{"SCHEMA", TokenSCHEMA},
		{"Schema", TokenSCHEMA},
		{"table", TokenTABLE},
		{"TABLE", TokenTABLE},
		{"Table", TokenTABLE},
		{"primary", TokenPRIMARY},
		{"PRIMARY", TokenPRIMARY},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Kind != tt.expected {
				t.Errorf("input '%s': expected %v, got %v", tt.input, tt.expected, tok.Kind)
			}
		})
	}
}

func TestLexer_Symbols(t *testing.T) {
	tests := []struct {
		input    string
		expected TokenKind
	}{
		{"(", TokenLParen},
		{")", TokenRParen},
		{"[", TokenLBracket},
		{"]", TokenRBracket},
		{",", TokenComma},
		{";", TokenSemi},
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Kind != tt.expected {
				t.Errorf("expected %v, got %v", tt.expected, tok.Kind)
			}
		})
	}
}

func TestLexer_Identifiers(t *testing.T) {
	tests := []string{
		"Customer_id",
		"Company_name",
		"Employees",
		"test123",
		"CamelCase",
		"a_b_c",
		"name123_test",
	}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			lexer := NewLexer(input)
			tok := lexer.NextToken()
			if tok.Kind != TokenIdent {
				t.Errorf("expected TokenIdent, got %v", tok.Kind)
			}
			if tok.Text != input {
				t.Errorf("expected text '%s', got '%s'", input, tok.Text)
			}
		})
	}
}

func TestLexer_InvalidIdentifiers(t *testing.T) {
	// Identifiers starting with underscore or digit should be tokenized as unknown/error tokens
	tests := []struct {
		input    string
		expected TokenKind
	}{
		{"_underscore", TokenUNKNOWN}, // underscore alone becomes UNKNOWN token
		{"123abc", TokenInteger},       // starts as integer, stops at letter
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			// First token should be the invalid start character
			if tok.Kind != tt.expected {
				t.Errorf("expected %v, got %v (text: '%s')", tt.expected, tok.Kind, tok.Text)
			}
		})
	}
}

func TestLexer_Integers(t *testing.T) {
	tests := []string{"10", "40", "30", "60", "15", "24", "0", "123"}

	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			lexer := NewLexer(input)
			tok := lexer.NextToken()
			if tok.Kind != TokenInteger {
				t.Errorf("expected TokenInteger, got %v", tok.Kind)
			}
			if tok.Text != input {
				t.Errorf("expected text '%s', got '%s'", input, tok.Text)
			}
		})
	}
}

func TestLexer_LineColumnTracking(t *testing.T) {
	input := `SCHEMA TestSchema
TABLE Customers`

	lexer := NewLexer(input)

	tok1 := lexer.NextToken()
	if tok1.Line != 1 || tok1.Col != 1 {
		t.Errorf("SCHEMA: expected line 1, col 1, got line %d, col %d", tok1.Line, tok1.Col)
	}

	tok2 := lexer.NextToken()
	if tok2.Line != 1 || tok2.Col != 8 {
		t.Errorf("TestSchema: expected line 1, col 8, got line %d, col %d", tok2.Line, tok2.Col)
	}

	tok3 := lexer.NextToken()
	if tok3.Line != 2 || tok3.Col != 1 {
		t.Errorf("TABLE: expected line 2, col 1, got line %d, col %d", tok3.Line, tok3.Col)
	}

	tok4 := lexer.NextToken()
	if tok4.Line != 2 || tok4.Col != 7 {
		t.Errorf("Customers: expected line 2, col 7, got line %d, col %d", tok4.Line, tok4.Col)
	}
}

func TestLexer_ComplexExpression(t *testing.T) {
	input := "Customer_id CHAR[10] PRIMARY KEY"

	expected := []struct {
		kind TokenKind
		text string
	}{
		{TokenIdent, "Customer_id"},
		{TokenCHAR, "CHAR"},
		{TokenLBracket, "["},
		{TokenInteger, "10"},
		{TokenRBracket, "]"},
		{TokenPRIMARY, "PRIMARY"},
		{TokenKEY, "KEY"},
		{TokenEOF, ""},
	}

	lexer := NewLexer(input)
	for i, exp := range expected {
		tok := lexer.NextToken()
		if tok.Kind != exp.kind {
			t.Errorf("token %d: expected kind %v, got %v", i, exp.kind, tok.Kind)
		}
		if tok.Text != exp.text {
			t.Errorf("token %d: expected text '%s', got '%s'", i, exp.text, tok.Text)
		}
	}
}

func TestLexer_TableDefinition(t *testing.T) {
	input := `TABLE Customers (
    Customer_id CHAR[10] PRIMARY KEY,
    Company_name STRING(40)
);`

	tokens := LexAll(input)

	// Verify we have the expected token sequence
	expectedKinds := []TokenKind{
		TokenTABLE,
		TokenIdent, // Customers
		TokenLParen,
		TokenIdent, // Customer_id
		TokenCHAR,
		TokenLBracket,
		TokenInteger, // 10
		TokenRBracket,
		TokenPRIMARY,
		TokenKEY,
		TokenComma,
		TokenIdent, // Company_name
		TokenSTRING,
		TokenLParen,
		TokenInteger, // 40
		TokenRParen,
		TokenRParen,
		TokenSemi,
		TokenEOF,
	}

	if len(tokens) != len(expectedKinds) {
		t.Fatalf("expected %d tokens, got %d", len(expectedKinds), len(tokens))
	}

	for i, expected := range expectedKinds {
		if tokens[i].Kind != expected {
			t.Errorf("token %d: expected %v, got %v (text: '%s')",
				i, expected, tokens[i].Kind, tokens[i].Text)
		}
	}
}

func TestLexer_WhitespaceHandling(t *testing.T) {
	inputs := []string{
		"TABLE   Customers",
		"TABLE\tCustomers",
		"TABLE\nCustomers",
		"  TABLE  Customers  ",
	}

	for _, input := range inputs {
		t.Run(input, func(t *testing.T) {
			tokens := LexAll(input)
			// Should have TABLE, Customers, EOF
			if len(tokens) != 3 {
				t.Errorf("expected 3 tokens, got %d", len(tokens))
			}
			if tokens[0].Kind != TokenTABLE {
				t.Errorf("expected TokenTABLE, got %v", tokens[0].Kind)
			}
			if tokens[1].Kind != TokenIdent || tokens[1].Text != "Customers" {
				t.Errorf("expected identifier 'Customers', got %v '%s'", tokens[1].Kind, tokens[1].Text)
			}
		})
	}
}

func TestLexer_UnknownTokens(t *testing.T) {
	tests := []struct {
		name  string
		input string
		char  string
	}{
		{"underscore at start", "_invalid", "_"},
		{"dollar sign", "$test", "$"},
		{"at sign", "@symbol", "@"},
		{"hash", "#comment", "#"},
		{"percent", "%mod", "%"},
		{"ampersand", "&and", "&"},
		{"pipe", "|or", "|"},
		{"backslash", "\\path", "\\"},
		{"tilde", "~home", "~"},
		{"backtick", "`quoted`", "`"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lexer := NewLexer(tt.input)
			tok := lexer.NextToken()
			if tok.Kind != TokenUNKNOWN {
				t.Errorf("expected TokenUNKNOWN, got %v", tok.Kind)
			}
			if tok.Text != tt.char {
				t.Errorf("expected text '%s', got '%s'", tt.char, tok.Text)
			}
			// Verify line/col are tracked
			if tok.Line != 1 || tok.Col != 1 {
				t.Errorf("expected line 1, col 1, got line %d, col %d", tok.Line, tok.Col)
			}
		})
	}
}

func TestLexer_UnknownInContext(t *testing.T) {
	// UNKNOWN token in the middle of valid syntax
	input := "TABLE Test$ (id INT)"
	tokens := LexAll(input)

	expectedKinds := []TokenKind{
		TokenTABLE,
		TokenIdent,   // Test
		TokenUNKNOWN, // $
		TokenLParen,
		TokenIdent, // id
		TokenINT,
		TokenRParen,
		TokenEOF,
	}

	if len(tokens) != len(expectedKinds) {
		t.Fatalf("expected %d tokens, got %d", len(expectedKinds), len(tokens))
	}

	for i, expected := range expectedKinds {
		if tokens[i].Kind != expected {
			t.Errorf("token %d: expected %v, got %v (text: '%s')",
				i, expected, tokens[i].Kind, tokens[i].Text)
		}
	}

	// Verify UNKNOWN token has correct text
	unknownToken := tokens[2]
	if unknownToken.Text != "$" {
		t.Errorf("UNKNOWN token text: expected '$', got '%s'", unknownToken.Text)
	}
}

func TestLexer_EOF(t *testing.T) {
	lexer := NewLexer("")
	tok := lexer.NextToken()
	if tok.Kind != TokenEOF {
		t.Errorf("expected TokenEOF for empty input, got %v", tok.Kind)
	}

	lexer2 := NewLexer("SCHEMA")
	lexer2.NextToken() // consume SCHEMA
	tok2 := lexer2.NextToken()
	if tok2.Kind != TokenEOF {
		t.Errorf("expected TokenEOF after consuming all tokens, got %v", tok2.Kind)
	}
}
