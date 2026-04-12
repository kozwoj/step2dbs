package step2DDLparser

import (
	"strings"
	"unicode"
)

// ========== TOKEN TYPES ==========

type TokenKind int

const (
	TokenEOF     TokenKind = iota
	TokenUNKNOWN           // Invalid/unrecognized token

	// Keywords
	TokenSCHEMA
	TokenTABLE
	TokenSETS
	TokenPRIMARY
	TokenKEY
	TokenFOREIGN
	TokenOPTIONAL

	// Data type keywords
	TokenSMALLINT
	TokenINT
	TokenBIGINT
	TokenDECIMAL
	TokenFLOAT
	TokenSTRING
	TokenCHAR
	TokenBOOLEAN
	TokenDATE
	TokenTIME

	// Symbols
	TokenLParen   // (
	TokenRParen   // )
	TokenLBracket // [
	TokenRBracket // ]
	TokenComma    // ,
	TokenSemi     // ;

	// Literals
	TokenIdent   // identifier
	TokenInteger // integer literal
)

func (k TokenKind) String() string {
	switch k {
	case TokenEOF:
		return "EOF"
	case TokenUNKNOWN:
		return "UNKNOWN"
	case TokenSCHEMA:
		return "SCHEMA"
	case TokenTABLE:
		return "TABLE"
	case TokenSETS:
		return "SETS"
	case TokenPRIMARY:
		return "PRIMARY"
	case TokenKEY:
		return "KEY"
	case TokenFOREIGN:
		return "FOREIGN"
	case TokenOPTIONAL:
		return "OPTIONAL"
	case TokenSMALLINT:
		return "SMALLINT"
	case TokenINT:
		return "INT"
	case TokenBIGINT:
		return "BIGINT"
	case TokenDECIMAL:
		return "DECIMAL"
	case TokenFLOAT:
		return "FLOAT"
	case TokenSTRING:
		return "STRING"
	case TokenCHAR:
		return "CHAR"
	case TokenBOOLEAN:
		return "BOOLEAN"
	case TokenDATE:
		return "DATE"
	case TokenTIME:
		return "TIME"
	case TokenLParen:
		return "("
	case TokenRParen:
		return ")"
	case TokenLBracket:
		return "["
	case TokenRBracket:
		return "]"
	case TokenComma:
		return ","
	case TokenSemi:
		return ";"
	case TokenIdent:
		return "IDENTIFIER"
	case TokenInteger:
		return "INTEGER"
	default:
		return "UNKNOWN"
	}
}

type Token struct {
	Kind TokenKind
	Text string
	Line int // 1-based line number
	Col  int // 1-based column number
}

// ========== LEXER ==========

type Lexer struct {
	input string
	pos   int // current position in input
	line  int // current line (1-based)
	col   int // current column (1-based)
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input: input,
		pos:   0,
		line:  1,
		col:   1,
	}
}

// peek returns the current character without advancing
func (l *Lexer) peek() rune {
	if l.pos >= len(l.input) {
		return 0
	}
	return rune(l.input[l.pos])
}

// advance moves to the next character and updates line/column tracking
func (l *Lexer) advance() {
	if l.pos < len(l.input) {
		if l.input[l.pos] == '\n' {
			l.line++
			l.col = 1
		} else {
			l.col++
		}
		l.pos++
	}
}

// skipWhitespace skips all whitespace characters
func (l *Lexer) skipWhitespace() {
	for unicode.IsSpace(l.peek()) {
		l.advance()
	}
}

// skipComment skips from * to end of line
func (l *Lexer) skipComment() {
	// Skip the * character
	l.advance()
	// Skip until newline or end of input
	for {
		ch := l.peek()
		if ch == 0 || ch == '\n' {
			break
		}
		l.advance()
	}
}

// skipWhitespaceAndComments skips whitespace and * comments
func (l *Lexer) skipWhitespaceAndComments() {
	for {
		l.skipWhitespace()
		if l.peek() == '*' {
			l.skipComment()
		} else {
			break
		}
	}
}

// readIdentifier reads an identifier or keyword
// Identifiers must start with a letter, followed by letters, digits, or underscores
func (l *Lexer) readIdentifier() string {
	start := l.pos
	// First character must be a letter (already verified by caller)
	l.advance()
	// Subsequent characters can be letters, digits, or underscores
	for {
		ch := l.peek()
		if unicode.IsLetter(ch) || unicode.IsDigit(ch) || ch == '_' {
			l.advance()
		} else {
			break
		}
	}
	return l.input[start:l.pos]
}

// readInteger reads an integer literal
func (l *Lexer) readInteger() string {
	start := l.pos
	for unicode.IsDigit(l.peek()) {
		l.advance()
	}
	return l.input[start:l.pos]
}

// keywordKind maps keyword strings to their token types
func keywordKind(word string) (TokenKind, bool) {
	upper := strings.ToUpper(word)
	switch upper {
	case "SCHEMA":
		return TokenSCHEMA, true
	case "TABLE":
		return TokenTABLE, true
	case "SETS":
		return TokenSETS, true
	case "PRIMARY":
		return TokenPRIMARY, true
	case "KEY":
		return TokenKEY, true
	case "FOREIGN":
		return TokenFOREIGN, true
	case "OPTIONAL":
		return TokenOPTIONAL, true
	case "SMALLINT":
		return TokenSMALLINT, true
	case "INT":
		return TokenINT, true
	case "BIGINT":
		return TokenBIGINT, true
	case "DECIMAL":
		return TokenDECIMAL, true
	case "FLOAT":
		return TokenFLOAT, true
	case "STRING":
		return TokenSTRING, true
	case "CHAR":
		return TokenCHAR, true
	case "BOOLEAN":
		return TokenBOOLEAN, true
	case "DATE":
		return TokenDATE, true
	case "TIME":
		return TokenTIME, true
	default:
		return TokenIdent, false
	}
}

// NextToken returns the next token from the input
func (l *Lexer) NextToken() Token {
	l.skipWhitespaceAndComments()

	// Record position for this token
	line := l.line
	col := l.col

	ch := l.peek()
	if ch == 0 {
		return Token{Kind: TokenEOF, Text: "", Line: line, Col: col}
	}

	// Single-character symbols
	switch ch {
	case '(':
		l.advance()
		return Token{Kind: TokenLParen, Text: "(", Line: line, Col: col}
	case ')':
		l.advance()
		return Token{Kind: TokenRParen, Text: ")", Line: line, Col: col}
	case '[':
		l.advance()
		return Token{Kind: TokenLBracket, Text: "[", Line: line, Col: col}
	case ']':
		l.advance()
		return Token{Kind: TokenRBracket, Text: "]", Line: line, Col: col}
	case ',':
		l.advance()
		return Token{Kind: TokenComma, Text: ",", Line: line, Col: col}
	case ';':
		l.advance()
		return Token{Kind: TokenSemi, Text: ";", Line: line, Col: col}
	}

	// Integer literal
	if unicode.IsDigit(ch) {
		text := l.readInteger()
		return Token{Kind: TokenInteger, Text: text, Line: line, Col: col}
	}

	// Identifier or keyword
	if unicode.IsLetter(ch) {
		text := l.readIdentifier()
		kind, isKeyword := keywordKind(text)
		if isKeyword {
			return Token{Kind: kind, Text: text, Line: line, Col: col}
		}
		return Token{Kind: TokenIdent, Text: text, Line: line, Col: col}
	}

	// Unknown character - return UNKNOWN token for parser error handling
	l.advance()
	return Token{Kind: TokenUNKNOWN, Text: string(ch), Line: line, Col: col}
}

// LexAll tokenizes the entire input and returns a slice of tokens
func LexAll(input string) []Token {
	lexer := NewLexer(input)
	var tokens []Token
	for {
		tok := lexer.NextToken()
		tokens = append(tokens, tok)
		if tok.Kind == TokenEOF {
			break
		}
	}
	return tokens
}
