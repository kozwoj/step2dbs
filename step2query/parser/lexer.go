package parser

import (
	"fmt"
	"time"
)

type Lexer struct {
	input       string
	start       int
	current     int
	line        int
	column      int
	startLine   int
	startColumn int
	startOffset int
}

func NewLexer(input string) *Lexer {
	return &Lexer{
		input:  input,
		line:   1,
		column: 1,
	}
}

func Tokenize(input string) ([]Token, error) {
	lexer := NewLexer(input)
	return lexer.Tokenize()
}

func (l *Lexer) Tokenize() ([]Token, error) {
	var tokens []Token
	for {
		tok, err := l.NextToken()
		if err != nil {
			return nil, err
		}
		tokens = append(tokens, tok)
		if tok.Kind == TokenEOF {
			return tokens, nil
		}
	}
}

func (l *Lexer) NextToken() (Token, error) {
	l.skipWhitespace()
	l.start = l.current
	l.startOffset = l.current
	l.startLine = l.line
	l.startColumn = l.column

	if l.isAtEnd() {
		return l.makeToken(TokenEOF), nil
	}

	ch := l.advance()
	switch ch {
	case '.':
		return l.makeToken(TokenDot), nil
	case '|':
		return l.makeToken(TokenPipe), nil
	case ',':
		return l.makeToken(TokenComma), nil
	case '(':
		return l.makeToken(TokenLParen), nil
	case ')':
		return l.makeToken(TokenRParen), nil
	case '=':
		if l.match('=') {
			return l.makeToken(TokenEq), nil
		}
		return Token{}, l.errorf("unexpected '='; expected '=='")
	case '!':
		if l.match('=') {
			return l.makeToken(TokenNe), nil
		}
		return Token{}, l.errorf("unexpected '!'; expected '!='")
	case '<':
		if l.match('=') {
			return l.makeToken(TokenLe), nil
		}
		return l.makeToken(TokenLt), nil
	case '>':
		if l.match('=') {
			return l.makeToken(TokenGe), nil
		}
		return l.makeToken(TokenGt), nil
	case '"':
		return l.scanString()
	default:
		if isIdentifierStart(ch) {
			return l.scanIdentifier()
		}
		if isDigit(ch) {
			return l.scanNumberDateOrTime()
		}
		return Token{}, l.errorf("unexpected character %q", ch)
	}
}

func (l *Lexer) scanString() (Token, error) {
	for !l.isAtEnd() {
		ch := l.peek()
		if ch == '"' {
			l.advance()
			return l.makeToken(TokenString), nil
		}
		if ch == '\n' || ch == '\r' {
			return Token{}, l.errorf("unterminated string literal")
		}
		if ch == '\\' {
			l.advance()
			if l.isAtEnd() {
				return Token{}, l.errorf("unterminated string literal")
			}
		}
		l.advance()
	}

	return Token{}, l.errorf("unterminated string literal")
}

func (l *Lexer) scanIdentifier() (Token, error) {
	for isIdentifierPart(l.peek()) {
		l.advance()
	}

	lexeme := l.input[l.start:l.current]
	if kind, ok := keywords[lexeme]; ok {
		return l.makeToken(kind), nil
	}

	return l.makeToken(TokenIdent), nil
}

func (l *Lexer) scanNumberDateOrTime() (Token, error) {
	l.current = l.start
	l.line = l.startLine
	l.column = l.startColumn

	if length, ok := l.matchDateLiteral(); ok {
		l.consumeBytes(length)
		return l.makeToken(TokenDate), nil
	}

	l.current = l.start
	l.line = l.startLine
	l.column = l.startColumn
	if length, ok := l.matchTimeLiteral(); ok {
		l.consumeBytes(length)
		return l.makeToken(TokenTime), nil
	}

	l.current = l.start
	l.line = l.startLine
	l.column = l.startColumn
	l.consumeDigits()
	if l.peek() == '.' && isDigit(l.peekNext()) {
		l.advance()
		l.consumeDigits()
	}

	return l.makeToken(TokenNumber), nil
}

func (l *Lexer) matchDateLiteral() (int, bool) {
	if l.start+10 > len(l.input) {
		return 0, false
	}

	candidate := l.input[l.start : l.start+10]
	if !matchesDateShape(candidate) {
		return 0, false
	}
	if _, err := time.Parse("2006-01-02", candidate); err != nil {
		return 0, false
	}
	if !l.hasTokenBoundary(l.start + 10) {
		return 0, false
	}

	return 10, true
}

func (l *Lexer) matchTimeLiteral() (int, bool) {
	for _, length := range []int{12, 8, 5} {
		if l.start+length > len(l.input) {
			continue
		}
		candidate := l.input[l.start : l.start+length]
		layout := ""
		switch length {
		case 5:
			layout = "15:04"
		case 8:
			layout = "15:04:05"
		case 12:
			layout = "15:04:05.000"
		}
		if _, err := time.Parse(layout, candidate); err != nil {
			continue
		}
		if !l.hasTokenBoundary(l.start + length) {
			continue
		}
		return length, true
	}

	return 0, false
}

func matchesDateShape(candidate string) bool {
	return len(candidate) == 10 &&
		isDigit(candidate[0]) &&
		isDigit(candidate[1]) &&
		isDigit(candidate[2]) &&
		isDigit(candidate[3]) &&
		candidate[4] == '-' &&
		isDigit(candidate[5]) &&
		isDigit(candidate[6]) &&
		candidate[7] == '-' &&
		isDigit(candidate[8]) &&
		isDigit(candidate[9])
}

func (l *Lexer) hasTokenBoundary(index int) bool {
	if index >= len(l.input) {
		return true
	}
	next := l.input[index]
	return !isIdentifierPart(next) && !isDigit(next)
}

func (l *Lexer) skipWhitespace() {
	for {
		if l.isAtEnd() {
			return
		}
		switch l.peek() {
		case ' ', '\t', '\r', '\n':
			l.advance()
		default:
			return
		}
	}
}

func (l *Lexer) consumeDigits() {
	for isDigit(l.peek()) {
		l.advance()
	}
}

func (l *Lexer) consumeBytes(length int) {
	for range length {
		l.advance()
	}
}

func (l *Lexer) match(expected byte) bool {
	if l.isAtEnd() || l.input[l.current] != expected {
		return false
	}
	l.advance()
	return true
}

func (l *Lexer) peek() byte {
	if l.isAtEnd() {
		return 0
	}
	return l.input[l.current]
}

func (l *Lexer) peekNext() byte {
	if l.current+1 >= len(l.input) {
		return 0
	}
	return l.input[l.current+1]
}

func (l *Lexer) advance() byte {
	ch := l.input[l.current]
	l.current++
	if ch == '\n' {
		l.line++
		l.column = 1
	} else {
		l.column++
	}
	return ch
}

func (l *Lexer) isAtEnd() bool {
	return l.current >= len(l.input)
}

func (l *Lexer) makeToken(kind TokenKind) Token {
	return Token{
		Kind:   kind,
		Lexeme: l.input[l.start:l.current],
		Pos: Position{
			Offset: l.startOffset,
			Line:   l.startLine,
			Column: l.startColumn,
		},
	}
}

func (l *Lexer) errorf(format string, args ...any) error {
	return fmt.Errorf("line %d, column %d: %s", l.startLine, l.startColumn, fmt.Sprintf(format, args...))
}

func isDigit(ch byte) bool {
	return ch >= '0' && ch <= '9'
}

func isIdentifierStart(ch byte) bool {
	return (ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z') || ch == '_'
}

func isIdentifierPart(ch byte) bool {
	return isIdentifierStart(ch) || isDigit(ch)
}
