package parser

type TokenKind string

const (
	TokenEOF      TokenKind = "EOF"      // end of input
	TokenIdent    TokenKind = "IDENT"    // Customers, Orders, ProductName
	TokenString   TokenKind = "STRING"   // "Active", "2024-09-13", "18:30:00", "Koza*"
	TokenNumber   TokenKind = "NUMBER"   // 1000, 33.77
	TokenDate     TokenKind = "DATE"     // 2026-03-11
	TokenTime     TokenKind = "TIME"     // 09:30, 09:30:00, 09:30:00.123
	TokenBoolean  TokenKind = "BOOLEAN"  // true, false
	TokenDot      TokenKind = "DOT"      // .
	TokenPipe     TokenKind = "PIPE"     // |
	TokenComma    TokenKind = "COMMA"    // ,
	TokenLParen   TokenKind = "LPAREN"   // (
	TokenRParen   TokenKind = "RPAREN"   // )
	TokenEq       TokenKind = "EQ"       // ==
	TokenNe       TokenKind = "NE"       // !=
	TokenLt       TokenKind = "LT"       // <
	TokenLe       TokenKind = "LE"       // <=
	TokenGt       TokenKind = "GT"       // >
	TokenGe       TokenKind = "GE"       // >=
	TokenWhere    TokenKind = "WHERE"    // where
	TokenNavigate TokenKind = "NAVIGATE" // navigate
	TokenSet      TokenKind = "SET"      // set
	TokenOn       TokenKind = "ON"       // on
	TokenReturn   TokenKind = "RETURN"   // return
	TokenLike     TokenKind = "LIKE"     // like
	TokenOr       TokenKind = "OR"       // or
	TokenAnd      TokenKind = "AND"      // and
	TokenNot      TokenKind = "NOT"      // not
)

type Position struct {
	Offset int
	Line   int
	Column int
}

type Token struct {
	Kind   TokenKind
	Lexeme string
	Pos    Position
}

var keywords = map[string]TokenKind{
	"where":    TokenWhere,
	"navigate": TokenNavigate,
	"set":      TokenSet,
	"on":       TokenOn,
	"return":   TokenReturn,
	"like":     TokenLike,
	"or":       TokenOr,
	"and":      TokenAnd,
	"not":      TokenNot,
	"true":     TokenBoolean,
	"false":    TokenBoolean,
}
