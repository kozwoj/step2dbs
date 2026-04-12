package step2DDLparser

import (
	"fmt"
	"strings"
)

// ========== PARSER COMBINATOR CORE ==========

type ParserState struct {
	tokens []Token
	pos    int // current position in tokens
}

type ParseResult struct {
	Value any
	Pos   int
}

type Parser func(st ParserState) (ParseResult, *ParseError)

// ========== BASIC COMBINATORS ==========

// tokenKind matches a specific token type
func tokenKind(kind TokenKind) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		if st.pos >= len(st.tokens) {
			return ParseResult{}, newParseErrorEOF(kind.String())
		}
		tok := st.tokens[st.pos]
		if tok.Kind == kind {
			return ParseResult{Value: tok, Pos: st.pos + 1}, nil
		}
		return ParseResult{}, newParseError(kind.String(), tok, "")
	}
}

// tokenIdent matches an identifier
func tokenIdent() Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		if st.pos >= len(st.tokens) {
			return ParseResult{}, newParseErrorEOF("identifier")
		}
		tok := st.tokens[st.pos]
		if tok.Kind == TokenIdent {
			return ParseResult{Value: tok, Pos: st.pos + 1}, nil
		}
		return ParseResult{}, newParseError("identifier", tok, "")
	}
}

// tokenInteger matches an integer literal
func tokenInteger() Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		if st.pos >= len(st.tokens) {
			return ParseResult{}, newParseErrorEOF("integer")
		}
		tok := st.tokens[st.pos]
		if tok.Kind == TokenInteger {
			return ParseResult{Value: tok, Pos: st.pos + 1}, nil
		}
		return ParseResult{}, newParseError("integer", tok, "")
	}
}

// Sequence runs parsers in sequence and returns all results
func Sequence(parsers ...Parser) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		values := make([]any, 0, len(parsers))
		pos := st.pos
		for _, p := range parsers {
			res, err := p(ParserState{tokens: st.tokens, pos: pos})
			if err != nil {
				return ParseResult{}, err
			}
			values = append(values, res.Value)
			pos = res.Pos
		}
		return ParseResult{Value: values, Pos: pos}, nil
	}
}

// Choice tries parsers in order, returns first success
func Choice(parsers ...Parser) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		var lastErr *ParseError
		for _, p := range parsers {
			res, err := p(st)
			if err == nil {
				return res, nil
			}
			lastErr = err
		}
		return ParseResult{}, lastErr
	}
}

// Optional makes a parser optional (returns nil if not found)
func Optional(p Parser) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		res, err := p(st)
		if err != nil {
			return ParseResult{Value: nil, Pos: st.pos}, nil
		}
		return res, nil
	}
}

// ZeroOrMore matches zero or more occurrences
func ZeroOrMore(p Parser) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		values := []any{}
		pos := st.pos
		for {
			res, err := p(ParserState{tokens: st.tokens, pos: pos})
			if err != nil {
				break
			}
			values = append(values, res.Value)
			pos = res.Pos
		}
		return ParseResult{Value: values, Pos: pos}, nil
	}
}

// OneOrMore matches one or more occurrences
func OneOrMore(p Parser) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		// First occurrence is required
		firstRes, err := p(st)
		if err != nil {
			return ParseResult{}, err
		}
		values := []any{firstRes.Value}
		pos := firstRes.Pos

		// Continue matching more
		for {
			res, err := p(ParserState{tokens: st.tokens, pos: pos})
			if err != nil {
				break
			}
			values = append(values, res.Value)
			pos = res.Pos
		}
		return ParseResult{Value: values, Pos: pos}, nil
	}
}

// Apply transforms parser result using a function
func Apply(p Parser, fn func(any) any) Parser {
	return func(st ParserState) (ParseResult, *ParseError) {
		res, err := p(st)
		if err != nil {
			return ParseResult{}, err
		}
		return ParseResult{Value: fn(res.Value), Pos: res.Pos}, nil
	}
}

// ========== GRAMMAR PARSERS ==========
// These will be initialized in init() to handle mutual recursion

var (
	SchemaP           Parser
	TableDefP         Parser
	ColumnListP       Parser
	ColumnDefP        Parser
	DataTypeP         Parser
	ColumnConstraintP Parser
	SetDefsP          Parser
	SetDefP           Parser
	NameP             Parser
)

func init() {
	// name = identifier
	NameP = Apply(tokenIdent(), func(v any) any {
		return makeNameFromToken(asToken(v))
	})

	// data_type = numeric_type | string_type | char_array | temporal_type
	// numeric_type = "SMALLINT" | "INT" | "BIGINT" | "DECIMAL" | "FLOAT"
	// string_type = "STRING" [ "(" integer ")" ]
	// char_array = "CHAR" "[" integer "]"
	// temporal_type = "DATE" | "TIME"
	DataTypeP = func(st ParserState) (ParseResult, *ParseError) {
		// Try STRING with optional size
		stringWithSize := func(st ParserState) (ParseResult, *ParseError) {
			res, err := tokenKind(TokenSTRING)(st)
			if err != nil {
				return ParseResult{}, err
			}
			pos := res.Pos
			sizeLimit := 0

			// Check for optional (size)
			if pos < len(st.tokens) && st.tokens[pos].Kind == TokenLParen {
				sizeSeq, err := Sequence(
					tokenKind(TokenLParen),
					tokenInteger(),
					tokenKind(TokenRParen),
				)(ParserState{tokens: st.tokens, pos: pos})
				if err != nil {
					return ParseResult{}, err
				}
				parts := asAnySlice(sizeSeq.Value)
				sizeLimit = makeIntFromToken(asToken(parts[1]))
				pos = sizeSeq.Pos
			}

			return ParseResult{
				Value: struct {
					Type      uint8
					SizeLimit int
				}{Type: 6, SizeLimit: sizeLimit},
				Pos: pos,
			}, nil
		}

		// Try CHAR array
		charArray := Apply(
			Sequence(
				tokenKind(TokenCHAR),
				tokenKind(TokenLBracket),
				tokenInteger(),
				tokenKind(TokenRBracket),
			),
			func(v any) any {
				parts := asAnySlice(v)
				size := makeIntFromToken(asToken(parts[2]))
				return struct {
					Type      uint8
					SizeLimit int
				}{Type: 7, SizeLimit: size} // 7 = CHAR array
			},
		)

		// Simple types (no size parameter)
		simpleType := func(kind TokenKind, typeCode uint8) Parser {
			return Apply(tokenKind(kind), func(v any) any {
				return struct {
					Type      uint8
					SizeLimit int
				}{Type: typeCode, SizeLimit: 0}
			})
		}

		return Choice(
			simpleType(TokenSMALLINT, 1),
			simpleType(TokenINT, 2),
			simpleType(TokenBIGINT, 3),
			simpleType(TokenDECIMAL, 4),
			simpleType(TokenFLOAT, 5),
			stringWithSize,
			charArray,
			simpleType(TokenBOOLEAN, 8),
			simpleType(TokenDATE, 9),
			simpleType(TokenTIME, 10),
		)(st)
	}

	// column_constraint = "PRIMARY" "KEY" | "FOREIGN" "KEY" table_name | "OPTIONAL"
	ColumnConstraintP = func(st ParserState) (ParseResult, *ParseError) {
		// PRIMARY KEY
		primaryKey := Apply(
			Sequence(tokenKind(TokenPRIMARY), tokenKind(TokenKEY)),
			func(v any) any {
				return ColumnConstraint{
					IsPrimaryKey: true,
				}
			},
		)

		// FOREIGN KEY table_name
		foreignKey := Apply(
			Sequence(tokenKind(TokenFOREIGN), tokenKind(TokenKEY), NameP),
			func(v any) any {
				parts := asAnySlice(v)
				tableName := parts[2].(string)
				return ColumnConstraint{
					IsForeignKey: true,
					TableName:    tableName,
				}
			},
		)

		// OPTIONAL
		optional := Apply(
			tokenKind(TokenOPTIONAL),
			func(v any) any {
				return ColumnConstraint{
					IsOptional: true,
				}
			},
		)

		return Choice(primaryKey, foreignKey, optional)(st)
	}

	// column_def = column_name data_type [ column_constraint ]
	ColumnDefP = func(st ParserState) (ParseResult, *ParseError) {
		// Parse column name and data type
		res, err := Sequence(NameP, DataTypeP)(st)
		if err != nil {
			return ParseResult{}, err
		}

		parts := asAnySlice(res.Value)
		name := parts[0].(string)
		dataType := parts[1].(struct {
			Type      uint8
			SizeLimit int
		})
		pos := res.Pos

		// Check for optional constraint
		var constraints []ColumnConstraint
		if pos < len(st.tokens) {
			tok := st.tokens[pos]
			// If it's PRIMARY, FOREIGN, or OPTIONAL, try to parse constraint
			if tok.Kind == TokenPRIMARY || tok.Kind == TokenFOREIGN || tok.Kind == TokenOPTIONAL {
				constraintRes, err := ColumnConstraintP(ParserState{tokens: st.tokens, pos: pos})
				if err != nil {
					return ParseResult{}, err
				}
				constraints = []ColumnConstraint{constraintRes.Value.(ColumnConstraint)}
				pos = constraintRes.Pos
			} else if tok.Kind != TokenComma && tok.Kind != TokenRParen {
				// Not a constraint keyword and not end of column, might be a typo
				// Only report error if it looks like an attempt at a constraint
				if tok.Kind == TokenIdent {
					tokText := strings.ToUpper(tok.Text)
					if strings.Contains(tokText, "PRIMARY") || strings.Contains(tokText, "FOREIGN") ||
						strings.Contains(tokText, "KEY") {
						return ParseResult{}, newParseError(
							"PRIMARY KEY or FOREIGN KEY",
							tok,
							"constraint must be 'PRIMARY KEY' or 'FOREIGN KEY <table>'",
						)
					}
				}
			}
		}

		column := Column{
			Name:        name,
			Type:        dataType.Type,
			SizeLimit:   dataType.SizeLimit,
			Constraints: constraints,
		}

		return ParseResult{Value: column, Pos: pos}, nil
	}

	// column_list = column_def { "," column_def }
	ColumnListP = Apply(
		Sequence(
			ColumnDefP,
			ZeroOrMore(Sequence(tokenKind(TokenComma), ColumnDefP)),
		),
		func(v any) any {
			parts := asAnySlice(v)
			columns := []Column{parts[0].(Column)}

			if rest := asAnySlice(parts[1]); len(rest) > 0 {
				for _, item := range rest {
					seq := asAnySlice(item)
					columns = append(columns, seq[1].(Column))
				}
			}

			return columns
		},
	)

	// set_definition = set_name table_name
	SetDefP = Apply(
		Sequence(NameP, NameP),
		func(v any) any {
			parts := asAnySlice(v)
			return Set{
				Name:      parts[0].(string),
				TableName: parts[1].(string),
			}
		},
	)

	// set_defs = "SETS" "(" set_definition { "," set_definition} ")"
	SetDefsP = Apply(
		Sequence(
			tokenKind(TokenSETS),
			tokenKind(TokenLParen),
			SetDefP,
			ZeroOrMore(Sequence(tokenKind(TokenComma), SetDefP)),
			tokenKind(TokenRParen),
		),
		func(v any) any {
			parts := asAnySlice(v)
			sets := []Set{parts[2].(Set)}

			if rest := asAnySlice(parts[3]); len(rest) > 0 {
				for _, item := range rest {
					seq := asAnySlice(item)
					sets = append(sets, seq[1].(Set))
				}
			}

			return sets
		},
	)

	// table_definition = "TABLE" table_name "(" column_list ")" [ set_defs ] ";"
	TableDefP = func(st ParserState) (ParseResult, *ParseError) {
		// Parse: TABLE name ( columns )
		res, err := Sequence(
			tokenKind(TokenTABLE),
			NameP,
			tokenKind(TokenLParen),
			ColumnListP,
			tokenKind(TokenRParen),
		)(st)
		if err != nil {
			return ParseResult{}, err
		}

		parts := asAnySlice(res.Value)
		pos := res.Pos

		// After ), we expect either SETS or ;
		// Peek at the next token to provide better error message
		var sets []Set
		if pos < len(st.tokens) {
			tok := st.tokens[pos]
			if tok.Kind == TokenSETS {
				// Try to parse SETS definition
				setsRes, err := SetDefsP(ParserState{tokens: st.tokens, pos: pos})
				if err != nil {
					return ParseResult{}, err
				}
				sets = setsRes.Value.([]Set)
				pos = setsRes.Pos
			} else if tok.Kind != TokenSemi {
				// Not SETS and not ;, provide helpful error
				return ParseResult{}, newParseError(
					"'SETS' or ';'",
					tok,
					"after table columns, expected either SETS definition or semicolon",
				)
			}
		}

		// Parse the semicolon
		semiRes, err := tokenKind(TokenSemi)(ParserState{tokens: st.tokens, pos: pos})
		if err != nil {
			return ParseResult{}, err
		}

		// Build the table
		table := Table{
			Name:    parts[1].(string),
			Columns: parts[3].([]Column),
			Sets:    sets,
		}

		return ParseResult{Value: table, Pos: semiRes.Pos}, nil
	}

	// schema_definition = "SCHEMA" schema_name table_definition { table_definition }
	SchemaP = Apply(
		Sequence(
			tokenKind(TokenSCHEMA),
			NameP,
			OneOrMore(TableDefP),
		),
		func(v any) any {
			parts := asAnySlice(v)
			name := parts[1].(string)
			tablesAny := asAnySlice(parts[2])

			tables := make([]Table, len(tablesAny))
			for i, t := range tablesAny {
				tables[i] = t.(Table)
			}

			return Schema{
				Name:   name,
				Tables: tables,
			}
		},
	)
}

// ========== PUBLIC API ==========

// ParseSchema parses a complete step2 DDL schema from input string
func ParseSchema(input string) (Schema, error) {
	tokens := LexAll(input)

	// Check for UNKNOWN tokens
	for _, tok := range tokens {
		if tok.Kind == TokenUNKNOWN {
			return Schema{}, newParseError(
				"invalid token",
				tok,
				fmt.Sprintf("invalid character '%s'", tok.Text),
			)
		}
	}

	state := ParserState{tokens: tokens, pos: 0}
	res, err := SchemaP(state)
	if err != nil {
		return Schema{}, err
	}

	// Ensure we consumed all tokens (except EOF)
	if res.Pos != len(tokens)-1 {
		tok := tokens[res.Pos]
		return Schema{}, newParseError(
			"end of input or next table definition",
			tok,
			"unexpected token after schema definition",
		)
	}

	schema, ok := res.Value.(Schema)
	if !ok {
		return Schema{}, &ParseError{
			Line:        0,
			Col:         0,
			Expected:    "schema",
			Found:       "parse result",
			Explanation: "internal error: failed to convert parse result to Schema",
		}
	}

	return schema, nil
}
