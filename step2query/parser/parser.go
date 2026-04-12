package parser

import (
	"fmt"
	"strings"
)

type Parser struct {
	tokens  []Token
	current int
}

func NewParser(tokens []Token) *Parser {
	return &Parser{tokens: tokens}
}

func Parse(input string) (*Query, error) {
	tokens, err := Tokenize(input)
	if err != nil {
		return nil, err
	}
	return NewParser(tokens).ParseQuery()
}

func (p *Parser) ParseQuery() (*Query, error) {
	start := p.peek().Pos

	source, err := p.parseSourceStage()
	if err != nil {
		return nil, err
	}

	stages := make([]Stage, 0)
	for p.match(TokenPipe) {
		stage, err := p.parseStage()
		if err != nil {
			return nil, err
		}
		stages = append(stages, stage)
	}

	eof, err := p.expect(TokenEOF, "end of query")
	if err != nil {
		return nil, err
	}

	return &Query{
		Source: source,
		Stages: stages,
		Span:   Span{Start: start, End: tokenEnd(eof)},
	}, nil
}

func (p *Parser) parseSourceStage() (*SourceStage, error) {
	table, err := p.expect(TokenIdent, "source table name")
	if err != nil {
		return nil, err
	}

	return &SourceStage{
		Table: table.Lexeme,
		Span:  Span{Start: table.Pos, End: tokenEnd(table)},
	}, nil
}

func (p *Parser) parseStage() (Stage, error) {
	switch {
	case p.match(TokenWhere):
		return p.parseWhereStage()
	case p.match(TokenNavigate):
		if p.match(TokenSet) {
			return p.parseNavigateSetStage()
		}
		return p.parseNavigateFKStage()
	case p.match(TokenReturn):
		return p.parseReturnStage()
	default:
		return nil, p.errorAtCurrent("expected 'where', 'navigate', or 'return'")
	}
}

func (p *Parser) parseWhereStage() (*WhereStage, error) {
	start := p.previous().Pos
	expr, err := p.parseBoolExpr()
	if err != nil {
		return nil, err
	}

	return &WhereStage{
		Expr: expr,
		Span: Span{Start: start, End: expr.NodeSpan().End},
	}, nil
}

func (p *Parser) parseNavigateFKStage() (*NavigateFKStage, error) {
	start := p.previous().Pos
	target, err := p.expect(TokenIdent, "target table name")
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenOn, "'on'"); err != nil {
		return nil, err
	}

	left, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}

	eq, err := p.expect(TokenEq, "'==' in join condition")
	if err != nil {
		return nil, err
	}

	right, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenReturn, "'return'"); err != nil {
		return nil, err
	}

	items, end, err := p.parseReturnList()
	if err != nil {
		return nil, err
	}

	return &NavigateFKStage{
		TargetTable: target.Lexeme,
		Join: JoinCond{
			Left:  *left,
			Right: *right,
			Span:  Span{Start: left.Span.Start, End: tokenEnd(eq)},
		},
		Return: items,
		Span:   Span{Start: start, End: end},
	}, nil
}

func (p *Parser) parseNavigateSetStage() (*NavigateSetStage, error) {
	start := p.previous().Pos
	setRef, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenReturn, "'return'"); err != nil {
		return nil, err
	}

	items, end, err := p.parseReturnList()
	if err != nil {
		return nil, err
	}

	return &NavigateSetStage{
		SetRef: *setRef,
		Return: items,
		Span:   Span{Start: start, End: end},
	}, nil
}

func (p *Parser) parseReturnStage() (*ReturnStage, error) {
	start := p.previous().Pos
	items, end, err := p.parseReturnList()
	if err != nil {
		return nil, err
	}

	return &ReturnStage{
		Items: items,
		Span:  Span{Start: start, End: end},
	}, nil
}

func (p *Parser) parseReturnList() ([]ReturnItem, Position, error) {
	first, err := p.parseReturnItem()
	if err != nil {
		return nil, Position{}, err
	}

	items := []ReturnItem{*first}
	end := first.Span.End
	for p.match(TokenComma) {
		item, err := p.parseReturnItem()
		if err != nil {
			return nil, Position{}, err
		}
		items = append(items, *item)
		end = item.Span.End
	}

	return items, end, nil
}

func (p *Parser) parseReturnItem() (*ReturnItem, error) {
	field, err := p.parseQualifiedIdent()
	if err != nil {
		return nil, err
	}

	return &ReturnItem{
		Field: *field,
		Span:  field.Span,
	}, nil
}

func (p *Parser) parseQualifiedIdent() (*QualifiedIdent, error) {
	table, err := p.expect(TokenIdent, "table name")
	if err != nil {
		return nil, err
	}

	if _, err := p.expect(TokenDot, "'.'"); err != nil {
		return nil, err
	}

	name, err := p.expect(TokenIdent, "field name")
	if err != nil {
		return nil, err
	}

	return &QualifiedIdent{
		Table: table.Lexeme,
		Name:  name.Lexeme,
		Span:  Span{Start: table.Pos, End: tokenEnd(name)},
	}, nil
}

func (p *Parser) parseBoolExpr() (BoolExpr, error) {
	return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() (BoolExpr, error) {
	left, err := p.parseAndExpr()
	if err != nil {
		return nil, err
	}

	for p.match(TokenOr) {
		right, err := p.parseAndExpr()
		if err != nil {
			return nil, err
		}
		left = &OrExpr{
			Left:  left,
			Right: right,
			Span:  Span{Start: left.NodeSpan().Start, End: right.NodeSpan().End},
		}
	}

	return left, nil
}

func (p *Parser) parseAndExpr() (BoolExpr, error) {
	left, err := p.parseNotExpr()
	if err != nil {
		return nil, err
	}

	for p.match(TokenAnd) {
		right, err := p.parseNotExpr()
		if err != nil {
			return nil, err
		}
		left = &AndExpr{
			Left:  left,
			Right: right,
			Span:  Span{Start: left.NodeSpan().Start, End: right.NodeSpan().End},
		}
	}

	return left, nil
}

func (p *Parser) parseNotExpr() (BoolExpr, error) {
	if p.match(TokenNot) {
		tok := p.previous()
		expr, err := p.parsePrimaryExpr()
		if err != nil {
			return nil, err
		}
		return &NotExpr{
			Expr: expr,
			Span: Span{Start: tok.Pos, End: expr.NodeSpan().End},
		}, nil
	}

	return p.parsePrimaryExpr()
}

func (p *Parser) parsePrimaryExpr() (BoolExpr, error) {
	if p.match(TokenLParen) {
		start := p.previous().Pos
		expr, err := p.parseBoolExpr()
		if err != nil {
			return nil, err
		}
		endTok, err := p.expect(TokenRParen, "')'")
		if err != nil {
			return nil, err
		}

		switch typed := expr.(type) {
		case *OrExpr:
			typed.Span = Span{Start: start, End: tokenEnd(endTok)}
		case *AndExpr:
			typed.Span = Span{Start: start, End: tokenEnd(endTok)}
		case *NotExpr:
			typed.Span = Span{Start: start, End: tokenEnd(endTok)}
		case *CompareExpr:
			typed.Span = Span{Start: start, End: tokenEnd(endTok)}
		}
		return expr, nil
	}

	return p.parseComparison()
}

func (p *Parser) parseComparison() (BoolExpr, error) {
	left, err := p.parseValueExpr()
	if err != nil {
		return nil, err
	}

	op, err := p.parseCompareOp()
	if err != nil {
		return nil, err
	}

	right, err := p.parseValueExpr()
	if err != nil {
		return nil, err
	}

	return &CompareExpr{
		Left:  left,
		Op:    op,
		Right: right,
		Span:  Span{Start: left.NodeSpan().Start, End: right.NodeSpan().End},
	}, nil
}

func (p *Parser) parseCompareOp() (CompareOp, error) {
	tok := p.peek()
	switch tok.Kind {
	case TokenEq:
		p.advance()
		return CompareEq, nil
	case TokenNe:
		p.advance()
		return CompareNe, nil
	case TokenLt:
		p.advance()
		return CompareLt, nil
	case TokenLe:
		p.advance()
		return CompareLe, nil
	case TokenGt:
		p.advance()
		return CompareGt, nil
	case TokenGe:
		p.advance()
		return CompareGe, nil
	case TokenLike:
		p.advance()
		return CompareLike, nil
	default:
		return "", p.errorAtCurrent("expected comparison operator")
	}
}

func (p *Parser) parseValueExpr() (ValueExpr, error) {
	if p.check(TokenIdent) && p.checkNext(TokenDot) {
		field, err := p.parseQualifiedIdent()
		if err != nil {
			return nil, err
		}
		return &FieldRef{Field: *field, Span: field.Span}, nil
	}

	literal, err := p.parseLiteral()
	if err != nil {
		return nil, err
	}
	return &LiteralExpr{Literal: literal, Span: literal.NodeSpan()}, nil
}

func (p *Parser) parseLiteral() (Literal, error) {
	tok := p.peek()
	span := Span{Start: tok.Pos, End: tokenEnd(tok)}

	switch tok.Kind {
	case TokenString:
		p.advance()
		return &StringLiteral{Raw: tok.Lexeme, Value: unquote(tok.Lexeme), Span: span}, nil
	case TokenNumber:
		p.advance()
		return &NumberLiteral{Raw: tok.Lexeme, Value: tok.Lexeme, Span: span}, nil
	case TokenDate:
		p.advance()
		return &DateLiteral{Raw: tok.Lexeme, Value: tok.Lexeme, Span: span}, nil
	case TokenTime:
		p.advance()
		return &TimeLiteral{Raw: tok.Lexeme, Value: tok.Lexeme, Span: span}, nil
	case TokenBoolean:
		p.advance()
		return &BooleanLiteral{Raw: tok.Lexeme, Value: tok.Lexeme == "true", Span: span}, nil
	default:
		return nil, p.errorAtCurrent("expected literal or qualified identifier")
	}
}

func (p *Parser) match(kinds ...TokenKind) bool {
	for _, kind := range kinds {
		if p.check(kind) {
			p.advance()
			return true
		}
	}
	return false
}

func (p *Parser) expect(kind TokenKind, expected string) (Token, error) {
	if p.check(kind) {
		return p.advance(), nil
	}
	return Token{}, p.errorAtCurrent("expected %s", expected)
}

func (p *Parser) check(kind TokenKind) bool {
	if p.isAtEnd() {
		return kind == TokenEOF
	}
	return p.peek().Kind == kind
}

func (p *Parser) checkNext(kind TokenKind) bool {
	if p.current+1 >= len(p.tokens) {
		return false
	}
	return p.tokens[p.current+1].Kind == kind
}

func (p *Parser) advance() Token {
	if !p.isAtEnd() {
		p.current++
	}
	return p.previous()
}

func (p *Parser) isAtEnd() bool {
	return p.peek().Kind == TokenEOF
}

func (p *Parser) peek() Token {
	return p.tokens[p.current]
}

func (p *Parser) previous() Token {
	return p.tokens[p.current-1]
}

func (p *Parser) errorAtCurrent(format string, args ...any) error {
	tok := p.peek()
	found := tok.Lexeme
	if tok.Kind == TokenEOF {
		found = "EOF"
	}
	message := fmt.Sprintf(format, args...)
	return fmt.Errorf("line %d, column %d: %s, found %s", tok.Pos.Line, tok.Pos.Column, message, quoteFound(found))
}

func tokenEnd(tok Token) Position {
	return Position{
		Offset: tok.Pos.Offset + len(tok.Lexeme),
		Line:   tok.Pos.Line,
		Column: tok.Pos.Column + len(tok.Lexeme),
	}
}

func unquote(raw string) string {
	if len(raw) >= 2 && strings.HasPrefix(raw, "\"") && strings.HasSuffix(raw, "\"") {
		return raw[1 : len(raw)-1]
	}
	return raw
}

func quoteFound(found string) string {
	if found == "EOF" {
		return found
	}
	return fmt.Sprintf("%q", found)
}
