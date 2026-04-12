# STEP2 Query Language Description

This document captures language description for the graph-relational pipeline query language, including grammar, parser outline, and validation responsibilities.

## Scope

The formal language layer is responsible for:

- lexical structure and tokens
- grammar and abstract syntax tree shape
- parser behavior and error reporting
- semantic validation against the database schema

It does not address execution strategy. Execution is described separately in [QueryEngine.md](./QueryEngine.md).

## Grammar Notes

The grammar still needs refinement around identifier rules. For Date and Time, this document assumes typed literals that the tokenizer can recognize directly. Prefix matching uses the `like` operator with a quoted string pattern instead of a dedicated token.

## Date and Time Literal Decision

Date and Time should be represented as typed, unquoted literals, not as strings.

Recommended canonical forms:

- `DateLiteral`: `YYYY-MM-DD`
- `TimeLiteral`: `HH:MM`, `HH:MM:SS`, or `HH:MM:SS.mmm`

Examples:

```text
| where Orders.OrderDate == 2026-03-11
| where Orders.OrderTime >= 09:30:00
```

Rationale:

- the tokenizer can emit distinct `DATE` and `TIME` tokens
- semantic validation can enforce type compatibility without guessing whether a string should be coerced
- quoted values remain plain strings, so `"2026-03-11"` is intentionally different from `2026-03-11`
- later planning and execution can work with typed scalar values instead of string conversions

## Prefix Match Decision

Prefix search should use an explicit `like` operator and a quoted string pattern.

Recommended form:

- right-hand side of `like`: a `StringLiteral` whose content is 1 to 8 characters followed by `*`

Examples:

```text
| where Students.LastName like "Koza*"
| where Products.Code like "AB12*"
```

This avoids overloading `==` with hidden prefix semantics while also making it visually clear that the match pattern is string-based. In particular:

- `A.B == "Koza*"` is string equality against the five-character string `Koza*`
- `A.B like "Koza*"` is prefix matching

The tokenizer should emit that pattern as `STRING`. A later validation pass should enforce that:

- the left-hand side of `like` resolves to a string-typed field or expression
- the right-hand string matches the prefix pattern rule
- the prefix contains at most 8 characters before `*`

## EBNF Draft

```text
Query
    = Identifier { "|" Stage } ;

Stage
    = WhereStage
    | NavigateFKStage
    | NavigateSetStage
    | ReturnStage ;

WhereStage
    = "where" BoolExpr ;

NavigateFKStage
    = "navigate" Identifier "on" QualifiedIdent "==" QualifiedIdent "return" ReturnList ;

NavigateSetStage
    = "navigate" "set" QualifiedIdent "return" ReturnList ;

ReturnStage
    = "return" ReturnList ;

ReturnList
    = ReturnItem { "," ReturnItem } ;

ReturnItem
    = QualifiedIdent ;

QualifiedIdent
    = Identifier "." Identifier ;

BoolExpr
    = OrExpr ;

OrExpr
    = AndExpr { "or" AndExpr } ;

AndExpr
    = NotExpr { "and" NotExpr } ;

NotExpr
    = [ "not" ] Primary ;

Primary
    = Comparison
    | "(" BoolExpr ")" ;

Comparison
    = ValueExpr CompareOp ValueExpr ;

CompareOp
    = "==" | "!=" | "<" | "<=" | ">" | ">=" | "like" ;

ValueExpr
    = QualifiedIdent
    | Literal ;

Literal
    = StringLiteral
    | NumberLiteral
    | BooleanLiteral
    | TimeLiteral
    | DateLiteral ;

StringLiteral
    = '"' { Character } '"' ;

NumberLiteral
    = Digit { Digit }
    | Digit { Digit } "." Digit { Digit } ;

BooleanLiteral
    = "true" | "false" ;

TimeLiteral
    = Digit Digit ":" Digit Digit
    | Digit Digit ":" Digit Digit ":" Digit Digit
    | Digit Digit ":" Digit Digit ":" Digit Digit "." Digit Digit Digit ;

DateLiteral
    = Digit Digit Digit Digit "-" Digit Digit "-" Digit Digit ;

Identifier
    = Letter { Letter | Digit | "_" } ;
```

## Tokens

A minimal token set:

```text
IDENT          // Customers, Orders, ProductName
DOT            // .
PIPE           // |
WHERE          // where
NAVIGATE       // navigate
SET            // set
ON             // on
RETURN         // return
LIKE           // like
OR             // or
AND            // and
NOT            // not
EQ             // ==
NE             // !=
LT             // <
LE             // <=
GT             // >
GE             // >=
LPAREN         // (
RPAREN         // )
COMMA          // ,
STRING         // "Active" or "2024-09-13" or "18:30:00" or "Koza*"
NUMBER         // 1000 or 33.77
DATE           // 2026-03-11
TIME           // 09:30, 09:30:00, 09:30:00.123
BOOLEAN        // true / false
EOF
```

Whitespace is ignored. Comments can be added later if needed, but they are not part of the current core language.

At the token level, quoted date-like and time-like text remains `STRING`. Only unquoted canonical forms become `DATE` and `TIME`. Prefix patterns are also tokenized as `STRING`; the `like` operator gives them their special meaning.

## AST Outline

One possible AST shape is:

```text
Query
    SourceStage  // it is a DB table where navigation begins
    []Stage

Stage
  WhereStage
  NavigateFKStage
  NavigateSetStage
  ReturnStage

BoolExpr
  OrExpr
  AndExpr
  NotExpr
  CompareExpr

ValueExpr
  FieldRef
  LiteralExpr
```

The AST should preserve enough structure for both semantic validation and later plan generation.

## Parser Structure

A recursive-descent parser is a good fit because the language is small, keyword-oriented, and has simple operator precedence.

The intended precedence is:

1. comparison operators: `==`, `!=`, `<`, `<=`, `>`, `>=`, `like`
2. `not`
3. `and`
4. `or`

With that rule, `not` applies to the following comparison or parenthesized boolean expression. For example:

```text
not Students.Active == true or Students.Name like "Koza*" and Students.Count >= 10
```

is parsed as:

```text
(not (Students.Active == true))
or
((Students.Name like "Koza*") and (Students.Count >= 10))
```

It is not parsed as:

```text
((not Students.Active) == true) or ...
```

If a different scope is intended, it should be written with parentheses.

```text
Parser
 ├── ParseQuery
 ├── parseSourceStage
 ├── parseStage
 │    ├── parseWhereStage
 │    ├── parseNavigateFKStage
 │    ├── parseNavigateSetStage
 │    └── parseReturnStage
 ├── parseBoolExpr
 │    ├── parseOrExpr
 │    ├── parseAndExpr
 │    ├── parseNotExpr
 │    └── parsePrimaryExpr
 ├── parseComparison
 ├── parseValueExpr
 ├── parseQualifiedIdent
 ├── parseReturnList
 └── parseLiteral
```

## Parser Entry Point

```go
func (p *Parser) ParseQuery() (*Query, error) {
    pipeline, err := p.parsePipeline()
    if err != nil {
        return nil, err
    }

    p.expect(EOF)
    return &Query{Pipeline: pipeline}, nil
}
```

## Pipeline Parsing

```go
func (p *Parser) parsePipeline() (*Pipeline, error) {
    source, err := p.parseSourceStage()
    if err != nil {
        return nil, err
    }

    stages := []Stage{}
    for p.match(PIPE) {
        st, err := p.parseStage()
        if err != nil {
            return nil, err
        }
        stages = append(stages, st)
    }

    return &Pipeline{
        Source: source,
        Stages: stages,
    }, nil
}
```

## Stage Parsing

```go
func (p *Parser) parseStage() (Stage, error) {
    switch {
    case p.match(WHERE):
        return p.parseWhereStage()
    case p.match(NAVIGATE):
        if p.match(SET) {
            return p.parseNavigateSetStage()
        }
        return p.parseNavigateFKStage()
    case p.match(RETURN):
        return p.parseReturnStage()
    default:
        return nil, p.error("expected 'where', 'navigate', or 'return'")
    }
}
```

## Where Stage

```go
func (p *Parser) parseWhereStage() (*WhereStage, error) {
    expr, err := p.parseBoolExpr()
    if err != nil {
        return nil, err
    }
    return &WhereStage{Expr: expr}, nil
}
```

## Navigate Stages

```go
func (p *Parser) parseNavigateFKStage() (*NavigateFKStage, error) {
    targetTable := p.expectIdent()

    p.expect(ON)
    left, err := p.parseQualifiedIdent()
    if err != nil {
        return nil, err
    }

    p.expect(EQ)
    right, err := p.parseQualifiedIdent()
    if err != nil {
        return nil, err
    }

    p.expect(RETURN)
    ret, err := p.parseReturnList()
    if err != nil {
        return nil, err
    }

    return &NavigateFKStage{
        TargetTable: targetTable,
        Join: JoinCond{Left: left, Right: right},
        Return: ret,
    }, nil
}

func (p *Parser) parseNavigateSetStage() (*NavigateSetStage, error) {
    setRef, err := p.parseQualifiedIdent()
    if err != nil {
        return nil, err
    }

    p.expect(RETURN)
    ret, err := p.parseReturnList()
    if err != nil {
        return nil, err
    }

    return &NavigateSetStage{
        SetRef: setRef,
        Return: ret,
    }, nil
}
```

## Return List

```go
func (p *Parser) parseReturnList() ([]ReturnItem, error) {
    items := []ReturnItem{}

    item, err := p.parseReturnItem()
    if err != nil {
        return nil, err
    }
    items = append(items, item)

    for p.match(COMMA) {
        item, err = p.parseReturnItem()
        if err != nil {
            return nil, err
        }
        items = append(items, item)
    }

    return items, nil
}

func (p *Parser) parseReturnItem() (ReturnItem, error) {
    qi, err := p.parseQualifiedIdent()
    if err != nil {
        return ReturnItem{}, err
    }
    return ReturnItem{Qual: qi}, nil
}
```

## Qualified Identifier

```go
func (p *Parser) parseQualifiedIdent() (QualifiedIdent, error) {
    table := p.expectIdent()
    p.expect(DOT)
    field := p.expectIdent()
    return QualifiedIdent{Table: table, Name: field}, nil
}
```

## Boolean Expression Parsing

Operator precedence:

1. `not`
2. `and`
3. `or`

```go
func (p *Parser) parseBoolExpr() (BoolExpr, error) {
    return p.parseOrExpr()
}

func (p *Parser) parseOrExpr() (BoolExpr, error) {
    left, err := p.parseAndExpr()
    if err != nil {
        return nil, err
    }
    for p.match(OR) {
        right, err := p.parseAndExpr()
        if err != nil {
            return nil, err
        }
        left = &OrExpr{Left: left, Right: right}
    }
    return left, nil
}

func (p *Parser) parseAndExpr() (BoolExpr, error) {
    left, err := p.parseNotExpr()
    if err != nil {
        return nil, err
    }
    for p.match(AND) {
        right, err := p.parseNotExpr()
        if err != nil {
            return nil, err
        }
        left = &AndExpr{Left: left, Right: right}
    }
    return left, nil
}

func (p *Parser) parseNotExpr() (BoolExpr, error) {
    if p.match(NOT) {
        expr, err := p.parsePrimaryExpr()
        if err != nil {
            return nil, err
        }
        return &NotExpr{Expr: expr}, nil
    }
    return p.parsePrimaryExpr()
}

func (p *Parser) parsePrimaryExpr() (BoolExpr, error) {
    if p.match(LPAREN) {
        expr, err := p.parseBoolExpr()
        if err != nil {
            return nil, err
        }
        p.expect(RPAREN)
        return expr, nil
    }
    return p.parseComparison()
}
```

## Comparison and Value Expressions

```go
func (p *Parser) parseComparison() (BoolExpr, error) {
    left, err := p.parseValueExpr()
    if err != nil {
        return nil, err
    }

    op := p.expectCompareOp()

    right, err := p.parseValueExpr()
    if err != nil {
        return nil, err
    }

    return &CompareExpr{Left: left, Op: op, Right: right}, nil
}

func (p *Parser) parseValueExpr() (ValueExpr, error) {
    switch {
    case p.peekIsQualifiedIdent():
        qi, err := p.parseQualifiedIdent()
        if err != nil {
            return nil, err
        }
        return &FieldRef{Qual: qi}, nil
    case p.peekIsLiteral():
        lit, err := p.parseLiteral()
        if err != nil {
            return nil, err
        }
        return &LiteralExpr{Value: lit}, nil
    default:
        return nil, p.error("expected value expression")
    }
}
```

## Error Handling

The parser should fail early and report precise source locations.

Typical parser errors include:

- unexpected token
- missing identifier
- missing `on` or malformed FK navigation
- missing return list
- malformed qualified identifier
- malformed boolean expression

Example diagnostic:

```text
line X, column Y: expected IDENT, found 'return'
```

## Semantic Validation

Parsing only confirms syntax. A separate validation walks the AST against the schema (the DBDefinition object) and the evolving working record types.

Validation should check that:

- source table exists
- every referenced table exists
- every referenced field exists on the table or working type where it is used
- a pipeline does not end with a bare `where`; if filtering is the final step, it must be followed by `return`
- `where` comparisons use type-compatible operands
- FK navigation references a valid FK to PK relationship
- SET navigation references a defined SET on the source table
- SET navigation appears only as the first navigation in the pipeline
- `return` items are valid and type-resolvable at that stage
- later stages only reference fields preserved by earlier projections

The validator should produce schema-aware errors such as:

```text
line X, column Y: field Orders.ProductID is not available after the previous return stage
```

## Outputs of the Language Front End

The language front end should produce:

- an AST for tooling and diagnostics
- a validated pipeline annotated with resolved schema information
- enough metadata to drive logical plan generation

The next layer is described in [QueryEngine.md](./QueryEngine.md).