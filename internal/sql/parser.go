package sql

import (
	"fmt"
	"strconv"
)

// Statement represents a parsed SQL statement
type Statement interface {
	Type() string
}

// SelectStatement represents SELECT * FROM kv WHERE key = <value>
type SelectStatement struct {
	Table string
	Key   uint32
}

func (s *SelectStatement) Type() string {
	return "SELECT"
}

// InsertStatement represents INSERT INTO kv VALUES (<key>, '<value>')
type InsertStatement struct {
	Table string
	Key   uint32
	Value string
}

func (s *InsertStatement) Type() string {
	return "INSERT"
}

// Parser parses tokens into SQL statements
type Parser struct {
	tokens []Token
	pos    int
}

// NewParser creates a new parser
func NewParser(tokens []Token) *Parser {
	return &Parser{
		tokens: tokens,
		pos:    0,
	}
}

// Parse parses tokens into a statement
func (p *Parser) Parse() (Statement, error) {
	if p.pos >= len(p.tokens) {
		return nil, fmt.Errorf("empty statement")
	}

	token := p.current()

	if token.Type != TokenKeyword {
		return nil, fmt.Errorf("expected keyword, got %v", token)
	}

	switch token.Value {
	case "SELECT":
		return p.parseSelect()
	case "INSERT":
		return p.parseInsert()
	default:
		return nil, fmt.Errorf("unsupported statement: %s", token.Value)
	}
}

// parseSelect parses: SELECT * FROM kv WHERE key = <number>
func (p *Parser) parseSelect() (Statement, error) {
	// SELECT
	if err := p.expect(TokenKeyword, "SELECT"); err != nil {
		return nil, err
	}

	// *
	if err := p.expect(TokenStar, "*"); err != nil {
		return nil, err
	}

	// FROM
	if err := p.expect(TokenKeyword, "FROM"); err != nil {
		return nil, err
	}

	// table name
	tableToken := p.current()
	if tableToken.Type != TokenIdentifier {
		return nil, fmt.Errorf("expected table name, got %v", tableToken)
	}
	tableName := tableToken.Value
	p.advance()

	// WHERE
	if err := p.expect(TokenKeyword, "WHERE"); err != nil {
		return nil, err
	}

	// key
	if err := p.expect(TokenIdentifier, "key"); err != nil {
		return nil, err
	}

	// =
	if err := p.expect(TokenOperator, "="); err != nil {
		return nil, err
	}

	// number
	keyToken := p.current()
	if keyToken.Type != TokenNumber {
		return nil, fmt.Errorf("expected number, got %v", keyToken)
	}

	key, err := strconv.ParseUint(keyToken.Value, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %v", err)
	}
	p.advance()

	// Optional semicolon
	if p.current().Type == TokenSemicolon {
		p.advance()
	}

	return &SelectStatement{
		Table: tableName,
		Key:   uint32(key),
	}, nil
}

// parseInsert parses: INSERT INTO kv VALUES (<number>, '<string>')
func (p *Parser) parseInsert() (Statement, error) {
	// INSERT
	if err := p.expect(TokenKeyword, "INSERT"); err != nil {
		return nil, err
	}

	// INTO
	if err := p.expect(TokenKeyword, "INTO"); err != nil {
		return nil, err
	}

	// table name
	tableToken := p.current()
	if tableToken.Type != TokenIdentifier {
		return nil, fmt.Errorf("expected table name, got %v", tableToken)
	}
	tableName := tableToken.Value
	p.advance()

	// VALUES
	if err := p.expect(TokenKeyword, "VALUES"); err != nil {
		return nil, err
	}

	// (
	if err := p.expect(TokenLeftParen, "("); err != nil {
		return nil, err
	}

	// key (number)
	keyToken := p.current()
	if keyToken.Type != TokenNumber {
		return nil, fmt.Errorf("expected number for key, got %v", keyToken)
	}

	key, err := strconv.ParseUint(keyToken.Value, 10, 32)
	if err != nil {
		return nil, fmt.Errorf("invalid key: %v", err)
	}
	p.advance()

	// ,
	if err := p.expect(TokenComma, ","); err != nil {
		return nil, err
	}

	// value (string)
	valueToken := p.current()
	if valueToken.Type != TokenString {
		return nil, fmt.Errorf("expected string for value, got %v", valueToken)
	}
	value := valueToken.Value
	p.advance()

	// )
	if err := p.expect(TokenRightParen, ")"); err != nil {
		return nil, err
	}

	// Optional semicolon
	if p.current().Type == TokenSemicolon {
		p.advance()
	}

	return &InsertStatement{
		Table: tableName,
		Key:   uint32(key),
		Value: value,
	}, nil
}

func (p *Parser) current() Token {
	if p.pos >= len(p.tokens) {
		return Token{Type: TokenEOF, Value: ""}
	}
	return p.tokens[p.pos]
}

func (p *Parser) advance() {
	p.pos++
}

func (p *Parser) expect(tokenType TokenType, value string) error {
	token := p.current()

	if token.Type != tokenType {
		return fmt.Errorf("expected token type %v, got %v", tokenType, token.Type)
	}

	if value != "" && token.Value != value {
		return fmt.Errorf("expected '%s', got '%s'", value, token.Value)
	}

	p.advance()
	return nil
}
