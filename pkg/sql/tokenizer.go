package sql

import (
	"fmt"
	"strings"
	"unicode"
)

// TokenType represents the type of token
type TokenType int

const (
	TokenEOF TokenType = iota
	TokenKeyword
	TokenIdentifier
	TokenNumber
	TokenString
	TokenOperator
	TokenComma
	TokenSemicolon
	TokenLeftParen
	TokenRightParen
	TokenStar
)

// Token represents a lexical token
type Token struct {
	Type  TokenType
	Value string
}

// Tokenizer breaks SQL into tokens
type Tokenizer struct {
	input  string
	pos    int
	tokens []Token
}

// NewTokenizer creates a new tokenizer
func NewTokenizer(input string) *Tokenizer {
	return &Tokenizer{
		input:  input,
		pos:    0,
		tokens: make([]Token, 0),
	}
}

// Tokenize converts input string into tokens
func (t *Tokenizer) Tokenize() ([]Token, error) {
	for t.pos < len(t.input) {
		ch := t.input[t.pos]

		// Skip whitespace
		if unicode.IsSpace(rune(ch)) {
			t.pos++
			continue
		}

		// Handle strings (single quotes)
		if ch == '\'' {
			if err := t.readString(); err != nil {
				return nil, err
			}
			continue
		}

		// Handle numbers
		if unicode.IsDigit(rune(ch)) {
			t.readNumber()
			continue
		}

		// Handle identifiers and keywords
		if unicode.IsLetter(rune(ch)) || ch == '_' {
			t.readIdentifierOrKeyword()
			continue
		}

		// Handle operators and punctuation
		switch ch {
		case '(':
			t.tokens = append(t.tokens, Token{Type: TokenLeftParen, Value: "("})
			t.pos++
		case ')':
			t.tokens = append(t.tokens, Token{Type: TokenRightParen, Value: ")"})
			t.pos++
		case ',':
			t.tokens = append(t.tokens, Token{Type: TokenComma, Value: ","})
			t.pos++
		case ';':
			t.tokens = append(t.tokens, Token{Type: TokenSemicolon, Value: ";"})
			t.pos++
		case '=':
			t.tokens = append(t.tokens, Token{Type: TokenOperator, Value: "="})
			t.pos++
		case '*':
			t.tokens = append(t.tokens, Token{Type: TokenStar, Value: "*"})
			t.pos++
		default:
			return nil, fmt.Errorf("unexpected character: %c at position %d", ch, t.pos)
		}
	}

	t.tokens = append(t.tokens, Token{Type: TokenEOF, Value: ""})
	return t.tokens, nil
}

// readString reads a string literal enclosed in single quotes
func (t *Tokenizer) readString() error {
	t.pos++ // Skip opening quote
	start := t.pos

	for t.pos < len(t.input) && t.input[t.pos] != '\'' {
		t.pos++
	}

	if t.pos >= len(t.input) {
		return fmt.Errorf("unterminated string literal")
	}

	value := t.input[start:t.pos]
	t.tokens = append(t.tokens, Token{Type: TokenString, Value: value})
	t.pos++ // Skip closing quote

	return nil
}

// readNumber reads a numeric literal
func (t *Tokenizer) readNumber() {
	start := t.pos

	for t.pos < len(t.input) && unicode.IsDigit(rune(t.input[t.pos])) {
		t.pos++
	}

	value := t.input[start:t.pos]
	t.tokens = append(t.tokens, Token{Type: TokenNumber, Value: value})
}

// readIdentifierOrKeyword reads an identifier or keyword
func (t *Tokenizer) readIdentifierOrKeyword() {
	start := t.pos

	for t.pos < len(t.input) {
		ch := rune(t.input[t.pos])
		if !unicode.IsLetter(ch) && !unicode.IsDigit(ch) && ch != '_' {
			break
		}
		t.pos++
	}

	value := t.input[start:t.pos]
	upper := strings.ToUpper(value)

	// Check if it's a keyword
	keywords := map[string]bool{
		"SELECT": true,
		"INSERT": true,
		"INTO":   true,
		"VALUES": true,
		"FROM":   true,
		"WHERE":  true,
	}

	if keywords[upper] {
		t.tokens = append(t.tokens, Token{Type: TokenKeyword, Value: upper})
	} else {
		t.tokens = append(t.tokens, Token{Type: TokenIdentifier, Value: value})
	}
}
