package sql

import (
	"testing"
)

func TestTokenizer(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []TokenType
	}{
		{
			name:  "Simple SELECT",
			input: "SELECT * FROM kv WHERE key = 100;",
			expected: []TokenType{
				TokenKeyword, TokenStar, TokenKeyword, TokenIdentifier,
				TokenKeyword, TokenIdentifier, TokenOperator, TokenNumber,
				TokenSemicolon, TokenEOF,
			},
		},
		{
			name:  "Simple INSERT",
			input: "INSERT INTO kv VALUES (100, 'Naruto');",
			expected: []TokenType{
				TokenKeyword, TokenKeyword, TokenIdentifier, TokenKeyword,
				TokenLeftParen, TokenNumber, TokenComma, TokenString,
				TokenRightParen, TokenSemicolon, TokenEOF,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("Tokenize failed: %v", err)
			}

			if len(tokens) != len(tt.expected) {
				t.Errorf("Token count: got %d, expected %d", len(tokens), len(tt.expected))
			}

			for i, token := range tokens {
				if i >= len(tt.expected) {
					break
				}
				if token.Type != tt.expected[i] {
					t.Errorf("Token %d: type=%v, expected %v (value='%s')",
						i, token.Type, tt.expected[i], token.Value)
				}
			}
		})
	}
}

func TestTokenizerEdgeCases(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectError bool
	}{
		{"Unterminated string", "INSERT INTO kv VALUES (1, 'test", true},
		{"Empty string", "INSERT INTO kv VALUES (1, '');", false},
		{"String with spaces", "INSERT INTO kv VALUES (1, 'hello world');", false},
		{"Multiple spaces", "SELECT   *   FROM   kv   WHERE   key = 1;", false},
		{"No semicolon", "SELECT * FROM kv WHERE key = 1", false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			_, err := tokenizer.Tokenize()

			if tt.expectError && err == nil {
				t.Error("Expected error, got none")
			}

			if !tt.expectError && err != nil {
				t.Errorf("Unexpected error: %v", err)
			}
		})
	}
}
