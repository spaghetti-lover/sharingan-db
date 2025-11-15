package sql

import "testing"

func TestParserSelect(t *testing.T) {
	tests := []struct {
		input       string
		expectedKey uint32
		expectError bool
	}{
		{"SELECT * FROM kv WHERE key = 100;", 100, false},
		{"SELECT * FROM kv WHERE key = 50", 50, false},
		{"SELECT * FROM users WHERE key = 10;", 10, false}, // Different table name
		{"SELECT * FROM kv WHERE id = 100;", 0, true},      // Wrong column name
		{"SELECT key FROM kv WHERE key = 100;", 0, true},   // Wrong SELECT syntax
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				t.Fatalf("Tokenize failed: %v", err)
			}

			parser := NewParser(tokens)
			stmt, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			selectStmt, ok := stmt.(*SelectStatement)
			if !ok {
				t.Fatalf("Expected SelectStatement, got %T", stmt)
			}

			if selectStmt.Key != tt.expectedKey {
				t.Errorf("Key: got %d, expected %d", selectStmt.Key, tt.expectedKey)
			}
		})
	}
}

func TestParserInsert(t *testing.T) {
	tests := []struct {
		input         string
		expectedKey   uint32
		expectedValue string
		expectError   bool
	}{
		{"INSERT INTO kv VALUES (100, 'Naruto');", 100, "Naruto", false},
		{"INSERT INTO kv VALUES (50, 'Sasuke')", 50, "Sasuke", false},
		{"INSERT INTO users VALUES (10, 'Admin');", 10, "Admin", false},
		{"INSERT INTO kv (100, 'Test');", 0, "", true},           // Missing VALUES
		{"INSERT INTO kv VALUES (100);", 0, "", true},            // Missing value
		{"INSERT INTO kv VALUES (100, 200);", 0, "", true},       // Value not string
		{"INSERT INTO kv VALUES ('key', 'value');", 0, "", true}, // Key not number
	}

	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			tokenizer := NewTokenizer(tt.input)
			tokens, err := tokenizer.Tokenize()
			if err != nil {
				if !tt.expectError {
					t.Fatalf("Tokenize failed: %v", err)
				}
				return
			}

			parser := NewParser(tokens)
			stmt, err := parser.Parse()

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			insertStmt, ok := stmt.(*InsertStatement)
			if !ok {
				t.Fatalf("Expected InsertStatement, got %T", stmt)
			}

			if insertStmt.Key != tt.expectedKey {
				t.Errorf("Key: got %d, expected %d", insertStmt.Key, tt.expectedKey)
			}

			if insertStmt.Value != tt.expectedValue {
				t.Errorf("Value: got '%s', expected '%s'", insertStmt.Value, tt.expectedValue)
			}
		})
	}
}
