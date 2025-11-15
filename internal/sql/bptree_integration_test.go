package sql

import (
	"os"
	"testing"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/storage"
)

func TestSQLIntegration(t *testing.T) {
	dbFile := "test_sql_integration.db"
	walFile := "test_sql_integration.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	// Create database
	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	tree, err := bptree.NewBPTree(pager, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}
	defer tree.Close()

	// Test INSERT statements
	insertTests := []string{
		"INSERT INTO kv VALUES (100, 'Naruto');",
		"INSERT INTO kv VALUES (200, 'Sasuke');",
		"INSERT INTO kv VALUES (50, 'Sakura');",
		"INSERT INTO kv VALUES (150, 'Kakashi');",
	}

	t.Log("Testing INSERT statements...")
	for _, sql := range insertTests {
		result, err := ParseAndExecute(sql, tree)
		if err != nil {
			t.Errorf("INSERT failed: %v\n  SQL: %s", err, sql)
		}
		if result != "OK" {
			t.Errorf("Expected 'OK', got '%s'", result)
		}
		t.Logf("✓ %s -> %s", sql, result)
	}

	// Test SELECT statements
	selectTests := []struct {
		sql      string
		expected string
	}{
		{"SELECT * FROM kv WHERE key = 100;", "100 | Naruto"},
		{"SELECT * FROM kv WHERE key = 200;", "200 | Sasuke"},
		{"SELECT * FROM kv WHERE key = 50;", "50 | Sakura"},
		{"SELECT * FROM kv WHERE key = 150;", "150 | Kakashi"},
	}

	t.Log("\nTesting SELECT statements...")
	for _, tt := range selectTests {
		result, err := ParseAndExecute(tt.sql, tree)
		if err != nil {
			t.Errorf("SELECT failed: %v\n  SQL: %s", err, tt.sql)
			continue
		}
		if result != tt.expected {
			t.Errorf("SELECT mismatch:\n  Expected: %s\n  Got: %s", tt.expected, result)
		}
		t.Logf("✓ %s -> %s", tt.sql, result)
	}

	// Test non-existent key
	t.Log("\nTesting non-existent key...")
	_, err = ParseAndExecute("SELECT * FROM kv WHERE key = 999;", tree)
	if err == nil {
		t.Error("Expected error for non-existent key, got none")
	} else {
		t.Logf("✓ Correctly returned error: %v", err)
	}
}

func TestSQLSyntaxErrors(t *testing.T) {
	dbFile := "test_sql_errors.db"
	walFile := "test_sql_errors.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, _ := storage.NewFilePager(dbFile)
	defer pager.Close()

	tree, _ := bptree.NewBPTree(pager, 100, walFile)
	defer tree.Close()

	errorTests := []string{
		"SELECT key FROM kv WHERE key = 100;",     // Wrong SELECT syntax
		"INSERT INTO kv (100, 'test');",           // Missing VALUES
		"SELECT * FROM kv WHERE id = 100;",        // Wrong column
		"INSERT INTO kv VALUES (100);",            // Missing value
		"INVALID SQL;",                            // Invalid command
		"SELECT * FROM kv",                        // Missing WHERE
		"INSERT INTO kv VALUES ('key', 'value');", // Key not number
	}

	for _, sql := range errorTests {
		_, err := ParseAndExecute(sql, tree)
		if err == nil {
			t.Errorf("Expected error for SQL: %s", sql)
		} else {
			t.Logf("✓ Correctly rejected: %s\n  Error: %v", sql, err)
		}
	}
}
