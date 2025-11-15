package query

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/sql"
)

// Query represents a SQL-like query
type Query struct {
	Type  string // "SELECT", "INSERT", etc.
	Key   uint32
	Value string
}

// ParseQuery parses BOTH simple syntax and SQL syntax
// Simple: INSERT 100 Naruto, SELECT 100
// SQL: INSERT INTO kv VALUES (100, 'Naruto'); SELECT * FROM kv WHERE key = 100;
func ParseQuery(input string) (*Query, error) {
	input = strings.TrimSpace(input)

	// Detect SQL syntax (contains keywords like INTO, FROM, WHERE, VALUES)
	if strings.Contains(strings.ToUpper(input), " INTO ") ||
		strings.Contains(strings.ToUpper(input), " FROM ") ||
		strings.Contains(strings.ToUpper(input), " WHERE ") ||
		strings.Contains(strings.ToUpper(input), " VALUES ") {
		return parseSQL(input)
	}

	// Fall back to simple syntax
	return parseSimple(input)
}

// parseSQL parses SQL-standard syntax
func parseSQL(input string) (*Query, error) {
	// Tokenize
	tokenizer := sql.NewTokenizer(input)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return nil, fmt.Errorf("tokenizer error: %w", err)
	}

	// Parse
	parser := sql.NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return nil, fmt.Errorf("parser error: %w", err)
	}

	// Convert to Query
	switch s := stmt.(type) {
	case *sql.SelectStatement:
		return &Query{
			Type: "SELECT",
			Key:  s.Key,
		}, nil

	case *sql.InsertStatement:
		return &Query{
			Type:  "INSERT",
			Key:   s.Key,
			Value: s.Value,
		}, nil

	default:
		return nil, fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// parseSimple parses simple syntax (backward compatibility)
func parseSimple(input string) (*Query, error) {
	parts := strings.Fields(input)

	if len(parts) == 0 {
		return nil, fmt.Errorf("empty query")
	}

	cmd := strings.ToUpper(parts[0])

	switch cmd {
	case "SELECT":
		if len(parts) != 2 {
			return nil, fmt.Errorf("SELECT syntax: SELECT <key>")
		}

		key, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid key: %v", err)
		}

		return &Query{
			Type: "SELECT",
			Key:  uint32(key),
		}, nil

	case "INSERT":
		if len(parts) != 3 {
			return nil, fmt.Errorf("INSERT syntax: INSERT <key> <value>")
		}

		key, err := strconv.ParseUint(parts[1], 10, 32)
		if err != nil {
			return nil, fmt.Errorf("invalid key: %v", err)
		}

		return &Query{
			Type:  "INSERT",
			Key:   uint32(key),
			Value: parts[2],
		}, nil

	default:
		return nil, fmt.Errorf("unsupported command: %s", cmd)
	}
}

// Execute executes a query against the B+ Tree
func Execute(tree *bptree.BPTree, query *Query) (string, error) {
	switch query.Type {
	case "SELECT":
		value, found, err := tree.Search(query.Key)
		if err != nil {
			return "", fmt.Errorf("search failed: %w", err)
		}
		if !found {
			return "", fmt.Errorf("key %d not found", query.Key)
		}
		return value, nil

	case "INSERT":
		if err := tree.Insert(query.Key, query.Value); err != nil {
			return "", fmt.Errorf("insert failed: %w", err)
		}
		return "OK", nil

	default:
		return "", fmt.Errorf("unsupported query type: %s", query.Type)
	}
}

// ExecuteSQL is a convenience function for SQL-standard syntax
func ExecuteSQL(sqlQuery string, tree *bptree.BPTree) (string, error) {
	return sql.ParseAndExecute(sqlQuery, tree)
}
