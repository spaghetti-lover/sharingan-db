package sql

import (
	"fmt"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
)

// Executor executes SQL statements against a B+ Tree
type Executor struct {
	tree *bptree.BPTree
}

// NewExecutor creates a new SQL executor
func NewExecutor(tree *bptree.BPTree) *Executor {
	return &Executor{tree: tree}
}

// Execute executes a SQL statement
func (e *Executor) Execute(stmt Statement) (string, error) {
	switch s := stmt.(type) {
	case *SelectStatement:
		return e.executeSelect(s)
	case *InsertStatement:
		return e.executeInsert(s)
	default:
		return "", fmt.Errorf("unsupported statement type: %T", stmt)
	}
}

// executeSelect executes a SELECT statement
func (e *Executor) executeSelect(stmt *SelectStatement) (string, error) {
	// For now, we only support the "kv" table
	if stmt.Table != "kv" {
		return "", fmt.Errorf("table '%s' not found (only 'kv' is supported)", stmt.Table)
	}

	value, found, err := e.tree.Search(stmt.Key)
	if err != nil {
		return "", fmt.Errorf("search failed: %w", err)
	}

	if !found {
		return "", fmt.Errorf("key %d not found", stmt.Key)
	}

	// Format: key | value
	return fmt.Sprintf("%d | %s", stmt.Key, value), nil
}

// executeInsert executes an INSERT statement
func (e *Executor) executeInsert(stmt *InsertStatement) (string, error) {
	// For now, we only support the "kv" table
	if stmt.Table != "kv" {
		return "", fmt.Errorf("table '%s' not found (only 'kv' is supported)", stmt.Table)
	}

	if err := e.tree.Insert(stmt.Key, stmt.Value); err != nil {
		return "", fmt.Errorf("insert failed: %w", err)
	}

	return "OK", nil
}

// ParseAndExecute is a convenience function that parses and executes SQL
func ParseAndExecute(sql string, tree *bptree.BPTree) (string, error) {
	// Tokenize
	tokenizer := NewTokenizer(sql)
	tokens, err := tokenizer.Tokenize()
	if err != nil {
		return "", fmt.Errorf("tokenizer error: %w", err)
	}

	// Parse
	parser := NewParser(tokens)
	stmt, err := parser.Parse()
	if err != nil {
		return "", fmt.Errorf("parser error: %w", err)
	}

	// Execute
	executor := NewExecutor(tree)
	return executor.Execute(stmt)
}
