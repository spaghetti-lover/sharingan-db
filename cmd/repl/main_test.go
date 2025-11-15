package main

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/storage"
)

func TestREPLBasicCommands(t *testing.T) {
	// Clean up test files
	testDB := "test_repl.db"
	testWAL := "test_repl.wal"
	defer os.Remove(testDB)
	defer os.Remove(testWAL)

	// Create test database
	pager, err := storage.NewFilePager(testDB)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, testWAL)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	// Test meta commands
	t.Run("MetaCommands", func(t *testing.T) {
		// Capture output
		oldStdout := os.Stdout
		r, w, _ := os.Pipe()
		os.Stdout = w

		// Test .stats command
		handleMetaCommand(".stats", tree, bufferPool)

		w.Close()
		os.Stdout = oldStdout

		var buf bytes.Buffer
		buf.ReadFrom(r)
		output := buf.String()

		if !strings.Contains(output, "Database Statistics") {
			t.Error(".stats command did not produce expected output")
		}
	})
}

func TestREPLFileExists(t *testing.T) {
	// Test fileExists helper
	testFile := "test_exists.tmp"

	// File doesn't exist
	if fileExists(testFile) {
		t.Error("fileExists returned true for non-existent file")
	}

	// Create file
	f, _ := os.Create(testFile)
	f.Close()
	defer os.Remove(testFile)

	// File exists
	if !fileExists(testFile) {
		t.Error("fileExists returned false for existing file")
	}
}

func TestREPLInitialization(t *testing.T) {
	testDB := "test_init.db"
	testWAL := "test_init.wal"
	defer os.Remove(testDB)
	defer os.Remove(testWAL)

	// Temporarily override globals for this test
	oldDBFile := dbFile
	oldWALFile := walFile
	defer func() {
		// Restore (though we can't actually modify consts)
		_ = oldDBFile
		_ = oldWALFile
	}()

	// Test creating fresh database
	tree, pager, bufferPool, err := createFreshDatabase()
	if err != nil {
		t.Fatalf("Failed to create fresh database: %v", err)
	}

	if tree == nil || pager == nil || bufferPool == nil {
		t.Error("createFreshDatabase returned nil values")
	}

	cleanup(tree, pager, bufferPool)
}

// Additional tests for cmd/repl/main_test.go

func TestREPLMetaCommands(t *testing.T) {
	testDB := "test_repl_meta.db"
	testWAL := "test_repl_meta.wal"
	defer os.Remove(testDB)
	defer os.Remove(testWAL)

	pager, err := storage.NewFilePager(testDB)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, testWAL)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	// Insert test data
	for i := 1; i <= 10; i++ {
		tree.Insert(uint32(i), fmt.Sprintf("value-%d", i))
	}

	tests := []struct {
		name     string
		cmd      string
		contains []string
	}{
		{
			name:     "Stats command",
			cmd:      ".stats",
			contains: []string{"Database Statistics", "Buffer Pool", "Total Keys"},
		},
		{
			name:     "Tree command",
			cmd:      ".tree",
			contains: []string{"B+ Tree Information", "Root Page ID", "Total Keys"},
		},
		{
			name:     "Buffer command",
			cmd:      ".buffer",
			contains: []string{"Buffer Pool Statistics", "Cache Hits", "Hit Rate"},
		},
		{
			name:     "Keys command",
			cmd:      ".keys",
			contains: []string{"All Keys", "10 total"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			oldStdout := os.Stdout
			r, w, _ := os.Pipe()
			os.Stdout = w

			handleMetaCommand(tt.cmd, tree, bufferPool)

			w.Close()
			os.Stdout = oldStdout

			var buf bytes.Buffer
			buf.ReadFrom(r)
			output := buf.String()

			for _, expected := range tt.contains {
				if !strings.Contains(output, expected) {
					t.Errorf("Output missing expected string '%s'\nOutput: %s", expected, output)
				}
			}
		})
	}
}

func TestREPLHelpCommand(t *testing.T) {
	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	showHelp()

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	expectedStrings := []string{
		"Available Commands",
		"SQL Commands",
		"INSERT INTO",
		"SELECT *",
		"Meta Commands",
		".stats",
		".help",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(output, expected) {
			t.Errorf("Help output missing: %s", expected)
		}
	}
}

func TestREPLUnknownMetaCommand(t *testing.T) {
	testDB := "test_unknown_meta.db"
	testWAL := "test_unknown_meta.wal"
	defer os.Remove(testDB)
	defer os.Remove(testWAL)

	pager, _ := storage.NewFilePager(testDB)
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, _ := bptree.NewBPTree(bufferPool, 100, testWAL)
	defer tree.Close()

	oldStdout := os.Stdout
	r, w, _ := os.Pipe()
	os.Stdout = w

	handleMetaCommand(".unknown", tree, bufferPool)

	w.Close()
	os.Stdout = oldStdout

	var buf bytes.Buffer
	buf.ReadFrom(r)
	output := buf.String()

	if !strings.Contains(output, "Unknown meta command") {
		t.Error("Expected 'Unknown meta command' message")
	}
}
