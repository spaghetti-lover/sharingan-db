package query

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/storage"
)

func TestParseQuery(t *testing.T) {
	tests := []struct {
		sql         string
		expectedKey uint32
		expectedVal string
		expectError bool
	}{
		{"SELECT 100", 100, "", false},
		{"INSERT 200 naruto", 200, "naruto", false},
		{"select 50", 50, "", false},
		{"insert 75 sasuke", 75, "sasuke", false},
		{"INVALID", 0, "", true},
		{"SELECT", 0, "", true},
		{"INSERT 100", 0, "", true},
	}

	for _, tt := range tests {
		query, err := ParseQuery(tt.sql)

		if tt.expectError {
			if err == nil {
				t.Errorf("ParseQuery(%q): expected error, got none", tt.sql)
			}
			continue
		}

		if err != nil {
			t.Errorf("ParseQuery(%q): unexpected error: %v", tt.sql, err)
			continue
		}

		if query.Key != tt.expectedKey {
			t.Errorf("ParseQuery(%q): key=%d, expected %d", tt.sql, query.Key, tt.expectedKey)
		}

		if query.Value != tt.expectedVal {
			t.Errorf("ParseQuery(%q): value=%s, expected %s", tt.sql, query.Value, tt.expectedVal)
		}
	}
}

func TestExecuteQueries(t *testing.T) {
	dbFile := "test_execute.db"
	walFile := "test_execute.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

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

	// Test INSERT queries
	insertQueries := []string{
		"INSERT 100 naruto",
		"INSERT 200 sasuke",
		"INSERT 50 sakura",
		"INSERT 150 kakashi",
	}

	for _, sql := range insertQueries {
		query, err := ParseQuery(sql)
		if err != nil {
			t.Fatalf("ParseQuery failed: %v", err)
		}

		result, err := Execute(tree, query)
		if err != nil {
			t.Errorf("Execute(%q) failed: %v", sql, err)
		}
		if result != "OK" {
			t.Errorf("Execute(%q): result=%s, expected OK", sql, result)
		}
	}

	t.Logf("✓ Inserted %d records", len(insertQueries))

	// Test SELECT queries
	selectTests := []struct {
		sql           string
		expectedValue string
	}{
		{"SELECT 100", "naruto"},
		{"SELECT 200", "sasuke"},
		{"SELECT 50", "sakura"},
		{"SELECT 150", "kakashi"},
	}

	for _, tt := range selectTests {
		query, err := ParseQuery(tt.sql)
		if err != nil {
			t.Fatalf("ParseQuery failed: %v", err)
		}

		result, err := Execute(tree, query)
		if err != nil {
			t.Errorf("Execute(%q) failed: %v", tt.sql, err)
			continue
		}

		if result != tt.expectedValue {
			t.Errorf("Execute(%q): result=%s, expected %s", tt.sql, result, tt.expectedValue)
		}
	}

	t.Log("✓ All SELECT queries successful")

	// Test SELECT with non-existent key
	query, _ := ParseQuery("SELECT 999")
	_, err = Execute(tree, query)
	if err == nil {
		t.Error("SELECT 999: expected error, got none")
	}
}

// TestSelect1000Operations tests 1000 SELECT operations with performance metrics
func TestSelect1000Operations(t *testing.T) {
	dbFile := "test_select_1000.db"
	walFile := "test_select_1000.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

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

	// Insert 1000 records
	numRecords := 1000
	t.Logf("Preparing: inserting %d records...", numRecords)

	insertStart := time.Now()
	for i := 1; i <= numRecords; i++ {
		key := uint32(i)
		value := fmt.Sprintf("value-%d", i)

		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Failed to insert key=%d: %v", key, err)
		}
	}
	insertDuration := time.Since(insertStart)

	t.Logf("✓ Inserted %d records in %v", numRecords, insertDuration)
	t.Logf("  Insert throughput: %.2f ops/sec", float64(numRecords)/insertDuration.Seconds())
	t.Logf("  WAL syncs: %d", tree.GetWALSyncCount())

	// Perform 1000 SELECT operations
	t.Logf("\nPerforming %d SELECT operations...", numRecords)
	selectStart := time.Now()

	successCount := 0
	for i := 1; i <= numRecords; i++ {
		sql := fmt.Sprintf("SELECT %d", i)
		query, err := ParseQuery(sql)
		if err != nil {
			t.Fatalf("ParseQuery failed: %v", err)
		}

		result, err := Execute(tree, query)
		if err != nil {
			t.Errorf("Execute(%q) failed: %v", sql, err)
			continue
		}

		expectedValue := fmt.Sprintf("value-%d", i)
		if result != expectedValue {
			t.Errorf("SELECT %d: result=%s, expected %s", i, result, expectedValue)
			continue
		}

		successCount++
	}

	selectDuration := time.Since(selectStart)

	t.Logf("\n✓ Completed %d SELECT operations in %v", successCount, selectDuration)
	t.Logf("  SELECT throughput: %.2f ops/sec", float64(successCount)/selectDuration.Seconds())
	t.Logf("  Average latency: %.2f µs/op", float64(selectDuration.Microseconds())/float64(successCount))

	if successCount != numRecords {
		t.Errorf("Only %d/%d SELECT operations successful", successCount, numRecords)
	}

	// Performance assertions
	avgLatencyUs := float64(selectDuration.Microseconds()) / float64(successCount)
	if avgLatencyUs > 1000 { // Should be < 1ms per operation
		t.Errorf("Performance degraded: average latency %.2f µs (expected < 1000 µs)", avgLatencyUs)
	}

	t.Logf("\n📊 Performance Summary:")
	t.Logf("  Tree order: %d", tree.GetOrder())
	t.Logf("  Root page ID: %d", tree.GetRootPageID())
	t.Logf("  Insert throughput: %.2f ops/sec", float64(numRecords)/insertDuration.Seconds())
	t.Logf("  SELECT throughput: %.2f ops/sec", float64(successCount)/selectDuration.Seconds())
	t.Logf("  SELECT avg latency: %.2f µs", avgLatencyUs)
	t.Logf("  Total WAL syncs: %d", tree.GetWALSyncCount())
}

// BenchmarkSelectOperations benchmarks SELECT performance
func BenchmarkSelectOperations(b *testing.B) {
	dbFile := "bench_select.db"
	walFile := "bench_select.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, _ := storage.NewFilePager(dbFile)
	defer pager.Close()

	tree, _ := bptree.NewBPTree(pager, 100, walFile)
	defer tree.Close()

	// Prepare data
	for i := 1; i <= 1000; i++ {
		tree.Insert(uint32(i), fmt.Sprintf("value-%d", i))
	}

	b.ResetTimer()

	// Benchmark SELECT operations
	for i := 0; i < b.N; i++ {
		key := uint32((i % 1000) + 1)
		sql := fmt.Sprintf("SELECT %d", key)
		query, _ := ParseQuery(sql)
		Execute(tree, query)
	}
}

// BenchmarkSelectWithSplits benchmarks SELECT on a tree with multiple splits
func BenchmarkSelectWithSplits(b *testing.B) {
	dbFile := "bench_select_splits.db"
	walFile := "bench_select_splits.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, _ := storage.NewFilePager(dbFile)
	defer pager.Close()

	tree, _ := bptree.NewBPTree(pager, 100, walFile)
	defer tree.Close()

	// Insert enough data to trigger multiple splits
	numRecords := 5000
	for i := 1; i <= numRecords; i++ {
		tree.Insert(uint32(i), fmt.Sprintf("value-%d", i))
	}

	b.Logf("Tree prepared with %d records, root page: %d", numRecords, tree.GetRootPageID())
	b.Logf("Total WAL syncs: %d", tree.GetWALSyncCount())
	b.ResetTimer()

	// Benchmark SELECT operations
	for i := 0; i < b.N; i++ {
		key := uint32((i % numRecords) + 1)
		sql := fmt.Sprintf("SELECT %d", key)
		query, _ := ParseQuery(sql)
		Execute(tree, query)
	}
}

// BenchmarkInsertOperations benchmarks INSERT performance with WAL
func BenchmarkInsertOperations(b *testing.B) {
	dbFile := "bench_insert.db"
	walFile := "bench_insert.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, _ := storage.NewFilePager(dbFile)
	defer pager.Close()

	tree, _ := bptree.NewBPTree(pager, 100, walFile)
	defer tree.Close()

	b.ResetTimer()

	// Benchmark INSERT operations
	for i := 0; i < b.N; i++ {
		key := uint32(i + 1)
		sql := fmt.Sprintf("INSERT %d value-%d", key, i)
		query, _ := ParseQuery(sql)
		Execute(tree, query)
	}

	b.StopTimer()
	b.Logf("Total WAL syncs: %d", tree.GetWALSyncCount())
	b.Logf("Final root page: %d", tree.GetRootPageID())
}

// TestQueryWithWALRecovery tests query execution after WAL recovery
func TestQueryWithWALRecovery(t *testing.T) {
	dbFile := "test_query_recovery.db"
	walFile := "test_query_recovery.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	var rootPageID uint64

	// Phase 1: Insert data
	{
		pager, err := storage.NewFilePager(dbFile)
		if err != nil {
			t.Fatalf("Failed to create pager: %v", err)
		}

		tree, err := bptree.NewBPTree(pager, 100, walFile)
		if err != nil {
			t.Fatalf("Failed to create B+ Tree: %v", err)
		}

		rootPageID = tree.GetRootPageID()

		insertQueries := []string{
			"INSERT 100 naruto",
			"INSERT 200 sasuke",
			"INSERT 50 sakura",
		}

		for _, sql := range insertQueries {
			query, _ := ParseQuery(sql)
			if _, err := Execute(tree, query); err != nil {
				t.Fatalf("Execute failed: %v", err)
			}
		}

		t.Logf("✓ Inserted %d records", len(insertQueries))

		// Simulate crash - don't close tree properly
		pager.Close()
	}

	// Phase 2: Recover and query
	{
		pager2, err := storage.NewFilePager(dbFile)
		if err != nil {
			t.Fatalf("Failed to reopen pager: %v", err)
		}
		defer pager2.Close()

		tree2, err := bptree.LoadBPTree(pager2, rootPageID, 100, walFile)
		if err != nil {
			t.Fatalf("Failed to load tree: %v", err)
		}
		defer tree2.Close()

		// Verify data via queries
		selectTests := []struct {
			sql           string
			expectedValue string
		}{
			{"SELECT 100", "naruto"},
			{"SELECT 200", "sasuke"},
			{"SELECT 50", "sakura"},
		}

		for _, tt := range selectTests {
			query, _ := ParseQuery(tt.sql)
			result, err := Execute(tree2, query)
			if err != nil {
				t.Errorf("Execute(%q) failed after recovery: %v", tt.sql, err)
			}
			if result != tt.expectedValue {
				t.Errorf("Execute(%q): result=%s, expected %s", tt.sql, result, tt.expectedValue)
			}
		}

		t.Log("✓ All queries successful after WAL recovery")
	}
}

func TestParseQueryBackwardCompatibility(t *testing.T) {
	tests := []struct {
		name        string
		input       string
		expectedKey uint32
		expectedVal string
		expectError bool
	}{
		// Simple syntax (backward compatible)
		{"Simple SELECT", "SELECT 100", 100, "", false},
		{"Simple INSERT", "INSERT 200 naruto", 200, "naruto", false},

		// SQL syntax
		{"SQL SELECT", "SELECT * FROM kv WHERE key = 100;", 100, "", false},
		{"SQL INSERT", "INSERT INTO kv VALUES (200, 'naruto');", 200, "naruto", false},

		// Errors
		{"Invalid SQL", "INVALID QUERY", 0, "", true},
		{"Empty", "", 0, "", true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			query, err := ParseQuery(tt.input)

			if tt.expectError {
				if err == nil {
					t.Errorf("Expected error, got none")
				}
				return
			}

			if err != nil {
				t.Fatalf("Parse failed: %v", err)
			}

			if query.Key != tt.expectedKey {
				t.Errorf("Key: got %d, expected %d", query.Key, tt.expectedKey)
			}

			if query.Value != tt.expectedVal {
				t.Errorf("Value: got '%s', expected '%s'", query.Value, tt.expectedVal)
			}
		})
	}
}
