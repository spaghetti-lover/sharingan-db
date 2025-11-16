package benchmark

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/storage"
)

// Benchmark100kInserts measures throughput for 100k sequential inserts
func Benchmark100kInserts(b *testing.B) {
	dbFile := "bench_100k_inserts.db"
	walFile := "bench_100k_inserts.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	b.ResetTimer()
	start := time.Now()

	for i := 0; i < 100000; i++ {
		key := uint32(i)
		value := fmt.Sprintf("value-%d", i)
		if err := tree.Insert(key, value); err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}

	duration := time.Since(start)
	b.StopTimer()

	// Calculate metrics
	throughput := float64(100000) / duration.Seconds()

	// Get file sizes
	dbInfo, _ := os.Stat(dbFile)
	walInfo, _ := os.Stat(walFile)

	// Get buffer pool stats
	stats := bufferPool.GetStats()

	b.Logf("\n📊 100k Insert Benchmark Results:")
	b.Logf("   Duration: %v", duration)
	b.Logf("   Throughput: %.2f ops/sec", throughput)
	b.Logf("   Avg latency: %.3f ms/op", duration.Seconds()*1000/100000)
	b.Logf("\n💾 File Sizes:")
	b.Logf("   Database: %.2f MB", float64(dbInfo.Size())/(1024*1024))
	b.Logf("   WAL: %.2f MB", float64(walInfo.Size())/(1024*1024))
	b.Logf("\n📦 Buffer Pool:")
	b.Logf("   Hit Rate: %.2f%%", stats.HitRate*100)
	b.Logf("   Hits: %d", stats.Hits)
	b.Logf("   Misses: %d", stats.Misses)
	b.Logf("   Evictions: %d", stats.Evictions)
}

// Benchmark100kReads measures read throughput after 100k inserts
func Benchmark100kReads(b *testing.B) {
	dbFile := "bench_100k_reads.db"
	walFile := "bench_100k_reads.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	// Prepare data
	b.Log("Preparing 100k keys...")
	for i := 0; i < 100000; i++ {
		key := uint32(i)
		value := fmt.Sprintf("value-%d", i)
		tree.Insert(key, value)
	}

	b.ResetTimer()
	start := time.Now()

	// Benchmark reads
	for i := 0; i < 100000; i++ {
		key := uint32(i)
		_, found, err := tree.Search(key)
		if err != nil {
			b.Fatalf("Search failed: %v", err)
		}
		if !found {
			b.Fatalf("Key %d not found", key)
		}
	}

	duration := time.Since(start)
	b.StopTimer()

	throughput := float64(100000) / duration.Seconds()
	stats := bufferPool.GetStats()

	b.Logf("\n📊 100k Read Benchmark Results:")
	b.Logf("   Duration: %v", duration)
	b.Logf("   Throughput: %.2f ops/sec", throughput)
	b.Logf("   Avg latency: %.3f ms/op", duration.Seconds()*1000/100000)
	b.Logf("\n📦 Buffer Pool:")
	b.Logf("   Hit Rate: %.2f%%", stats.HitRate*100)
	b.Logf("   Cache Hits: %d", stats.Hits)
	b.Logf("   Cache Misses: %d", stats.Misses)
}

// BenchmarkMixedWorkload simulates mixed read/write workload
func BenchmarkMixedWorkload(b *testing.B) {
	dbFile := "bench_mixed.db"
	walFile := "bench_mixed.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	// Prepare initial data
	for i := 0; i < 10000; i++ {
		tree.Insert(uint32(i), fmt.Sprintf("value-%d", i))
	}

	b.ResetTimer()
	start := time.Now()

	// Mixed workload: 70% reads, 30% writes
	operations := 100000
	writes := 0
	reads := 0

	for i := 0; i < operations; i++ {
		if i%10 < 7 {
			// Read operation
			key := uint32(i % 10000)
			tree.Search(key)
			reads++
		} else {
			// Write operation
			key := uint32(10000 + i)
			tree.Insert(key, fmt.Sprintf("new-value-%d", i))
			writes++
		}
	}

	duration := time.Since(start)
	b.StopTimer()

	stats := bufferPool.GetStats()

	b.Logf("\n📊 Mixed Workload Benchmark (100k ops):")
	b.Logf("   Duration: %v", duration)
	b.Logf("   Throughput: %.2f ops/sec", float64(operations)/duration.Seconds())
	b.Logf("   Reads: %d (%.1f%%)", reads, float64(reads)/float64(operations)*100)
	b.Logf("   Writes: %d (%.1f%%)", writes, float64(writes)/float64(operations)*100)
	b.Logf("\n📦 Buffer Pool:")
	b.Logf("   Hit Rate: %.2f%%", stats.HitRate*100)
}

// BenchmarkInOrderTraversal measures traversal performance
func BenchmarkInOrderTraversal(b *testing.B) {
	dbFile := "bench_traversal.db"
	walFile := "bench_traversal.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	// Insert 100k keys
	b.Log("Preparing 100k keys...")
	for i := 0; i < 100000; i++ {
		key := uint32(i)
		value := fmt.Sprintf("value-%d", i)
		tree.Insert(key, value)
	}

	b.ResetTimer()
	start := time.Now()

	keys, err := tree.InOrderTraversal()
	if err != nil {
		b.Fatalf("Traversal failed: %v", err)
	}

	duration := time.Since(start)
	b.StopTimer()

	// Verify correctness
	if len(keys) != 100000 {
		b.Fatalf("Expected 100000 keys, got %d", len(keys))
	}

	// Check sorted order
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			b.Fatalf("Keys not in order at index %d: %d <= %d", i, keys[i], keys[i-1])
		}
	}

	b.Logf("\n📊 In-Order Traversal Benchmark:")
	b.Logf("   Keys: 100,000")
	b.Logf("   Duration: %v", duration)
	b.Logf("   Throughput: %.2f keys/sec", float64(len(keys))/duration.Seconds())
	b.Logf("   ✓ All keys in sorted order")
}

// BenchmarkRandomInserts tests random insert performance
func BenchmarkRandomInserts(b *testing.B) {
	dbFile := "bench_random_inserts.db"
	walFile := "bench_random_inserts.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		b.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 128)
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		b.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	// Generate random keys
	keys := make([]uint32, 100000)
	for i := 0; i < 100000; i++ {
		keys[i] = uint32(i)
	}

	// Shuffle keys (simple Fisher-Yates)
	for i := len(keys) - 1; i > 0; i-- {
		j := i % (i + 1) // Simple pseudo-random
		keys[i], keys[j] = keys[j], keys[i]
	}

	b.ResetTimer()
	start := time.Now()

	for _, key := range keys {
		value := fmt.Sprintf("value-%d", key)
		if err := tree.Insert(key, value); err != nil {
			b.Fatalf("Insert failed: %v", err)
		}
	}

	duration := time.Since(start)
	b.StopTimer()

	stats := bufferPool.GetStats()

	b.Logf("\n📊 Random Insert Benchmark (100k ops):")
	b.Logf("   Duration: %v", duration)
	b.Logf("   Throughput: %.2f ops/sec", float64(100000)/duration.Seconds())
	b.Logf("   Buffer Pool Hit Rate: %.2f%%", stats.HitRate*100)
}

// Test100kCorrectnessWithTraversal verifies data integrity after 100k inserts
func Test100kCorrectnessWithTraversal(t *testing.T) {
	dbFile := "test_100k_correctness.db"
	walFile := "test_100k_correctness.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bufferPool := storage.NewBufferPool(pager, 256) // Larger buffer for test
	defer bufferPool.Close()

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create tree: %v", err)
	}
	defer tree.Close()

	t.Log("Inserting 100k keys...")
	startInsert := time.Now()

	for i := 0; i < 100000; i++ {
		key := uint32(i)
		value := fmt.Sprintf("value-%d", i)
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Insert failed at key %d: %v", key, err)
		}

		if (i+1)%10000 == 0 {
			t.Logf("  Progress: %d/100000", i+1)
		}
	}

	insertDuration := time.Since(startInsert)
	t.Logf("✓ Insert completed in %v (%.2f ops/sec)",
		insertDuration, float64(100000)/insertDuration.Seconds())

	// Verify all keys can be read
	t.Log("\nVerifying all 100k keys...")
	startVerify := time.Now()

	for i := 0; i < 100000; i++ {
		key := uint32(i)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Search failed at key %d: %v", key, err)
		}
		if !found {
			t.Fatalf("Key %d not found", key)
		}
		if value != expectedValue {
			t.Fatalf("Key %d: expected '%s', got '%s'", key, expectedValue, value)
		}

		if (i+1)%10000 == 0 {
			t.Logf("  Progress: %d/100000", i+1)
		}
	}

	verifyDuration := time.Since(startVerify)
	t.Logf("✓ Verification completed in %v (%.2f ops/sec)",
		verifyDuration, float64(100000)/verifyDuration.Seconds())

	// In-order traversal
	t.Log("\nPerforming in-order traversal...")
	startTraversal := time.Now()

	keys, err := tree.InOrderTraversal()
	if err != nil {
		t.Fatalf("Traversal failed: %v", err)
	}

	traversalDuration := time.Since(startTraversal)
	t.Logf("✓ Traversal completed in %v", traversalDuration)

	// Check count
	if len(keys) != 100000 {
		t.Fatalf("Expected 100000 keys, got %d", len(keys))
	}

	// Check sorted order
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Fatalf("Keys not sorted at index %d: %d <= %d", i, keys[i], keys[i-1])
		}
	}

	// Check range
	if keys[0] != 0 {
		t.Fatalf("Min key: expected 0, got %d", keys[0])
	}
	if keys[len(keys)-1] != 99999 {
		t.Fatalf("Max key: expected 99999, got %d", keys[len(keys)-1])
	}

	t.Log("✓ All keys in correct sorted order")

	// Get metrics
	dbInfo, _ := os.Stat(dbFile)
	walInfo, _ := os.Stat(walFile)
	stats := bufferPool.GetStats()

	t.Logf("\n📊 Final Metrics:")
	t.Logf("   Database Size: %.2f MB", float64(dbInfo.Size())/(1024*1024))
	t.Logf("   WAL Size: %.2f MB", float64(walInfo.Size())/(1024*1024))
	t.Logf("   Buffer Pool Hit Rate: %.2f%%", stats.HitRate*100)
	t.Logf("   Cache Hits: %d", stats.Hits)
	t.Logf("   Cache Misses: %d", stats.Misses)
	t.Logf("   Evictions: %d", stats.Evictions)
}

// BenchmarkBufferPoolSizes compares performance with different buffer pool sizes
func BenchmarkBufferPoolSizes(b *testing.B) {
	sizes := []int{32, 64, 128, 256, 512}

	for _, size := range sizes {
		b.Run(fmt.Sprintf("Size%d", size), func(b *testing.B) {
			dbFile := fmt.Sprintf("bench_buffer_%d.db", size)
			walFile := fmt.Sprintf("bench_buffer_%d.wal", size)
			defer os.Remove(dbFile)
			defer os.Remove(walFile)

			pager, _ := storage.NewFilePager(dbFile)
			defer pager.Close()

			bufferPool := storage.NewBufferPool(pager, size)
			defer bufferPool.Close()

			tree, _ := bptree.NewBPTree(bufferPool, 100, walFile)
			defer tree.Close()

			// Insert 10k keys
			for i := 0; i < 10000; i++ {
				tree.Insert(uint32(i), fmt.Sprintf("value-%d", i))
			}

			b.ResetTimer()

			// Benchmark reads
			for i := 0; i < 10000; i++ {
				tree.Search(uint32(i % 10000))
			}

			b.StopTimer()

			stats := bufferPool.GetStats()
			b.Logf("Buffer size %d pages: Hit rate %.2f%%", size, stats.HitRate*100)
		})
	}
}
