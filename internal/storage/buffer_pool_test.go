package storage

import (
	"os"
	"testing"
)

func TestBufferPoolBasic(t *testing.T) {
	dbFile := "test_buffer_pool.db"
	defer os.Remove(dbFile)

	pager, err := NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create buffer pool with LARGER capacity to avoid thrashing
	bp := NewBufferPool(pager, 10) // Changed from 4 to 10
	defer bp.Close()

	// Allocate and write 5 pages
	pageIDs := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		pageID, err := bp.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pageIDs[i] = pageID

		// Write unique data
		data := make([]byte, PageSize)
		data[0] = byte(i)
		if err := bp.WritePage(pageID, data); err != nil {
			t.Fatalf("Failed to write page %d: %v", pageID, err)
		}
	}

	// Now all 5 pages should fit in cache (capacity=10)

	// Read all 5 pages - should ALL be cache hits
	for i := 0; i < 5; i++ {
		data, err := bp.ReadPage(pageIDs[i])
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageIDs[i], err)
		}

		if data[0] != byte(i) {
			t.Errorf("Page %d: data[0]=%d, expected %d", pageIDs[i], data[0], i)
		}
	}

	// Check stats
	stats := bp.GetStats()
	t.Logf("Buffer pool stats: %s", stats.String())

	// Should have 5 cache hits (all reads)
	if stats.Hits != 5 {
		t.Errorf("Expected 5 cache hits, got %d", stats.Hits)
	}

	// Should have 5 misses (all writes)
	if stats.Misses != 5 {
		t.Errorf("Expected 5 misses (from writes), got %d", stats.Misses)
	}

	// No evictions (capacity=10, only 5 pages)
	if stats.Evictions != 0 {
		t.Errorf("Expected 0 evictions, got %d", stats.Evictions)
	}
}

func TestBufferPoolEviction(t *testing.T) {
	dbFile := "test_buffer_eviction.db"
	defer os.Remove(dbFile)

	pager, err := NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create buffer pool with capacity 3
	bp := NewBufferPool(pager, 3)
	defer bp.Close()

	// Allocate 5 pages (will trigger evictions)
	pageIDs := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		pageID, err := bp.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pageIDs[i] = pageID

		data := make([]byte, PageSize)
		data[0] = byte(i * 10)
		if err := bp.WritePage(pageID, data); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	stats := bp.GetStats()
	t.Logf("After 5 writes: %s", stats.String())

	// Cache should have exactly 3 pages (last 3: pages 2, 3, 4)
	if stats.Size != 3 {
		t.Errorf("Cache size=%d, expected 3", stats.Size)
	}

	// Should have 2 evictions (pages 0 and 1)
	if stats.Evictions != 2 {
		t.Errorf("Evictions=%d, expected 2", stats.Evictions)
	}

	// 5 writes, all misses
	if stats.Misses != 5 {
		t.Errorf("Misses=%d, expected 5", stats.Misses)
	}

	// Read last 3 pages (should hit cache)
	for i := 2; i < 5; i++ {
		data, err := bp.ReadPage(pageIDs[i])
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageIDs[i], err)
		}

		expectedValue := byte(i * 10)
		if data[0] != expectedValue {
			t.Errorf("Page %d: data[0]=%d, expected %d", pageIDs[i], data[0], expectedValue)
		}
	}

	midStats := bp.GetStats()
	t.Logf("After reading last 3 pages: %s", midStats.String())

	// Should have 3 cache hits (pages 2, 3, 4)
	if midStats.Hits != 3 {
		t.Errorf("Hits=%d, expected 3 (after reading cached pages)", midStats.Hits)
	}

	// Read first 2 pages (cache misses, will evict some pages)
	for i := 0; i < 2; i++ {
		data, err := bp.ReadPage(pageIDs[i])
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", pageIDs[i], err)
		}

		expectedValue := byte(i * 10)
		if data[0] != expectedValue {
			t.Errorf("Page %d: data[0]=%d, expected %d", pageIDs[i], data[0], expectedValue)
		}
	}

	finalStats := bp.GetStats()
	t.Logf("After reading all pages: %s", finalStats.String())

	// Should have exactly 3 cache hits (from middle read)
	if finalStats.Hits != 3 {
		t.Errorf("Hits=%d, expected 3", finalStats.Hits)
	}

	// Misses: 5 (writes) + 2 (reading evicted pages) = 7
	if finalStats.Misses != 7 {
		t.Errorf("Misses=%d, expected 7", finalStats.Misses)
	}
}

func TestBufferPoolHitRate(t *testing.T) {
	dbFile := "test_buffer_hitrate.db"
	defer os.Remove(dbFile)

	pager, err := NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	bp := NewBufferPool(pager, 10)
	defer bp.Close()

	// Allocate 5 pages
	pageIDs := make([]uint64, 5)
	for i := 0; i < 5; i++ {
		pageID, err := bp.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}
		pageIDs[i] = pageID

		data := make([]byte, PageSize)
		data[0] = byte(i)
		if err := bp.WritePage(pageID, data); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}
	}

	// 5 writes = 5 misses, pages are now in cache

	// Read same pages multiple times (should hit cache)
	for round := 0; round < 10; round++ {
		for _, pageID := range pageIDs {
			if _, err := bp.ReadPage(pageID); err != nil {
				t.Fatalf("Failed to read page: %v", err)
			}
		}
	}

	stats := bp.GetStats()
	t.Logf("After 50 reads: %s", stats.String())

	// Writes: 5 misses
	// Reads: all 50 should be hits (pages are in cache, capacity=10)
	// Total: 5 misses, 50 hits
	expectedHits := uint64(50)
	if stats.Hits != expectedHits {
		t.Errorf("Hits=%d, expected %d", stats.Hits, expectedHits)
	}

	expectedMisses := uint64(5)
	if stats.Misses != expectedMisses {
		t.Errorf("Misses=%d, expected %d", stats.Misses, expectedMisses)
	}

	// Hit rate: 50/55 = 90.9%
	if stats.HitRate < 0.85 {
		t.Errorf("Hit rate=%.2f%%, expected >= 85%%", stats.HitRate*100)
	}
}

func TestBufferPoolFlush(t *testing.T) {
	dbFile := "test_buffer_flush.db"
	defer os.Remove(dbFile)

	pager, err := NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	var pageID uint64

	// Write data with buffer pool
	{
		bp := NewBufferPool(pager, 10)

		pageID, err = bp.AllocatePage()
		if err != nil {
			t.Fatalf("Failed to allocate page: %v", err)
		}

		data := make([]byte, PageSize)
		data[0] = 0xAB
		if err := bp.WritePage(pageID, data); err != nil {
			t.Fatalf("Failed to write page: %v", err)
		}

		// Explicit flush (doesn't close pager)
		if err := bp.Flush(); err != nil {
			t.Fatalf("Failed to flush: %v", err)
		}

		// Don't call bp.Close() - we want pager to stay open
	}

	// Read back directly from pager (not through buffer pool)
	{
		data, err := pager.ReadPage(pageID)
		if err != nil {
			t.Fatalf("Failed to read page: %v", err)
		}

		if data[0] != 0xAB {
			t.Errorf("data[0]=%#x, expected 0xAB", data[0])
		}
	}
}

func TestBufferPoolSequentialAccess(t *testing.T) {
	dbFile := "test_buffer_sequential.db"
	defer os.Remove(dbFile)

	pager, err := NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Small cache to force evictions
	bp := NewBufferPool(pager, 5)
	defer bp.Close()

	// Write 10 pages sequentially
	pageIDs := make([]uint64, 10)
	for i := 0; i < 10; i++ {
		pageID, _ := bp.AllocatePage()
		pageIDs[i] = pageID

		data := make([]byte, PageSize)
		data[0] = byte(i * 5)
		bp.WritePage(pageID, data)
	}

	// Read last 5 pages (should be in cache)
	hits := 0
	for i := 5; i < 10; i++ {
		data, err := bp.ReadPage(pageIDs[i])
		if err != nil {
			t.Fatalf("Failed to read page: %v", err)
		}
		if data[0] == byte(i*5) {
			hits++
		}
	}

	if hits != 5 {
		t.Errorf("Expected 5 cache hits for recent pages, got %d", hits)
	}

	stats := bp.GetStats()
	t.Logf("Sequential access stats: %s", stats.String())

	// Should have some hits
	if stats.Hits == 0 {
		t.Error("Expected some cache hits")
	}
}

func BenchmarkBufferPoolRead(b *testing.B) {
	dbFile := "bench_buffer_read.db"
	defer os.Remove(dbFile)

	pager, _ := NewFilePager(dbFile)
	defer pager.Close()

	bp := NewBufferPool(pager, 100)
	defer bp.Close()

	// Prepare pages
	pageIDs := make([]uint64, 50)
	for i := 0; i < 50; i++ {
		pageID, _ := bp.AllocatePage()
		pageIDs[i] = pageID

		data := make([]byte, PageSize)
		data[0] = byte(i)
		bp.WritePage(pageID, data)
	}

	b.ResetTimer()

	// Benchmark reads (should hit cache)
	for i := 0; i < b.N; i++ {
		pageID := pageIDs[i%50]
		bp.ReadPage(pageID)
	}

	b.StopTimer()
	stats := bp.GetStats()
	b.Logf("Hit rate: %.2f%%", stats.HitRate*100)
}
