package storage

import (
	"fmt"
	"sync"
)

// BufferPool implements an LRU cache for pages
type BufferPool struct {
	capacity int
	cache    map[uint64]*cacheNode
	head     *cacheNode // Most recently used
	tail     *cacheNode // Least recently used
	pager    Pager      // Underlying pager
	mu       sync.RWMutex
	hits     uint64 // Cache hits
	misses   uint64 // Cache misses
	evicts   uint64 // Evictions
}

// cacheNode represents a node in the doubly linked list
type cacheNode struct {
	pageID uint64
	data   []byte
	prev   *cacheNode
	next   *cacheNode
	dirty  bool // Track if page needs to be written back
}

// NewBufferPool creates a new buffer pool
func NewBufferPool(pager Pager, capacity int) *BufferPool {
	if capacity < 1 {
		capacity = 64 // Default capacity
	}

	bp := &BufferPool{
		capacity: capacity,
		cache:    make(map[uint64]*cacheNode, capacity),
		pager:    pager,
	}

	// Initialize dummy head and tail
	bp.head = &cacheNode{}
	bp.tail = &cacheNode{}
	bp.head.next = bp.tail
	bp.tail.prev = bp.head

	return bp
}

// ReadPage reads a page (from cache or disk)
func (bp *BufferPool) ReadPage(id uint64) ([]byte, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check cache first
	if node, exists := bp.cache[id]; exists {
		bp.hits++
		bp.moveToHead(node)
		// Return a copy to prevent external modification
		dataCopy := make([]byte, len(node.data))
		copy(dataCopy, node.data)
		return dataCopy, nil
	}

	// Cache miss - read from disk
	bp.misses++
	data, err := bp.pager.ReadPage(id)
	if err != nil {
		return nil, err
	}

	bp.addToCache(id, data, false)

	// Return a copy
	dataCopy := make([]byte, len(data))
	copy(dataCopy, data)
	return dataCopy, nil
}

// WritePage writes a page (to cache, deferred to disk)
func (bp *BufferPool) WritePage(id uint64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size: %d, expected %d", len(data), PageSize)
	}

	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Check if page is in cache
	if node, exists := bp.cache[id]; exists {
		// Update cached data
		copy(node.data, data)
		node.dirty = true
		bp.moveToHead(node)
		return nil
	}

	// Not in cache - this is a cache miss for writes
	bp.misses++

	// Add to cache
	dataCopy := make([]byte, PageSize)
	copy(dataCopy, data)
	bp.addToCache(id, dataCopy, true)

	return nil
}

// AllocatePage allocates a new page
func (bp *BufferPool) AllocatePage() (uint64, error) {
	// Delegate to underlying pager
	return bp.pager.AllocatePage()
}

// Close flushes all dirty pages and closes underlying pager
func (bp *BufferPool) Close() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// Flush all dirty pages
	for pageID, node := range bp.cache {
		if node.dirty {
			if err := bp.pager.WritePage(pageID, node.data); err != nil {
				return fmt.Errorf("failed to flush page %d: %w", pageID, err)
			}
		}
	}

	return bp.pager.Close()
}

// Flush writes all dirty pages to disk (but doesn't close pager)
func (bp *BufferPool) Flush() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for pageID, node := range bp.cache {
		if node.dirty {
			if err := bp.pager.WritePage(pageID, node.data); err != nil {
				return fmt.Errorf("failed to flush page %d: %w", pageID, err)
			}
			node.dirty = false
		}
	}

	return nil
}

// addToCache adds a page to the cache (evicts LRU if full)
func (bp *BufferPool) addToCache(pageID uint64, data []byte, dirty bool) {
	// Check if we need to evict
	if len(bp.cache) >= bp.capacity {
		bp.evictLRU()
	}

	// Create new node
	dataCopy := make([]byte, PageSize)
	copy(dataCopy, data)

	node := &cacheNode{
		pageID: pageID,
		data:   dataCopy,
		dirty:  dirty,
	}

	// Add to map
	bp.cache[pageID] = node

	// Add to head of list (most recently used)
	bp.addToHead(node)
}

// evictLRU removes the least recently used page
func (bp *BufferPool) evictLRU() {
	// Get tail node (LRU)
	lru := bp.tail.prev
	if lru == bp.head {
		return // Empty list
	}

	// Write dirty page to disk before eviction
	if lru.dirty {
		if err := bp.pager.WritePage(lru.pageID, lru.data); err != nil {
			// Log error but continue (in production, handle this better)
			fmt.Printf("Warning: failed to write page %d during eviction: %v\n", lru.pageID, err)
		}
	}

	// Remove from list
	bp.removeNode(lru)

	// Remove from map
	delete(bp.cache, lru.pageID)

	bp.evicts++
}

// moveToHead moves a node to the head (mark as most recently used)
func (bp *BufferPool) moveToHead(node *cacheNode) {
	bp.removeNode(node)
	bp.addToHead(node)
}

// addToHead adds a node to the head of the list
func (bp *BufferPool) addToHead(node *cacheNode) {
	node.next = bp.head.next
	node.prev = bp.head

	bp.head.next.prev = node
	bp.head.next = node
}

// removeNode removes a node from the list
func (bp *BufferPool) removeNode(node *cacheNode) {
	node.prev.next = node.next
	node.next.prev = node.prev
}

// GetStats returns cache statistics
func (bp *BufferPool) GetStats() BufferPoolStats {
	bp.mu.RLock()
	defer bp.mu.RUnlock()

	hitRate := float64(0)
	if bp.hits+bp.misses > 0 {
		hitRate = float64(bp.hits) / float64(bp.hits+bp.misses)
	}

	return BufferPoolStats{
		Capacity:   bp.capacity,
		Size:       len(bp.cache),
		Hits:       bp.hits,
		Misses:     bp.misses,
		Evictions:  bp.evicts,
		HitRate:    hitRate,
		DirtyPages: bp.countDirtyPages(),
	}
}

// countDirtyPages counts number of dirty pages in cache
func (bp *BufferPool) countDirtyPages() int {
	count := 0
	for _, node := range bp.cache {
		if node.dirty {
			count++
		}
	}
	return count
}

// BufferPoolStats holds cache statistics
type BufferPoolStats struct {
	Capacity   int
	Size       int
	Hits       uint64
	Misses     uint64
	Evictions  uint64
	HitRate    float64
	DirtyPages int
}

// String returns a formatted string of stats
func (s BufferPoolStats) String() string {
	return fmt.Sprintf(
		"BufferPool{Capacity: %d, Size: %d, Hits: %d, Misses: %d, Evictions: %d, HitRate: %.2f%%, DirtyPages: %d}",
		s.Capacity, s.Size, s.Hits, s.Misses, s.Evictions, s.HitRate*100, s.DirtyPages,
	)
}
