package database

import (
	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/storage"
	"github.com/spaghetti-lover/sharingan-db/pkg/query"
)

type Database struct {
	tree       *bptree.BPTree
	pager      storage.Pager
	bufferPool *storage.BufferPool
}

// Open opens or creates a database
func Open(path string) (*Database, error) {
	pager, err := storage.NewFilePager(path + ".db")
	if err != nil {
		return nil, err
	}

	bufferPool := storage.NewBufferPool(pager, 128)

	tree, err := bptree.NewBPTree(bufferPool, 100, path+".wal")
	if err != nil {
		bufferPool.Close()
		pager.Close()
		return nil, err
	}

	return &Database{
		tree:       tree,
		pager:      pager,
		bufferPool: bufferPool,
	}, nil
}

// Close closes the database
func (db *Database) Close() error {
	if db.bufferPool != nil {
		db.bufferPool.Close()
	}
	if db.tree != nil {
		db.tree.Close()
	}
	if db.pager != nil {
		return db.pager.Close()
	}
	return nil
}

// Put inserts a key-value pair
func (db *Database) Put(key uint32, value string) error {
	return db.tree.Insert(key, value)
}

// Get retrieves a value by key
func (db *Database) Get(key uint32) (string, bool, error) {
	return db.tree.Search(key)
}

// Query executes SQL query
func (db *Database) Query(sql string) (string, error) {
	return query.ExecuteSQL(sql, db.tree)
}

// Keys returns all keys in sorted order
func (db *Database) Keys() ([]uint32, error) {
	return db.tree.InOrderTraversal()
}

// Stats returns database statistics
func (db *Database) Stats() *Stats {
	poolStats := db.bufferPool.GetStats()
	keys, _ := db.tree.InOrderTraversal()

	return &Stats{
		TotalKeys:      len(keys),
		RootPageID:     db.tree.GetRootPageID(),
		TreeOrder:      db.tree.GetOrder(),
		CacheHitRate:   poolStats.HitRate,
		BufferPoolSize: poolStats.Size,
	}
}

type Stats struct {
	TotalKeys      int
	RootPageID     uint64
	TreeOrder      int
	CacheHitRate   float64
	BufferPoolSize int
}
