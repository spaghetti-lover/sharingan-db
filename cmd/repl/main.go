package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/spaghetti-lover/sharingan-db/internal/bptree"
	"github.com/spaghetti-lover/sharingan-db/internal/sql"
	"github.com/spaghetti-lover/sharingan-db/internal/storage"
)

const (
	dbFile   = "sharingan.db"
	walFile  = "sharingan.wal"
	metaFile = "sharingan.wal.meta"
)

func main() {
	fmt.Println("🔥 Sharingan DB - Interactive Shell")
	fmt.Println("Type 'help' for commands, 'exit' to quit")
	fmt.Println()

	// Initialize database
	tree, pager, bufferPool, err := initDatabase()
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize database: %v\n", err)
		os.Exit(1)
	}
	defer cleanup(tree, pager, bufferPool)

	// Start REPL
	runREPL(tree, bufferPool)
}

// initDatabase initializes or loads existing database
func initDatabase() (*bptree.BPTree, storage.Pager, *storage.BufferPool, error) {
	// Check if database exists
	dbExists := fileExists(dbFile)
	walExists := fileExists(walFile) && fileExists(metaFile)

	if !dbExists {
		fmt.Println("📁 Creating new database...")
		return createFreshDatabase()
	}

	if walExists {
		fmt.Println("⚠️  WAL detected - recovering database...")
		tree, pager, bufferPool, err := recoverFromWAL()
		if err != nil {
			return nil, nil, nil, err
		}
		fmt.Println("✅ Recovery completed")
		return tree, pager, bufferPool, nil
	}

	// Load existing database without recovery
	fmt.Println("📂 Loading existing database...")
	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		return nil, nil, nil, err
	}

	bufferPool := storage.NewBufferPool(pager, 128)

	// Load metadata
	rootPageID, order, err := bptree.LoadMetadata(metaFile)
	if err != nil {
		bufferPool.Close()
		return nil, nil, nil, fmt.Errorf("failed to load metadata: %w", err)
	}

	tree, err := bptree.LoadBPTree(bufferPool, rootPageID, order, walFile)
	if err != nil {
		bufferPool.Close()
		return nil, nil, nil, err
	}

	return tree, pager, bufferPool, nil
}

// createFreshDatabase creates a new database
func createFreshDatabase() (*bptree.BPTree, storage.Pager, *storage.BufferPool, error) {
	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		return nil, nil, nil, err
	}

	bufferPool := storage.NewBufferPool(pager, 128)

	tree, err := bptree.NewBPTree(bufferPool, 100, walFile)
	if err != nil {
		bufferPool.Close()
		return nil, nil, nil, err
	}

	return tree, pager, bufferPool, nil
}

// recoverFromWAL recovers database from WAL
func recoverFromWAL() (*bptree.BPTree, storage.Pager, *storage.BufferPool, error) {
	// Load metadata
	rootPageID, order, err := bptree.LoadMetadata(metaFile)
	if err != nil {
		return nil, nil, nil, err
	}

	// Open pager
	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		return nil, nil, nil, err
	}

	// Create buffer pool
	bufferPool := storage.NewBufferPool(pager, 128)

	// Load tree (will replay WAL automatically)
	tree, err := bptree.LoadBPTree(bufferPool, rootPageID, order, walFile)
	if err != nil {
		bufferPool.Close()
		return nil, nil, nil, err
	}

	return tree, pager, bufferPool, nil
}

// runREPL runs the interactive shell
func runREPL(tree *bptree.BPTree, bufferPool *storage.BufferPool) {
	scanner := bufio.NewScanner(os.Stdin)

	for {
		fmt.Print("db> ")

		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())

		if line == "" {
			continue
		}

		// Check for exit commands
		if line == "exit" || line == "quit" || line == "\\q" {
			fmt.Println("Goodbye! 👋")
			break
		}

		// Check for meta commands (start with .)
		if strings.HasPrefix(line, ".") {
			handleMetaCommand(line, tree, bufferPool)
			continue
		}

		// Check for help
		if line == "help" || line == "\\h" {
			showHelp()
			continue
		}

		// Execute SQL query
		result, err := sql.ParseAndExecute(line, tree)
		if err != nil {
			fmt.Printf("Error: %v\n", err)
			continue
		}

		fmt.Println(result)
	}

	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "Error reading input: %v\n", err)
	}
}

// handleMetaCommand handles meta commands (starting with .)
func handleMetaCommand(cmd string, tree *bptree.BPTree, bufferPool *storage.BufferPool) {
	switch cmd {
	case ".stats", ".statistics":
		showStats(tree, bufferPool)

	case ".help":
		showHelp()

	case ".tree":
		showTreeInfo(tree)

	case ".buffer":
		showBufferPoolStats(bufferPool)

	case ".clear":
		// Clear screen (Unix-like systems)
		fmt.Print("\033[H\033[2J")

	case ".keys":
		showAllKeys(tree)

	default:
		fmt.Printf("Unknown meta command: %s\n", cmd)
		fmt.Println("Type '.help' for available meta commands")
	}
}

// showStats displays database statistics
func showStats(tree *bptree.BPTree, bufferPool *storage.BufferPool) {
	fmt.Println("\n📊 Database Statistics:")
	fmt.Printf("   Root Page: %d\n", tree.GetRootPageID())
	fmt.Printf("   Tree Order: %d\n", tree.GetOrder())
	fmt.Printf("   WAL Syncs: %d\n", tree.GetWALSyncCount())

	stats := bufferPool.GetStats()
	fmt.Printf("\n📦 Buffer Pool:\n")
	fmt.Printf("   Capacity: %d pages\n", stats.Capacity)
	fmt.Printf("   Current Size: %d pages\n", stats.Size)
	fmt.Printf("   Cache Hits: %d\n", stats.Hits)
	fmt.Printf("   Cache Misses: %d\n", stats.Misses)
	fmt.Printf("   Hit Rate: %.2f%%\n", stats.HitRate*100)
	fmt.Printf("   Evictions: %d\n", stats.Evictions)
	fmt.Printf("   Dirty Pages: %d\n", stats.DirtyPages)

	// Get all keys for count
	keys, err := tree.InOrderTraversal()
	if err == nil {
		fmt.Printf("\n📚 Data:\n")
		fmt.Printf("   Total Keys: %d\n", len(keys))
		if len(keys) > 0 {
			fmt.Printf("   Key Range: [%d, %d]\n", keys[0], keys[len(keys)-1])
		}
	}

	// File sizes
	if info, err := os.Stat(dbFile); err == nil {
		fmt.Printf("\n💾 Files:\n")
		fmt.Printf("   Database: %s (%.2f KB)\n", dbFile, float64(info.Size())/1024)
	}
	if info, err := os.Stat(walFile); err == nil {
		fmt.Printf("   WAL: %s (%.2f KB)\n", walFile, float64(info.Size())/1024)
	}

	fmt.Println()
}

// showTreeInfo displays B+ Tree structure info
func showTreeInfo(tree *bptree.BPTree) {
	fmt.Println("\n🌲 B+ Tree Information:")
	fmt.Printf("   Root Page ID: %d\n", tree.GetRootPageID())
	fmt.Printf("   Order (max keys per node): %d\n", tree.GetOrder())
	fmt.Printf("   WAL Syncs: %d\n", tree.GetWALSyncCount())

	keys, err := tree.InOrderTraversal()
	if err != nil {
		fmt.Printf("   Error getting keys: %v\n", err)
		return
	}

	fmt.Printf("   Total Keys: %d\n", len(keys))

	if len(keys) > 0 {
		fmt.Printf("   Min Key: %d\n", keys[0])
		fmt.Printf("   Max Key: %d\n", keys[len(keys)-1])
	}

	fmt.Println()
}

// showBufferPoolStats displays detailed buffer pool statistics
func showBufferPoolStats(bufferPool *storage.BufferPool) {
	stats := bufferPool.GetStats()

	fmt.Println("\n📦 Buffer Pool Statistics:")
	fmt.Printf("   Capacity: %d pages (%.2f KB)\n", stats.Capacity, float64(stats.Capacity*4)/1024)
	fmt.Printf("   Current Size: %d pages\n", stats.Size)
	fmt.Printf("   Utilization: %.2f%%\n", float64(stats.Size)/float64(stats.Capacity)*100)
	fmt.Println()

	fmt.Println("   Performance:")
	fmt.Printf("     Cache Hits: %d\n", stats.Hits)
	fmt.Printf("     Cache Misses: %d\n", stats.Misses)
	fmt.Printf("     Hit Rate: %.2f%%\n", stats.HitRate*100)
	fmt.Printf("     Evictions: %d\n", stats.Evictions)
	fmt.Println()

	fmt.Println("   Memory:")
	fmt.Printf("     Dirty Pages: %d\n", stats.DirtyPages)
	fmt.Printf("     Clean Pages: %d\n", stats.Size-stats.DirtyPages)
	fmt.Println()
}

// showAllKeys displays all keys in the database
func showAllKeys(tree *bptree.BPTree) {
	keys, err := tree.InOrderTraversal()
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}

	if len(keys) == 0 {
		fmt.Println("(empty database)")
		return
	}

	fmt.Printf("\n📋 All Keys (%d total):\n", len(keys))

	// Show first 50 keys
	limit := 50
	if len(keys) < limit {
		limit = len(keys)
	}

	for i := 0; i < limit; i++ {
		if i > 0 && i%10 == 0 {
			fmt.Println()
		}
		fmt.Printf("%d ", keys[i])
	}

	if len(keys) > limit {
		fmt.Printf("\n... (%d more keys)", len(keys)-limit)
	}

	fmt.Println("\n")
}

// showHelp displays available commands
func showHelp() {
	fmt.Println("\n📚 Available Commands:")
	fmt.Println()
	fmt.Println("  SQL Commands:")
	fmt.Println("    INSERT INTO kv VALUES (<key>, '<value>');  - Insert a key-value pair")
	fmt.Println("    SELECT * FROM kv WHERE key = <key>;        - Query by key")
	fmt.Println()
	fmt.Println("  Meta Commands (start with .):")
	fmt.Println("    .stats         - Show database statistics")
	fmt.Println("    .tree          - Show B+ Tree information")
	fmt.Println("    .buffer        - Show buffer pool statistics")
	fmt.Println("    .keys          - List all keys")
	fmt.Println("    .clear         - Clear screen")
	fmt.Println("    .help          - Show this help")
	fmt.Println()
	fmt.Println("  Control Commands:")
	fmt.Println("    help           - Show this help")
	fmt.Println("    exit, quit     - Exit the shell")
	fmt.Println()
}

// cleanup closes all resources
func cleanup(tree *bptree.BPTree, pager storage.Pager, bufferPool *storage.BufferPool) {
	if bufferPool != nil {
		bufferPool.Close()
	}
	if tree != nil {
		tree.Close()
	}
	if pager != nil {
		pager.Close()
	}
}

// fileExists checks if a file exists
func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
