package bptree

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"

	"github.com/spaghetti-lover/sharingan-db/internal/storage"
	"github.com/spaghetti-lover/sharingan-db/internal/wal"
)

// BPTree represents a B+ Tree index
type BPTree struct {
	pager    storage.Pager
	rootPage uint64
	order    int // Maximum number of keys per node
	wal      *wal.WAL
}

// NewBPTree creates a new B+ Tree
func NewBPTree(pager storage.Pager, order int, walPath string) (*BPTree, error) {
	rootPageID, rootPage, err := allocatePageWithType(pager, storage.PageTypeLeaf)
	if err != nil {
		return nil, fmt.Errorf("failed to allocate root page: %w", err)
	}

	if err := writePageStruct(pager, rootPageID, rootPage); err != nil {
		return nil, fmt.Errorf("failed to write root page: %w", err)
	}

	// Open WAL
	walFile, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to create WAL: %w", err)
	}

	// Create tree instance FIRST
	tree := &BPTree{
		pager:    pager,
		rootPage: rootPageID,
		order:    order,
		wal:      walFile,
	}

	// Save metadata for recovery
	if err := tree.SaveMetadata(walPath + ".meta"); err != nil {
		walFile.Close() // Clean up WAL if metadata save fails
		return nil, fmt.Errorf("failed to save metadata: %w", err)
	}

	return tree, nil
}

// LoadBPTree loads an existing B+ Tree from disk
func LoadBPTree(pager storage.Pager, rootPageID uint64, order int, walPath string) (*BPTree, error) {
	// Open WAL
	walFile, err := wal.NewWAL(walPath)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL: %w", err)
	}

	tree := &BPTree{
		pager:    pager,
		rootPage: rootPageID,
		order:    order,
		wal:      walFile,
	}

	// Replay WAL entries
	if err := tree.replayWAL(); err != nil {
		walFile.Close() // Clean up WAL if replay fails
		return nil, fmt.Errorf("failed to replay WAL: %w", err)
	}

	return tree, nil
}

// Insert inserts a key-value pair into the B+ Tree
func (tree *BPTree) Insert(key uint32, value string) error {
	walEntry := &wal.Entry{
		OpType: wal.OpInsert,
		Key:    key,
		Value:  value,
	}

	if err := tree.wal.Append(walEntry); err != nil {
		return fmt.Errorf("failed to write WAL: %w", err)
	}

	record := storage.NewRecordFromInts(key, value)

	// Load root page
	rootPage, err := readPageStruct(tree.pager, tree.rootPage)
	if err != nil {
		return fmt.Errorf("failed to load root page: %w", err)
	}

	// If root is leaf, handle insertion (possibly splitting)
	if rootPage.IsLeaf() {
		newChildKey, newChildPageID, err := tree.insertIntoLeafWithSplit(tree.rootPage, rootPage, record)
		if err != nil {
			return err
		}

		// If split occurred, create new root
		if newChildPageID != 0 {
			return tree.createNewRoot(tree.rootPage, newChildKey, newChildPageID)
		}

		return nil
	}

	// Root is internal, navigate and insert
	return tree.insertNonLeafRoot(key, record)
}

// replayWAL replays all WAL entries to restore state
func (tree *BPTree) replayWAL() error {
	entries, err := tree.wal.ReadAll()
	if err != nil {
		return fmt.Errorf("failed to read WAL: %w", err)
	}

	if len(entries) == 0 {
		return nil // Nothing to replay
	}

	fmt.Printf("🔄 Replaying %d WAL entries...\n", len(entries))

	for i, entry := range entries {
		switch entry.OpType {
		case wal.OpInsert:
			// Apply insert directly to tree (without writing to WAL again)
			record := storage.NewRecordFromInts(entry.Key, entry.Value)
			if err := tree.insertWithoutWAL(record); err != nil {
				return fmt.Errorf("failed to replay insert at entry %d: %w", i, err)
			}
		default:
			return fmt.Errorf("unsupported WAL operation: %d", entry.OpType)
		}
	}

	fmt.Printf("✓ WAL replay complete\n")

	// Clear WAL after successful replay
	return tree.wal.Truncate()
}

// insertWithoutWAL inserts without writing to WAL (used during replay)
func (tree *BPTree) insertWithoutWAL(record *storage.Record) error {
	key, _ := record.GetKeyAsUint32()

	rootPage, err := readPageStruct(tree.pager, tree.rootPage)
	if err != nil {
		return fmt.Errorf("failed to load root page: %w", err)
	}

	if rootPage.IsLeaf() {
		newChildKey, newChildPageID, err := tree.insertIntoLeafWithSplit(tree.rootPage, rootPage, record)
		if err != nil {
			return err
		}

		if newChildPageID != 0 {
			return tree.createNewRoot(tree.rootPage, newChildKey, newChildPageID)
		}

		return nil
	}

	return tree.insertNonLeafRoot(key, record)
}

// Close closes the B+ Tree and WAL
func (tree *BPTree) Close() error {
	if tree.wal != nil {
		if err := tree.wal.Close(); err != nil {
			return fmt.Errorf("failed to close WAL: %w", err)
		}
	}
	return nil
}

// GetWALSyncCount returns number of WAL syncs
func (tree *BPTree) GetWALSyncCount() int {
	if tree.wal == nil {
		return 0
	}
	return tree.wal.GetSyncCount()
}

// insertNonLeafRoot handles insertion when root is internal
func (tree *BPTree) insertNonLeafRoot(key uint32, record *storage.Record) error {
	// Find leaf page
	leafPageID, err := tree.findLeafPage(key)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	leafPage, err := readPageStruct(tree.pager, leafPageID)
	if err != nil {
		return fmt.Errorf("failed to load leaf page: %w", err)
	}

	// Insert into leaf (possibly splitting)
	newChildKey, newChildPageID, err := tree.insertIntoLeafWithSplit(leafPageID, leafPage, record)
	if err != nil {
		return err
	}

	// If split occurred, insert promoted key into parent
	if newChildPageID != 0 {
		return tree.insertIntoParent(leafPageID, newChildKey, newChildPageID)
	}

	return nil
}

// insertIntoLeafWithSplit inserts record into leaf, splitting if necessary
// Returns (promotedKey, newPageID, error)
// If no split: returns (0, 0, nil)
func (tree *BPTree) insertIntoLeafWithSplit(pageID uint64, page *storage.Page, record *storage.Record) (uint32, uint64, error) {
	leaf := storage.NewLeafPage(page)

	// Try simple insert
	err := leaf.InsertRecord(record)
	if err == nil {
		// Success without split
		return 0, 0, writePageStruct(tree.pager, pageID, page)
	}

	// Page is full, need to split
	return tree.splitLeaf(pageID, page, record)
}

// splitLeaf splits a full leaf page
// Returns (promotedKey, newPageID, error)
func (tree *BPTree) splitLeaf(oldPageID uint64, oldPage *storage.Page, newRecord *storage.Record) (uint32, uint64, error) {
	oldLeaf := storage.NewLeafPage(oldPage)

	// Get all existing records + new record
	allRecords, err := oldLeaf.GetAllRecords()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get records: %w", err)
	}
	allRecords = append(allRecords, newRecord)

	// Sort records by key
	sortRecordsByKey(allRecords)

	// Find split point (middle)
	splitIndex := len(allRecords) / 2

	// Create new right leaf
	newPageID, newPage, err := allocatePageWithType(tree.pager, storage.PageTypeLeaf)
	if err != nil {
		return 0, 0, fmt.Errorf("failed to allocate new leaf: %w", err)
	}
	newLeaf := storage.NewLeafPage(newPage)

	// Clear old leaf and re-insert left half
	oldPage.Header.NumKeys = 0
	for i := 0; i < splitIndex; i++ {
		if err := oldLeaf.InsertRecord(allRecords[i]); err != nil {
			return 0, 0, fmt.Errorf("failed to insert into old leaf: %w", err)
		}
	}

	// Insert right half into new leaf
	for i := splitIndex; i < len(allRecords); i++ {
		if err := newLeaf.InsertRecord(allRecords[i]); err != nil {
			return 0, 0, fmt.Errorf("failed to insert into new leaf: %w", err)
		}
	}

	// Update leaf chain: newLeaf.next = oldLeaf.next, oldLeaf.next = newLeaf
	newPage.Header.NextPage = oldPage.Header.NextPage
	oldPage.Header.NextPage = uint32(newPageID)

	// Copy parent pointer
	newPage.Header.Parent = oldPage.Header.Parent

	// Get promoted key (first key of right leaf)
	promotedKey, err := allRecords[splitIndex].GetKeyAsUint32()
	if err != nil {
		return 0, 0, fmt.Errorf("failed to get promoted key: %w", err)
	}

	// Write both pages
	if err := writePageStruct(tree.pager, oldPageID, oldPage); err != nil {
		return 0, 0, err
	}
	if err := writePageStruct(tree.pager, newPageID, newPage); err != nil {
		return 0, 0, err
	}

	return promotedKey, newPageID, nil
}

// insertIntoParent inserts promoted key into parent internal node
// Handles recursive splitting up the tree
func (tree *BPTree) insertIntoParent(leftChildID uint64, key uint32, rightChildID uint64) error {
	// Load left child to get parent pointer
	leftChild, err := readPageStruct(tree.pager, leftChildID)
	if err != nil {
		return fmt.Errorf("failed to load left child: %w", err)
	}

	// If no parent, create new root
	if leftChild.Header.Parent == 0 {
		return tree.createNewRoot(leftChildID, key, rightChildID)
	}

	// Load parent
	parentID := uint64(leftChild.Header.Parent)
	parentPage, err := readPageStruct(tree.pager, parentID)
	if err != nil {
		return fmt.Errorf("failed to load parent: %w", err)
	}

	parent := storage.NewInternalPage(parentPage)

	// Try to insert into parent
	err = parent.InsertEntry(key, rightChildID)
	if err == nil {
		// Success without split - update right child's parent pointer
		rightChild, err := readPageStruct(tree.pager, rightChildID)
		if err != nil {
			return err
		}
		rightChild.Header.Parent = uint32(parentID)
		if err := writePageStruct(tree.pager, rightChildID, rightChild); err != nil {
			return err
		}

		return writePageStruct(tree.pager, parentID, parentPage)
	}

	// Parent is full, need to split
	return tree.splitInternal(parentID, parentPage, key, rightChildID)
}

// splitInternal splits a full internal page
func (tree *BPTree) splitInternal(oldPageID uint64, oldPage *storage.Page, newKey uint32, newChildID uint64) error {
	oldInternal := storage.NewInternalPage(oldPage)

	// Collect all entries (keys + pointers)
	type entry struct {
		key    uint32
		pageID uint64
	}

	entries := make([]entry, 0, oldPage.Header.NumKeys+1)

	// Get existing entries
	for i := 0; i < int(oldPage.Header.NumKeys); i++ {
		k, p, err := oldInternal.GetKeyPointer(i)
		if err != nil {
			return fmt.Errorf("failed to get entry %d: %w", i, err)
		}
		entries = append(entries, entry{key: k, pageID: p})
	}

	// Insert new entry in sorted position
	inserted := false
	for i, e := range entries {
		if newKey < e.key {
			entries = append(entries[:i], append([]entry{{newKey, newChildID}}, entries[i:]...)...)
			inserted = true
			break
		}
	}
	if !inserted {
		entries = append(entries, entry{newKey, newChildID})
	}

	// Find middle point
	middleIndex := len(entries) / 2
	middleKey := entries[middleIndex].key

	// Create new right internal page
	newPageID, newPage, err := allocatePageWithType(tree.pager, storage.PageTypeInternal)
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}
	newInternal := storage.NewInternalPage(newPage)

	// Get leftmost pointer (before all keys)
	leftmostPtr, err := oldInternal.GetLeftmostPointer()
	if err != nil {
		return err
	}

	// Rebuild old page (left half)
	oldPage.Header.NumKeys = 0
	oldInternal.SetLeftmostPointer(leftmostPtr)

	for i := 0; i < middleIndex; i++ {
		if err := oldInternal.InsertEntry(entries[i].key, entries[i].pageID); err != nil {
			return fmt.Errorf("failed to insert into old internal: %w", err)
		}
	}

	// Build new page (right half)
	// Leftmost pointer of new page is the pointer associated with middle key
	newInternal.SetLeftmostPointer(entries[middleIndex].pageID)

	for i := middleIndex + 1; i < len(entries); i++ {
		if err := newInternal.InsertEntry(entries[i].key, entries[i].pageID); err != nil {
			return fmt.Errorf("failed to insert into new internal: %w", err)
		}
	}

	// Copy parent pointer
	newPage.Header.Parent = oldPage.Header.Parent

	// Update parent pointers of children in new page
	// Update leftmost pointer's child
	child, err := readPageStruct(tree.pager, entries[middleIndex].pageID)
	if err == nil {
		child.Header.Parent = uint32(newPageID)
		writePageStruct(tree.pager, entries[middleIndex].pageID, child)
	}

	// Update other children
	for i := middleIndex + 1; i < len(entries); i++ {
		child, err := readPageStruct(tree.pager, entries[i].pageID)
		if err == nil {
			child.Header.Parent = uint32(newPageID)
			writePageStruct(tree.pager, entries[i].pageID, child)
		}
	}

	// Write both internal pages
	if err := writePageStruct(tree.pager, oldPageID, oldPage); err != nil {
		return err
	}
	if err := writePageStruct(tree.pager, newPageID, newPage); err != nil {
		return err
	}

	// Recursively insert promoted key into parent
	// Note: middle key is PUSHED UP (not copied like in leaf split)
	return tree.insertIntoParent(oldPageID, middleKey, newPageID)
}

// createNewRoot creates a new root when current root splits
func (tree *BPTree) createNewRoot(leftChildID uint64, key uint32, rightChildID uint64) error {
	// Allocate new root (internal node)
	newRootID, newRootPage, err := allocatePageWithType(tree.pager, storage.PageTypeInternal)
	if err != nil {
		return fmt.Errorf("failed to allocate new root: %w", err)
	}

	newRoot := storage.NewInternalPage(newRootPage)

	// Set leftmost pointer to left child
	if err := newRoot.SetLeftmostPointer(leftChildID); err != nil {
		return err
	}

	// Insert key pointing to right child
	if err := newRoot.InsertEntry(key, rightChildID); err != nil {
		return err
	}

	// Update parent pointers of children
	leftChild, err := readPageStruct(tree.pager, leftChildID)
	if err != nil {
		return err
	}
	leftChild.Header.Parent = uint32(newRootID)
	if err := writePageStruct(tree.pager, leftChildID, leftChild); err != nil {
		return err
	}

	rightChild, err := readPageStruct(tree.pager, rightChildID)
	if err != nil {
		return err
	}
	rightChild.Header.Parent = uint32(newRootID)
	if err := writePageStruct(tree.pager, rightChildID, rightChild); err != nil {
		return err
	}

	// Write new root
	if err := writePageStruct(tree.pager, newRootID, newRootPage); err != nil {
		return err
	}

	// Update tree's root pointer
	tree.rootPage = newRootID

	// Update metadata file with new root
	if tree.wal != nil {
		metaPath := tree.wal.Path() + ".meta"
		if err := tree.SaveMetadata(metaPath); err != nil {
			fmt.Printf("Warning: failed to update metadata after root change: %v\n", err)
		}
	}

	return nil
}

// Search searches for a key in the B+ Tree
func (tree *BPTree) Search(key uint32) (string, bool, error) {
	leafPageID, err := tree.findLeafPage(key)
	if err != nil {
		return "", false, fmt.Errorf("failed to find leaf page: %w", err)
	}

	leafPage, err := readPageStruct(tree.pager, leafPageID)
	if err != nil {
		return "", false, fmt.Errorf("failed to load leaf page: %w", err)
	}

	leaf := storage.NewLeafPage(leafPage)
	record, found := leaf.SearchRecord(key)
	if !found {
		return "", false, nil
	}

	return record.GetValueAsString(), true, nil
}

// findLeafPage navigates from root to leaf
func (tree *BPTree) findLeafPage(key uint32) (uint64, error) {
	currentPageID := tree.rootPage

	for {
		page, err := readPageStruct(tree.pager, currentPageID)
		if err != nil {
			return 0, fmt.Errorf("failed to read page %d: %w", currentPageID, err)
		}

		if page.IsLeaf() {
			return currentPageID, nil
		}

		internalPage := storage.NewInternalPage(page)
		childPageID, err := internalPage.SearchChild(key)
		if err != nil {
			return 0, fmt.Errorf("failed to search child in page %d: %w", currentPageID, err)
		}

		currentPageID = childPageID
	}
}

// InOrderTraversal returns all keys in sorted order
func (tree *BPTree) InOrderTraversal() ([]uint32, error) {
	keys := make([]uint32, 0)

	leftmostLeafID, err := tree.findLeftmostLeaf()
	if err != nil {
		return nil, err
	}

	currentPageID := leftmostLeafID
	for currentPageID != 0 {
		page, err := readPageStruct(tree.pager, currentPageID)
		if err != nil {
			return nil, fmt.Errorf("failed to read page %d: %w", currentPageID, err)
		}

		leaf := storage.NewLeafPage(page)
		records, err := leaf.GetAllRecords()
		if err != nil {
			return nil, fmt.Errorf("failed to get records from page %d: %w", currentPageID, err)
		}

		for _, record := range records {
			key, _ := record.GetKeyAsUint32()
			keys = append(keys, key)
		}

		currentPageID = uint64(page.Header.NextPage)
	}

	return keys, nil
}

// findLeftmostLeaf finds leftmost leaf
func (tree *BPTree) findLeftmostLeaf() (uint64, error) {
	currentPageID := tree.rootPage

	for {
		page, err := readPageStruct(tree.pager, currentPageID)
		if err != nil {
			return 0, err
		}

		if page.IsLeaf() {
			return currentPageID, nil
		}

		internalPage := storage.NewInternalPage(page)
		leftmostPtr, err := internalPage.GetLeftmostPointer()
		if err != nil {
			return 0, err
		}

		currentPageID = leftmostPtr
	}
}

// GetRootPageID returns root page ID
func (tree *BPTree) GetRootPageID() uint64 {
	return tree.rootPage
}

// GetOrder returns tree order
func (tree *BPTree) GetOrder() int {
	return tree.order
}

func readPageStruct(pager storage.Pager, pageID uint64) (*storage.Page, error) {
	data, err := pager.ReadPage(pageID)
	if err != nil {
		return nil, err
	}
	return storage.DeserializePage(data)
}

func writePageStruct(pager storage.Pager, pageID uint64, page *storage.Page) error {
	data := page.Serialize()
	return pager.WritePage(pageID, data)
}

func allocatePageWithType(pager storage.Pager, pageType storage.PageType) (uint64, *storage.Page, error) {
	pageID, err := pager.AllocatePage()
	if err != nil {
		return 0, nil, err
	}

	page := storage.NewPage(pageType)
	if err := writePageStruct(pager, pageID, page); err != nil {
		return 0, nil, err
	}

	return pageID, page, nil
}

// sortRecordsByKey sorts records by key (ascending)
func sortRecordsByKey(records []*storage.Record) {
	// TODO: Simple bubble sort (good enough for small arrays). Need to change this shit in the future
	n := len(records)
	for i := 0; i < n-1; i++ {
		for j := 0; j < n-i-1; j++ {
			key1, _ := records[j].GetKeyAsUint32()
			key2, _ := records[j+1].GetKeyAsUint32()
			if key1 > key2 {
				records[j], records[j+1] = records[j+1], records[j]
			}
		}
	}
}

// SaveMetadata saves tree metadata to a file
func (tree *BPTree) SaveMetadata(path string) error {
	file, err := os.Create(path)
	if err != nil {
		return fmt.Errorf("failed to create metadata file: %w", err)
	}
	defer file.Close()

	// Write: rootPageID (8 bytes) + order (4 bytes)
	data := make([]byte, 12)
	binary.LittleEndian.PutUint64(data[0:8], tree.rootPage)
	binary.LittleEndian.PutUint32(data[8:12], uint32(tree.order))

	if _, err := file.Write(data); err != nil {
		return fmt.Errorf("failed to write metadata: %w", err)
	}

	return file.Sync() // Ensure metadata is flushed to disk
}

// LoadMetadata loads tree metadata from a file
func LoadMetadata(path string) (rootPageID uint64, order int, err error) {
	file, err := os.Open(path)
	if err != nil {
		return 0, 0, err
	}
	defer file.Close()

	data := make([]byte, 12)
	if _, err := io.ReadFull(file, data); err != nil {
		return 0, 0, fmt.Errorf("failed to read metadata: %w", err)
	}

	rootPageID = binary.LittleEndian.Uint64(data[0:8])
	order = int(binary.LittleEndian.Uint32(data[8:12]))

	return rootPageID, order, nil
}
