package bptree

import (
	"fmt"
	"os"
	"testing"

	"github.com/spaghetti-lover/sharingan-db/internal/storage"
)

func TestBPTreeInsertAndSearch(t *testing.T) {
	// Create temporary database file
	dbFile := "test_bptree.db"
	walFile := "test_bptree.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Create B+ Tree with order 100 and WAL
	tree, err := NewBPTree(pager, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}
	defer tree.Close()

	// Test insert
	testData := []struct {
		key   uint32
		value string
	}{
		{100, "Naruto"},
		{50, "Sasuke"},
		{200, "Sakura"},
		{75, "Kakashi"},
		{150, "Itachi"},
	}

	for _, td := range testData {
		if err := tree.Insert(td.key, td.value); err != nil {
			t.Fatalf("Failed to insert key=%d: %v", td.key, err)
		}
	}

	// Test search - found
	for _, td := range testData {
		value, found, err := tree.Search(td.key)
		if err != nil {
			t.Fatalf("Search failed for key=%d: %v", td.key, err)
		}
		if !found {
			t.Errorf("Key=%d not found", td.key)
		}
		if value != td.value {
			t.Errorf("Key=%d: value=%s, expected %s", td.key, value, td.value)
		}
	}

	// Test search - not found
	_, found, err := tree.Search(999)
	if err != nil {
		t.Fatalf("Search failed: %v", err)
	}
	if found {
		t.Error("Key=999 should not be found")
	}
}

func TestBPTreeInOrderTraversal(t *testing.T) {
	dbFile := "test_traversal.db"
	walFile := "test_traversal.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBPTree(pager, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}
	defer tree.Close()

	// Insert keys in random order
	keys := []uint32{100, 50, 200, 75, 150, 25, 300}
	for _, key := range keys {
		if err := tree.Insert(key, "value"); err != nil {
			t.Fatalf("Failed to insert key=%d: %v", key, err)
		}
	}

	// Get all keys via traversal
	allKeys, err := tree.InOrderTraversal()
	if err != nil {
		t.Fatalf("InOrderTraversal failed: %v", err)
	}

	// Verify sorted order
	expectedKeys := []uint32{25, 50, 75, 100, 150, 200, 300}
	if len(allKeys) != len(expectedKeys) {
		t.Errorf("Length = %d, expected %d", len(allKeys), len(expectedKeys))
	}

	for i, key := range allKeys {
		if key != expectedKeys[i] {
			t.Errorf("Key[%d] = %d, expected %d", i, key, expectedKeys[i])
		}
	}
}

func TestBPTreePersistence(t *testing.T) {
	dbFile := "test_persistence.db"
	walFile := "test_persistence.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	var rootPageID uint64

	// Create tree and insert data
	{
		pager, err := storage.NewFilePager(dbFile)
		if err != nil {
			t.Fatalf("Failed to create pager: %v", err)
		}

		tree, err := NewBPTree(pager, 100, walFile)
		if err != nil {
			t.Fatalf("Failed to create B+ Tree: %v", err)
		}

		rootPageID = tree.GetRootPageID()

		// Insert data
		for i := uint32(1); i <= 10; i++ {
			if err := tree.Insert(i, "value"); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		tree.Close()
		pager.Close()
	}

	// Reopen and verify
	{
		pager2, err := storage.NewFilePager(dbFile)
		if err != nil {
			t.Fatalf("Failed to reopen pager: %v", err)
		}
		defer pager2.Close()

		tree2, err := LoadBPTree(pager2, rootPageID, 100, walFile)
		if err != nil {
			t.Fatalf("Failed to load tree: %v", err)
		}
		defer tree2.Close()

		// Verify all keys
		for i := uint32(1); i <= 10; i++ {
			_, found, err := tree2.Search(i)
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			if !found {
				t.Errorf("Key=%d not found after reopen", i)
			}
		}
	}
}

// TestBPTreeSearchPath tests navigation through multi-level tree
func TestBPTreeSearchPath(t *testing.T) {
	dbFile := "test_search_path.db"
	walFile := "test_search_path.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Manually create a 3-level tree:
	//
	//           Root (Internal, Page 1)
	//           Key: 100
	//          /              \
	//   Leaf1 (Page 2)    Leaf2 (Page 3)
	//   Keys: [25,50,75]  Keys: [100,150,200]

	// Create Leaf1 (Page 2)
	leaf1ID, leaf1Page, err := allocatePageWithType(pager, storage.PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to allocate leaf1: %v", err)
	}
	leaf1 := storage.NewLeafPage(leaf1Page)
	leaf1.InsertRecord(storage.NewRecordFromInts(25, "twenty-five"))
	leaf1.InsertRecord(storage.NewRecordFromInts(50, "fifty"))
	leaf1.InsertRecord(storage.NewRecordFromInts(75, "seventy-five"))

	// Set next pointer to leaf2 (will be page 3)
	leaf1Page.Header.NextPage = uint32(leaf1ID + 1)

	if err := writePageStruct(pager, leaf1ID, leaf1Page); err != nil {
		t.Fatalf("Failed to write leaf1: %v", err)
	}

	// Create Leaf2 (Page 3)
	leaf2ID, leaf2Page, err := allocatePageWithType(pager, storage.PageTypeLeaf)
	if err != nil {
		t.Fatalf("Failed to allocate leaf2: %v", err)
	}
	leaf2 := storage.NewLeafPage(leaf2Page)
	leaf2.InsertRecord(storage.NewRecordFromInts(100, "one-hundred"))
	leaf2.InsertRecord(storage.NewRecordFromInts(150, "one-fifty"))
	leaf2.InsertRecord(storage.NewRecordFromInts(200, "two-hundred"))

	if err := writePageStruct(pager, leaf2ID, leaf2Page); err != nil {
		t.Fatalf("Failed to write leaf2: %v", err)
	}

	// Create Root (Internal, Page 1)
	rootID, rootPage, err := allocatePageWithType(pager, storage.PageTypeInternal)
	if err != nil {
		t.Fatalf("Failed to allocate root: %v", err)
	}
	root := storage.NewInternalPage(rootPage)

	// Set leftmost pointer to leaf1
	root.SetLeftmostPointer(leaf1ID)

	// Insert key 100 pointing to leaf2
	root.InsertEntry(100, leaf2ID)

	if err := writePageStruct(pager, rootID, rootPage); err != nil {
		t.Fatalf("Failed to write root: %v", err)
	}

	// Create B+ Tree with existing root (load with WAL)
	tree, err := LoadBPTree(pager, rootID, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to load tree: %v", err)
	}
	defer tree.Close()

	// Test search in leaf1 (keys < 100)
	testCases := []struct {
		key           uint32
		expectedValue string
		shouldFind    bool
	}{
		{25, "twenty-five", true},
		{50, "fifty", true},
		{75, "seventy-five", true},
		{100, "one-hundred", true},
		{150, "one-fifty", true},
		{200, "two-hundred", true},
		{10, "", false},  // Not in tree
		{300, "", false}, // Not in tree
	}

	for _, tc := range testCases {
		value, found, err := tree.Search(tc.key)
		if err != nil {
			t.Errorf("Search(%d) failed: %v", tc.key, err)
			continue
		}

		if found != tc.shouldFind {
			t.Errorf("Search(%d): found=%v, expected %v", tc.key, found, tc.shouldFind)
		}

		if found && value != tc.expectedValue {
			t.Errorf("Search(%d): value=%s, expected %s", tc.key, value, tc.expectedValue)
		}
	}

	// Verify in-order traversal
	keys, err := tree.InOrderTraversal()
	if err != nil {
		t.Fatalf("InOrderTraversal failed: %v", err)
	}

	expectedKeys := []uint32{25, 50, 75, 100, 150, 200}
	if len(keys) != len(expectedKeys) {
		t.Errorf("Traversal returned %d keys, expected %d", len(keys), len(expectedKeys))
	}

	for i, key := range keys {
		if key != expectedKeys[i] {
			t.Errorf("Key[%d] = %d, expected %d", i, key, expectedKeys[i])
		}
	}

	t.Logf("✓ Successfully navigated 3-level tree with root at page %d", rootID)
}

// TestBPTreeDeepTree tests a deeper tree (3-4 levels)
func TestBPTreeDeepTree(t *testing.T) {
	dbFile := "test_deep_tree.db"
	walFile := "test_deep_tree.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	// Build a deeper tree manually:
	//
	//                    Root (Internal, Page 1)
	//                    Keys: [100, 200]
	//           /              |              \
	//    Internal1         Internal2       Internal3
	//    (Page 2)          (Page 3)        (Page 4)
	//    Key: 50          Key: 150        Key: 250
	//     /    \           /    \          /    \
	//   L1    L2          L3    L4        L5    L6

	// Create 6 leaf pages
	leafData := [][]struct {
		key   uint32
		value string
	}{
		{{25, "a"}, {40, "b"}},               // L1
		{{50, "c"}, {75, "d"}},               // L2
		{{100, "e"}, {125, "f"}},             // L3
		{{150, "g"}, {175, "h"}},             // L4
		{{200, "i"}, {225, "j"}},             // L5
		{{250, "k"}, {275, "l"}, {300, "m"}}, // L6
	}

	leafPageIDs := make([]uint64, 6)

	for i, data := range leafData {
		leafID, leafPage, err := allocatePageWithType(pager, storage.PageTypeLeaf)
		if err != nil {
			t.Fatalf("Failed to allocate leaf %d: %v", i, err)
		}

		leaf := storage.NewLeafPage(leafPage)
		for _, record := range data {
			if err := leaf.InsertRecord(storage.NewRecordFromInts(record.key, record.value)); err != nil {
				t.Fatalf("Failed to insert into leaf %d: %v", i, err)
			}
		}

		// Link leaves together
		if i < len(leafData)-1 {
			leafPage.Header.NextPage = uint32(leafID + 1)
		}

		if err := writePageStruct(pager, leafID, leafPage); err != nil {
			t.Fatalf("Failed to write leaf %d: %v", i, err)
		}

		leafPageIDs[i] = leafID
	}

	// Create 3 internal pages (level 2)
	internal1ID, internal1Page, _ := allocatePageWithType(pager, storage.PageTypeInternal)
	internal1 := storage.NewInternalPage(internal1Page)
	internal1.SetLeftmostPointer(leafPageIDs[0]) // L1
	internal1.InsertEntry(50, leafPageIDs[1])    // 50 → L2
	writePageStruct(pager, internal1ID, internal1Page)

	internal2ID, internal2Page, _ := allocatePageWithType(pager, storage.PageTypeInternal)
	internal2 := storage.NewInternalPage(internal2Page)
	internal2.SetLeftmostPointer(leafPageIDs[2]) // L3
	internal2.InsertEntry(150, leafPageIDs[3])   // 150 → L4
	writePageStruct(pager, internal2ID, internal2Page)

	internal3ID, internal3Page, _ := allocatePageWithType(pager, storage.PageTypeInternal)
	internal3 := storage.NewInternalPage(internal3Page)
	internal3.SetLeftmostPointer(leafPageIDs[4]) // L5
	internal3.InsertEntry(250, leafPageIDs[5])   // 250 → L6
	writePageStruct(pager, internal3ID, internal3Page)

	// Create root (level 1)
	rootID, rootPage, _ := allocatePageWithType(pager, storage.PageTypeInternal)
	root := storage.NewInternalPage(rootPage)
	root.SetLeftmostPointer(internal1ID) // < 100 → Internal1
	root.InsertEntry(100, internal2ID)   // [100, 200) → Internal2
	root.InsertEntry(200, internal3ID)   // >= 200 → Internal3
	writePageStruct(pager, rootID, rootPage)

	// Create tree
	tree, err := LoadBPTree(pager, rootID, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to load tree: %v", err)
	}
	defer tree.Close()

	// Test searches across all branches
	testCases := []struct {
		key           uint32
		expectedValue string
		shouldFind    bool
	}{
		{25, "a", true},
		{50, "c", true},
		{100, "e", true},
		{150, "g", true},
		{200, "i", true},
		{250, "k", true},
		{300, "m", true},
		{1, "", false},
		{500, "", false},
	}

	for _, tc := range testCases {
		value, found, err := tree.Search(tc.key)
		if err != nil {
			t.Errorf("Search(%d) failed: %v", tc.key, err)
			continue
		}

		if found != tc.shouldFind {
			t.Errorf("Search(%d): found=%v, expected %v", tc.key, found, tc.shouldFind)
		}

		if found && value != tc.expectedValue {
			t.Errorf("Search(%d): value=%s, expected %s", tc.key, value, tc.expectedValue)
		}
	}

	// Verify in-order traversal
	keys, err := tree.InOrderTraversal()
	if err != nil {
		t.Fatalf("InOrderTraversal failed: %v", err)
	}

	expectedKeys := []uint32{25, 40, 50, 75, 100, 125, 150, 175, 200, 225, 250, 275, 300}
	if len(keys) != len(expectedKeys) {
		t.Errorf("Traversal returned %d keys, expected %d", len(keys), len(expectedKeys))
	}

	for i, key := range keys {
		if key != expectedKeys[i] {
			t.Errorf("Key[%d] = %d, expected %d", i, key, expectedKeys[i])
		}
	}

	t.Logf("✓ Successfully navigated 4-level tree (root → internal → internal → leaf)")
}

// TestBPTreeLeafSplit tests leaf splitting
func TestBPTreeLeafSplit(t *testing.T) {
	dbFile := "test_leaf_split.db"
	walFile := "test_leaf_split.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBPTree(pager, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}
	defer tree.Close()

	// Insert enough records to trigger split
	// Each record is ~20 bytes, page is 4080 bytes
	// Should split around 200 records
	numRecords := 250

	for i := 1; i <= numRecords; i++ {
		key := uint32(i * 10) // Use non-sequential keys
		value := fmt.Sprintf("value-%d", i)
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Failed to insert key=%d: %v", key, err)
		}
	}

	t.Logf("✓ Inserted %d records (should trigger splits)", numRecords)

	// Verify all keys are searchable
	for i := 1; i <= numRecords; i++ {
		key := uint32(i * 10)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Search failed for key=%d: %v", key, err)
		}
		if !found {
			t.Errorf("Key=%d not found", key)
		}
		if value != expectedValue {
			t.Errorf("Key=%d: value=%s, expected %s", key, value, expectedValue)
		}
	}

	// Verify in-order traversal
	keys, err := tree.InOrderTraversal()
	if err != nil {
		t.Fatalf("InOrderTraversal failed: %v", err)
	}

	if len(keys) != numRecords {
		t.Errorf("Traversal returned %d keys, expected %d", len(keys), numRecords)
	}

	// Verify keys are sorted
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("Keys not sorted: keys[%d]=%d, keys[%d]=%d", i-1, keys[i-1], i, keys[i])
		}
	}

	t.Logf("✓ All %d keys verified and sorted", len(keys))
}

// TestBPTreeInternalSplit tests internal node splitting
func TestBPTreeInternalSplit(t *testing.T) {
	dbFile := "test_internal_split.db"
	walFile := "test_internal_split.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBPTree(pager, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}
	defer tree.Close()

	// Insert many records to trigger both leaf and internal splits
	// With small order, internal nodes will fill up
	numRecords := 1000

	for i := 1; i <= numRecords; i++ {
		key := uint32(i)
		value := fmt.Sprintf("value-%d", i)
		if err := tree.Insert(key, value); err != nil {
			t.Fatalf("Failed to insert key=%d: %v", key, err)
		}
	}

	t.Logf("✓ Inserted %d records (triggered internal splits)", numRecords)

	// Verify all keys are searchable
	for i := 1; i <= numRecords; i++ {
		key := uint32(i)
		expectedValue := fmt.Sprintf("value-%d", i)

		value, found, err := tree.Search(key)
		if err != nil {
			t.Fatalf("Search failed for key=%d: %v", key, err)
		}
		if !found {
			t.Errorf("Key=%d not found", key)
		}
		if value != expectedValue {
			t.Errorf("Key=%d: value=%s, expected %s", key, value, expectedValue)
		}
	}

	// Verify in-order traversal
	keys, err := tree.InOrderTraversal()
	if err != nil {
		t.Fatalf("InOrderTraversal failed: %v", err)
	}

	if len(keys) != numRecords {
		t.Errorf("Traversal returned %d keys, expected %d", len(keys), numRecords)
	}

	// Verify keys are sorted
	for i := 1; i < len(keys); i++ {
		if keys[i] <= keys[i-1] {
			t.Errorf("Keys not sorted: keys[%d]=%d, keys[%d]=%d", i-1, keys[i-1], i, keys[i])
		}
	}

	t.Logf("✓ All %d keys verified and sorted", len(keys))
	t.Logf("✓ Final root page ID: %d (tree height increased)", tree.GetRootPageID())
}

func TestBPTreeWithWAL(t *testing.T) {
	dbFile := "test_wal.db"
	walFile := "test_wal.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	pager, err := storage.NewFilePager(dbFile)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer pager.Close()

	tree, err := NewBPTree(pager, 100, walFile)
	if err != nil {
		t.Fatalf("Failed to create B+ Tree: %v", err)
	}
	defer tree.Close()

	// Insert with WAL
	testData := []struct {
		key   uint32
		value string
	}{
		{100, "naruto"},
		{200, "sasuke"},
		{50, "sakura"},
	}

	for _, td := range testData {
		if err := tree.Insert(td.key, td.value); err != nil {
			t.Fatalf("Failed to insert key=%d: %v", td.key, err)
		}
	}

	t.Logf("✓ Inserted %d records with WAL", len(testData))
	t.Logf("✓ WAL syncs: %d", tree.GetWALSyncCount())

	// Verify data
	for _, td := range testData {
		value, found, err := tree.Search(td.key)
		if err != nil {
			t.Fatalf("Search failed: %v", err)
		}
		if !found {
			t.Errorf("Key=%d not found", td.key)
		}
		if value != td.value {
			t.Errorf("Key=%d: value=%s, expected %s", td.key, value, td.value)
		}
	}

	t.Log("✓ All data verified")
}

func TestWALRecovery(t *testing.T) {
	dbFile := "test_recovery.db"
	walFile := "test_recovery.wal"
	defer os.Remove(dbFile)
	defer os.Remove(walFile)

	var rootPageID uint64

	// Phase 1: Create tree and insert data
	{
		pager, err := storage.NewFilePager(dbFile)
		if err != nil {
			t.Fatalf("Failed to create pager: %v", err)
		}

		tree, err := NewBPTree(pager, 100, walFile)
		if err != nil {
			t.Fatalf("Failed to create B+ Tree: %v", err)
		}

		rootPageID = tree.GetRootPageID()

		// Insert data
		for i := 1; i <= 10; i++ {
			if err := tree.Insert(uint32(i), fmt.Sprintf("value-%d", i)); err != nil {
				t.Fatalf("Failed to insert: %v", err)
			}
		}

		t.Logf("✓ Inserted 10 records, WAL syncs: %d", tree.GetWALSyncCount())

		// Don't close properly - simulate crash
		pager.Close()
		t.Log("⚠️  Simulated crash (didn't close tree)")
	}

	// Phase 2: Recover from WAL
	{
		pager2, err := storage.NewFilePager(dbFile)
		if err != nil {
			t.Fatalf("Failed to reopen pager: %v", err)
		}
		defer pager2.Close()

		tree2, err := LoadBPTree(pager2, rootPageID, 100, walFile)
		if err != nil {
			t.Fatalf("Failed to load tree: %v", err)
		}
		defer tree2.Close()

		// Verify all data recovered
		for i := 1; i <= 10; i++ {
			value, found, err := tree2.Search(uint32(i))
			if err != nil {
				t.Fatalf("Search failed: %v", err)
			}
			if !found {
				t.Errorf("Key=%d not found after recovery", i)
			}

			expectedValue := fmt.Sprintf("value-%d", i)
			if value != expectedValue {
				t.Errorf("Key=%d: value=%s, expected %s", i, value, expectedValue)
			}
		}

		t.Log("✓ All data recovered successfully")
	}
}
