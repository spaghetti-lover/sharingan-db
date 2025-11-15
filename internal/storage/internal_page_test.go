package storage

import (
	"testing"
)

func TestInternalPageInsertAndSearch(t *testing.T) {
	page := NewPage(PageTypeInternal)
	internalPage := NewInternalPage(page)

	// Set leftmost pointer
	if err := internalPage.SetLeftmostPointer(100); err != nil {
		t.Fatalf("Failed to set leftmost pointer: %v", err)
	}

	// Insert key-pointer pairs
	entries := []struct {
		key    uint32
		pageID uint64
	}{
		{50, 101},
		{100, 102},
		{150, 103},
	}

	for _, entry := range entries {
		if err := internalPage.InsertEntry(entry.key, entry.pageID); err != nil {
			t.Fatalf("Failed to insert entry: %v", err)
		}
	}

	// Verify count
	if internalPage.NumKeys() != 3 {
		t.Errorf("NumKeys = %d, expected 3", internalPage.NumKeys())
	}

	// Test search
	testCases := []struct {
		key            uint32
		expectedPageID uint64
	}{
		{25, 100},  // < 50 → leftmost
		{50, 101},  // = 50 → second child
		{75, 101},  // 50 < x < 100 → second child
		{100, 102}, // = 100 → third child
		{200, 103}, // > 150 → rightmost
	}

	for _, tc := range testCases {
		pageID, err := internalPage.SearchChild(tc.key)
		if err != nil {
			t.Errorf("SearchChild(%d) failed: %v", tc.key, err)
		}
		if pageID != tc.expectedPageID {
			t.Errorf("SearchChild(%d) = %d, expected %d", tc.key, pageID, tc.expectedPageID)
		}
	}
}
