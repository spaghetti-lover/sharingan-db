package storage

import (
	"testing"
)

func TestLeafPageInsertAndSearch(t *testing.T) {
	page := NewPage(PageTypeLeaf)
	leafPage := NewLeafPage(page)

	// Insert records
	records := []*Record{
		NewRecordFromInts(100, "Naruto"),
		NewRecordFromInts(50, "Sasuke"),
		NewRecordFromInts(200, "Sakura"),
		NewRecordFromInts(75, "Kakashi"),
	}

	for _, record := range records {
		if err := leafPage.InsertRecord(record); err != nil {
			t.Fatalf("Failed to insert record: %v", err)
		}
	}

	// Verify count
	if leafPage.NumRecords() != 4 {
		t.Errorf("NumRecords = %d, expected 4", leafPage.NumRecords())
	}

	// Verify sorted order
	allRecords, err := leafPage.GetAllRecords()
	if err != nil {
		t.Fatalf("Failed to get all records: %v", err)
	}

	expectedKeys := []uint32{50, 75, 100, 200}
	for i, record := range allRecords {
		key, _ := record.GetKeyAsUint32()
		if key != expectedKeys[i] {
			t.Errorf("Record %d: key = %d, expected %d", i, key, expectedKeys[i])
		}
	}

	// Test search
	record, found := leafPage.SearchRecord(100)
	if !found {
		t.Error("Record with key 100 not found")
	}
	if record.GetValueAsString() != "Naruto" {
		t.Errorf("Value = %s, expected 'Naruto'", record.GetValueAsString())
	}

	// Test search not found
	_, found = leafPage.SearchRecord(999)
	if found {
		t.Error("Should not find record with key 999")
	}
}

func TestLeafPageFull(t *testing.T) {
	page := NewPage(PageTypeLeaf)
	leafPage := NewLeafPage(page)

	// Insert until full
	i := 0
	for {
		record := NewRecordFromInts(uint32(i), "value"+string(rune(i)))
		err := leafPage.InsertRecord(record)
		if err != nil {
			break
		}
		i++
	}

	t.Logf("Inserted %d records before page full", i)
	t.Logf("Available space: %d bytes", leafPage.AvailableSpace())

	if i < 10 {
		t.Errorf("Page should fit at least 10 records, only fit %d", i)
	}
}
