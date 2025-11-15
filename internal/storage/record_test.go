package storage

import (
	"testing"
)

func TestRecordSerialization(t *testing.T) {
	// Test record encoding/decoding
	record := NewRecordFromInts(42, "Hello World")

	serialized := record.Serialize()

	deserialized, bytesRead, err := DeserializeRecord(serialized)
	if err != nil {
		t.Fatalf("Failed to deserialize: %v", err)
	}

	if bytesRead != len(serialized) {
		t.Errorf("BytesRead = %d, expected %d", bytesRead, len(serialized))
	}

	key, _ := deserialized.GetKeyAsUint32()
	if key != 42 {
		t.Errorf("Key = %d, expected 42", key)
	}

	if deserialized.GetValueAsString() != "Hello World" {
		t.Errorf("Value = %s, expected 'Hello World'", deserialized.GetValueAsString())
	}
}

func TestRecordList(t *testing.T) {
	page := NewPage(PageTypeLeaf)
	rl := NewRecordList()

	// Add some records
	rl.Add(NewRecordFromInts(1, "one"))
	rl.Add(NewRecordFromInts(2, "two"))
	rl.Add(NewRecordFromInts(3, "three"))

	// Serialize to page
	if err := rl.SerializeToPage(page); err != nil {
		t.Fatalf("Failed to serialize to page: %v", err)
	}

	if page.Header.NumKeys != 3 {
		t.Errorf("NumKeys = %d, expected 3", page.Header.NumKeys)
	}

	// Deserialize from page
	deserializedRL, err := DeserializeRecordsFromPage(page)
	if err != nil {
		t.Fatalf("Failed to deserialize from page: %v", err)
	}

	if deserializedRL.Size() != 3 {
		t.Errorf("Size = %d, expected 3", deserializedRL.Size())
	}

	// Verify each record
	for i := 0; i < 3; i++ {
		record := deserializedRL.Get(i)
		key, _ := record.GetKeyAsUint32()
		if key != uint32(i+1) {
			t.Errorf("Record %d: key = %d, expected %d", i, key, i+1)
		}
	}
}

func TestFreeList(t *testing.T) {
	fl := NewFreeList()

	// Test empty
	if !fl.IsEmpty() {
		t.Error("New free list should be empty")
	}

	// Test push/pop
	fl.Push(10)
	fl.Push(20)
	fl.Push(30)

	if fl.Size() != 3 {
		t.Errorf("Size = %d, expected 3", fl.Size())
	}

	// Pop in LIFO order
	pageID, ok := fl.Pop()
	if !ok || pageID != 30 {
		t.Errorf("Pop = %d, expected 30", pageID)
	}

	pageID, ok = fl.Pop()
	if !ok || pageID != 20 {
		t.Errorf("Pop = %d, expected 20", pageID)
	}

	// Test serialization
	page := fl.SerializeToPage()
	deserializedFL, err := DeserializeFreeList(page)
	if err != nil {
		t.Fatalf("Failed to deserialize free list: %v", err)
	}

	if deserializedFL.Size() != 1 {
		t.Errorf("Deserialized size = %d, expected 1", deserializedFL.Size())
	}
}
