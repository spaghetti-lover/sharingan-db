package storage

import (
	"testing"
)

func TestPageSerialization(t *testing.T) {
	page := NewPage(PageTypeLeaf)
	page.Header.NumKeys = 10
	page.Header.NextPage = 42
	page.Header.Parent = 5

	copy(page.Data, []byte("test data"))

	// Serialize
	serialized := page.Serialize()

	if len(serialized) != PageSize {
		t.Errorf("Serialized page size = %d, expected %d", len(serialized), PageSize)
	}

	// Deserialize
	deserialized, err := DeserializePage(serialized)
	if err != nil {
		t.Fatalf("Deserialize failed: %v", err)
	}

	// Verify header
	if deserialized.Header.PageType != PageTypeLeaf {
		t.Errorf("PageType = %v, expected %v", deserialized.Header.PageType, PageTypeLeaf)
	}
	if deserialized.Header.NumKeys != 10 {
		t.Errorf("NumKeys = %d, expected 10", deserialized.Header.NumKeys)
	}
	if deserialized.Header.NextPage != 42 {
		t.Errorf("NextPage = %d, expected 42", deserialized.Header.NextPage)
	}
	if deserialized.Header.Parent != 5 {
		t.Errorf("Parent = %d, expected 5", deserialized.Header.Parent)
	}

	// Verify data
	if string(deserialized.Data[:9]) != "test data" {
		t.Errorf("Data mismatch: got %s", string(deserialized.Data[:9]))
	}
}

func TestPageTypes(t *testing.T) {
	leaf := NewPage(PageTypeLeaf)
	if !leaf.IsLeaf() {
		t.Error("IsLeaf() should return true")
	}

	internal := NewPage(PageTypeInternal)
	if !internal.IsInternal() {
		t.Error("IsInternal() should return true")
	}

	free := NewPage(PageTypeFree)
	if !free.IsFree() {
		t.Error("IsFree() should return true")
	}
}
