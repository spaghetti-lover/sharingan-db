package wal

import (
	"os"
	"testing"
)

func TestWALBasicOperations(t *testing.T) {
	walPath := "test_basic.wal"
	defer os.Remove(walPath)

	// Create WAL
	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Append entries
	entries := []*Entry{
		{OpType: OpInsert, Key: 100, Value: "naruto"},
		{OpType: OpInsert, Key: 200, Value: "sasuke"},
		{OpType: OpInsert, Key: 50, Value: "sakura"},
	}

	for _, entry := range entries {
		if err := w.Append(entry); err != nil {
			t.Fatalf("Failed to append entry: %v", err)
		}
	}

	t.Logf("✓ Appended %d entries", len(entries))
	t.Logf("✓ Performed %d fsync operations", w.GetSyncCount())

	// Read all entries
	readEntries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read entries: %v", err)
	}

	if len(readEntries) != len(entries) {
		t.Errorf("Read %d entries, expected %d", len(readEntries), len(entries))
	}

	// Verify entries
	for i, entry := range readEntries {
		if entry.OpType != entries[i].OpType {
			t.Errorf("Entry %d: OpType=%d, expected %d", i, entry.OpType, entries[i].OpType)
		}
		if entry.Key != entries[i].Key {
			t.Errorf("Entry %d: Key=%d, expected %d", i, entry.Key, entries[i].Key)
		}
		if entry.Value != entries[i].Value {
			t.Errorf("Entry %d: Value=%s, expected %s", i, entry.Value, entries[i].Value)
		}
	}

	t.Log("✓ All entries verified")
}

func TestWALPersistence(t *testing.T) {
	walPath := "test_persistence.wal"
	defer os.Remove(walPath)

	// Write entries
	{
		w, err := NewWAL(walPath)
		if err != nil {
			t.Fatalf("Failed to create WAL: %v", err)
		}

		entries := []*Entry{
			{OpType: OpInsert, Key: 1, Value: "one"},
			{OpType: OpInsert, Key: 2, Value: "two"},
			{OpType: OpInsert, Key: 3, Value: "three"},
		}

		for _, entry := range entries {
			if err := w.Append(entry); err != nil {
				t.Fatalf("Failed to append: %v", err)
			}
		}

		w.Close()
		t.Log("✓ WAL closed after writing")
	}

	// Reopen and verify
	{
		w2, err := NewWAL(walPath)
		if err != nil {
			t.Fatalf("Failed to reopen WAL: %v", err)
		}
		defer w2.Close()

		entries, err := w2.ReadAll()
		if err != nil {
			t.Fatalf("Failed to read entries: %v", err)
		}

		if len(entries) != 3 {
			t.Errorf("Read %d entries, expected 3", len(entries))
		}

		// Verify values
		expectedValues := []string{"one", "two", "three"}
		for i, entry := range entries {
			if entry.Value != expectedValues[i] {
				t.Errorf("Entry %d: value=%s, expected %s", i, entry.Value, expectedValues[i])
			}
		}

		t.Log("✓ All entries persisted correctly")
	}
}

func TestWALTruncate(t *testing.T) {
	walPath := "test_truncate.wal"
	defer os.Remove(walPath)

	w, err := NewWAL(walPath)
	if err != nil {
		t.Fatalf("Failed to create WAL: %v", err)
	}
	defer w.Close()

	// Write some entries
	for i := 1; i <= 5; i++ {
		entry := &Entry{
			OpType: OpInsert,
			Key:    uint32(i),
			Value:  "value",
		}
		if err := w.Append(entry); err != nil {
			t.Fatalf("Failed to append: %v", err)
		}
	}

	size1, _ := w.Size()
	t.Logf("WAL size after 5 entries: %d bytes", size1)

	// Truncate
	if err := w.Truncate(); err != nil {
		t.Fatalf("Failed to truncate: %v", err)
	}

	size2, _ := w.Size()
	if size2 != 0 {
		t.Errorf("WAL size after truncate: %d, expected 0", size2)
	}

	// Verify empty
	entries, err := w.ReadAll()
	if err != nil {
		t.Fatalf("Failed to read after truncate: %v", err)
	}

	if len(entries) != 0 {
		t.Errorf("Found %d entries after truncate, expected 0", len(entries))
	}

	t.Log("✓ Truncate successful")
}

func BenchmarkWALAppend(b *testing.B) {
	walPath := "bench_append.wal"
	defer os.Remove(walPath)

	w, _ := NewWAL(walPath)
	defer w.Close()

	entry := &Entry{
		OpType: OpInsert,
		Key:    100,
		Value:  "benchmark-value",
	}

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		w.Append(entry)
	}

	b.Logf("Performed %d fsync operations", w.GetSyncCount())
}
