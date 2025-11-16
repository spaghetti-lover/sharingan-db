package wal

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sync"
)

// OpType represents the type of operation
type OpType byte

const (
	OpInsert OpType = 0x01
	OpDelete OpType = 0x02
	OpUpdate OpType = 0x03
)

// Entry represents a single WAL entry
type Entry struct {
	OpType OpType
	Key    uint32
	Value  string
}

// WAL represents a Write-Ahead Log
type WAL struct {
	file  *os.File
	mu    sync.Mutex
	path  string
	syncs int // Counter for fsync operations
}

// NewWAL creates a new WAL file
func NewWAL(path string) (*WAL, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open WAL file: %w", err)
	}

	return &WAL{
		file:  file,
		path:  path,
		syncs: 0,
	}, nil
}

// Append writes an entry to the WAL
func (w *WAL) Append(entry *Entry) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Serialize entry
	data := w.serializeEntry(entry)

	// Write to file
	if _, err := w.file.Write(data); err != nil {
		return fmt.Errorf("failed to write WAL entry: %w", err)
	}

	// Flush to disk (fsync)
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync WAL: %w", err)
	}

	w.syncs++
	return nil
}

// serializeEntry converts an entry to bytes
func (w *WAL) serializeEntry(entry *Entry) []byte {
	valueBytes := []byte(entry.Value)
	valueSize := uint32(len(valueBytes))

	// Total size: 1 (opType) + 4 (key) + 4 (valueSize) + len(value)
	data := make([]byte, 9+valueSize)

	data[0] = byte(entry.OpType)
	binary.LittleEndian.PutUint32(data[1:5], entry.Key)
	binary.LittleEndian.PutUint32(data[5:9], valueSize)
	copy(data[9:], valueBytes)

	return data
}

// ReadAll reads all entries from the WAL
func (w *WAL) ReadAll() ([]*Entry, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to beginning
	if _, err := w.file.Seek(0, 0); err != nil {
		return nil, fmt.Errorf("failed to seek WAL: %w", err)
	}

	entries := make([]*Entry, 0)

	for {
		entry, err := w.readEntry()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("failed to read WAL entry: %w", err)
		}

		entries = append(entries, entry)
	}

	// Seek back to end for future appends
	if _, err := w.file.Seek(0, 2); err != nil {
		return nil, fmt.Errorf("failed to seek to end: %w", err)
	}

	return entries, nil
}

// readEntry reads a single entry from the current file position
func (w *WAL) readEntry() (*Entry, error) {
	// Read header (9 bytes: 1 opType + 4 key + 4 valueSize)
	header := make([]byte, 9)
	if _, err := io.ReadFull(w.file, header); err != nil {
		return nil, err
	}

	opType := OpType(header[0])
	key := binary.LittleEndian.Uint32(header[1:5])
	valueSize := binary.LittleEndian.Uint32(header[5:9])

	// Read value
	valueBytes := make([]byte, valueSize)
	if _, err := io.ReadFull(w.file, valueBytes); err != nil {
		return nil, fmt.Errorf("failed to read value: %w", err)
	}

	return &Entry{
		OpType: opType,
		Key:    key,
		Value:  string(valueBytes),
	}, nil
}

// Truncate clears the WAL file
func (w *WAL) Truncate() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Truncate(0); err != nil {
		return fmt.Errorf("failed to truncate WAL: %w", err)
	}

	if _, err := w.file.Seek(0, 0); err != nil {
		return fmt.Errorf("failed to seek after truncate: %w", err)
	}

	w.syncs = 0
	return nil
}

// Close closes the WAL file
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("failed to sync before close: %w", err)
	}

	if err := w.file.Close(); err != nil {
		return fmt.Errorf("failed to close WAL: %w", err)
	}

	return nil
}

// GetSyncCount returns the number of fsync operations
func (w *WAL) GetSyncCount() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.syncs
}

// Size returns the current size of the WAL file
func (w *WAL) Size() (int64, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	info, err := w.file.Stat()
	if err != nil {
		return 0, err
	}

	return info.Size(), nil
}

// Exists checks if WAL file exists and has content
func Exists(path string) bool {
	info, err := os.Stat(path)
	if err != nil {
		return false
	}
	return info.Size() > 0
}

// Path returns the WAL file path
func (w *WAL) Path() string {
	return w.path
}
