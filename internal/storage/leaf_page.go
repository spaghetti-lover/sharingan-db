package storage

import (
	"encoding/binary"
	"fmt"
)

// LeafPage represents a B+ Tree leaf node with slot-based layout
type LeafPage struct {
	page *Page
}

// NewLeafPage creates a new leaf page
func NewLeafPage(page *Page) *LeafPage {
	if page.Header.PageType != PageTypeLeaf {
		panic("page must be of type Leaf")
	}
	return &LeafPage{page: page}
}

// SlotOffset returns the offset of a record in the data area
// Format: [numSlots: 2 bytes][slot1: 2 bytes][slot2: 2 bytes]...
func (lp *LeafPage) getSlotOffset(index int) uint16 {
	if index < 0 || index >= int(lp.page.Header.NumKeys) {
		return 0
	}
	// Skip numSlots (2 bytes), then read slot at index
	offset := 2 + index*2
	return binary.LittleEndian.Uint16(lp.page.Data[offset : offset+2])
}

// setSlotOffset sets the offset for a slot
func (lp *LeafPage) setSlotOffset(index int, offset uint16) {
	slotPos := 2 + index*2
	binary.LittleEndian.PutUint16(lp.page.Data[slotPos:slotPos+2], offset)
}

// FreeSpaceStart returns where free space begins (after slot table)
func (lp *LeafPage) freeSpaceStart() int {
	return 2 + int(lp.page.Header.NumKeys)*2
}

// FreeSpaceEnd returns where free space ends (start of record area)
func (lp *LeafPage) freeSpaceEnd() int {
	if lp.page.Header.NumKeys == 0 {
		return len(lp.page.Data)
	}
	// Find lowest offset in slot table
	minOffset := len(lp.page.Data)
	for i := 0; i < int(lp.page.Header.NumKeys); i++ {
		offset := int(lp.getSlotOffset(i))
		if offset < minOffset {
			minOffset = offset
		}
	}
	return minOffset
}

// AvailableSpace returns free space in bytes
func (lp *LeafPage) AvailableSpace() int {
	return lp.freeSpaceEnd() - lp.freeSpaceStart()
}

// InsertRecord inserts a record into the leaf page (sorted by key)
// Returns error if page is full
func (lp *LeafPage) InsertRecord(record *Record) error {
	recordSize := record.Size()
	slotSize := 2 // 2 bytes per slot

	// Check if we have space (need space for both slot and record)
	if lp.AvailableSpace() < recordSize+slotSize {
		return fmt.Errorf("leaf page full: need %d bytes, have %d", recordSize+slotSize, lp.AvailableSpace())
	}

	// Serialize record
	serialized := record.Serialize()

	// Find insertion position (binary search for sorted order)
	insertPos := lp.findInsertPosition(record)

	// Allocate space for record at end of data area
	recordOffset := lp.freeSpaceEnd() - recordSize
	copy(lp.page.Data[recordOffset:recordOffset+recordSize], serialized)

	// Shift slots to make room
	if insertPos < int(lp.page.Header.NumKeys) {
		for i := int(lp.page.Header.NumKeys); i > insertPos; i-- {
			lp.setSlotOffset(i, lp.getSlotOffset(i-1))
		}
	}

	// Insert new slot
	lp.setSlotOffset(insertPos, uint16(recordOffset))

	// Update numKeys
	lp.page.Header.NumKeys++

	// Write numSlots at beginning
	binary.LittleEndian.PutUint16(lp.page.Data[0:2], lp.page.Header.NumKeys)

	return nil
}

// findInsertPosition finds where to insert record to maintain sorted order
func (lp *LeafPage) findInsertPosition(record *Record) int {
	key, err := record.GetKeyAsUint32()
	if err != nil {
		return int(lp.page.Header.NumKeys) // Insert at end if key is not uint32
	}

	// Binary search
	left, right := 0, int(lp.page.Header.NumKeys)
	for left < right {
		mid := (left + right) / 2
		midRecord, err := lp.GetRecord(mid)
		if err != nil {
			return int(lp.page.Header.NumKeys)
		}

		midKey, err := midRecord.GetKeyAsUint32()
		if err != nil {
			return int(lp.page.Header.NumKeys)
		}

		if midKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return left
}

// GetRecord retrieves a record by slot index
func (lp *LeafPage) GetRecord(index int) (*Record, error) {
	if index < 0 || index >= int(lp.page.Header.NumKeys) {
		return nil, fmt.Errorf("index %d out of bounds", index)
	}

	offset := int(lp.getSlotOffset(index))
	record, _, err := DeserializeRecord(lp.page.Data[offset:])
	return record, err
}

// SearchRecord searches for a record by key (binary search)
// Returns (record, found)
func (lp *LeafPage) SearchRecord(key uint32) (*Record, bool) {
	left, right := 0, int(lp.page.Header.NumKeys)

	for left < right {
		mid := (left + right) / 2
		record, err := lp.GetRecord(mid)
		if err != nil {
			return nil, false
		}

		recordKey, err := record.GetKeyAsUint32()
		if err != nil {
			return nil, false
		}

		if recordKey == key {
			return record, true
		} else if recordKey < key {
			left = mid + 1
		} else {
			right = mid
		}
	}

	return nil, false
}

// GetAllRecords returns all records in sorted order
func (lp *LeafPage) GetAllRecords() ([]*Record, error) {
	records := make([]*Record, 0, lp.page.Header.NumKeys)
	for i := 0; i < int(lp.page.Header.NumKeys); i++ {
		record, err := lp.GetRecord(i)
		if err != nil {
			return nil, err
		}
		records = append(records, record)
	}
	return records, nil
}

// IsFull checks if page is full (less than threshold free space)
func (lp *LeafPage) IsFull(threshold int) bool {
	return lp.AvailableSpace() < threshold
}

// NumRecords returns number of records
func (lp *LeafPage) NumRecords() int {
	return int(lp.page.Header.NumKeys)
}

// String returns string representation
func (lp *LeafPage) String() string {
	return fmt.Sprintf("LeafPage{NumKeys: %d, AvailableSpace: %d bytes}",
		lp.page.Header.NumKeys, lp.AvailableSpace())
}
