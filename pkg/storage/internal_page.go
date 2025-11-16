package storage

import (
	"encoding/binary"
	"fmt"
)

// InternalPage represents a B+ Tree internal node
// Layout: [leftmost_ptr: 8 bytes][key1: 4 bytes][ptr1: 8 bytes][key2: 4 bytes][ptr2: 8 bytes]...
// Structure: P0 | K1 P1 | K2 P2 | K3 P3 | ...
// Where P0 is for keys < K1, P1 for [K1, K2), P2 for [K2, K3), etc.
type InternalPage struct {
	page *Page
}

// InternalEntry represents a key-pointer pair
type InternalEntry struct {
	Key    uint32
	PageID uint64
}

// NewInternalPage creates a new internal page
func NewInternalPage(page *Page) *InternalPage {
	if page.Header.PageType != PageTypeInternal {
		panic("page must be of type Internal")
	}
	return &InternalPage{page: page}
}

// GetLeftmostPointer returns the leftmost child pointer (P0)
func (ip *InternalPage) GetLeftmostPointer() (uint64, error) {
	if len(ip.page.Data) < 8 {
		return 0, fmt.Errorf("insufficient data for leftmost pointer")
	}
	ptr := binary.LittleEndian.Uint64(ip.page.Data[0:8])
	return ptr, nil
}

// SetLeftmostPointer sets the leftmost child pointer
func (ip *InternalPage) SetLeftmostPointer(pageID uint64) error {
	if len(ip.page.Data) < 8 {
		return fmt.Errorf("insufficient data for leftmost pointer")
	}
	binary.LittleEndian.PutUint64(ip.page.Data[0:8], pageID)
	return nil
}

// GetKeyPointer returns the key and pointer at index (0-based)
// index 0 returns key[0] and pointer[1]
// index i returns key[i] and pointer[i+1]
func (ip *InternalPage) GetKeyPointer(index int) (uint32, uint64, error) {
	if index < 0 || index >= int(ip.page.Header.NumKeys) {
		return 0, 0, fmt.Errorf("index %d out of bounds", index)
	}

	// Offset: leftmost_ptr(8) + index * (key(4) + ptr(8))
	offset := 8 + index*12

	if offset+12 > len(ip.page.Data) {
		return 0, 0, fmt.Errorf("insufficient data at offset %d", offset)
	}

	key := binary.LittleEndian.Uint32(ip.page.Data[offset : offset+4])
	ptr := binary.LittleEndian.Uint64(ip.page.Data[offset+4 : offset+12])

	return key, ptr, nil
}

// SetKeyPointer sets key and pointer at index
func (ip *InternalPage) SetKeyPointer(index int, key uint32, pageID uint64) error {
	// Allow setting at NumKeys position for insertion
	if index < 0 || index > int(ip.page.Header.NumKeys) {
		return fmt.Errorf("index %d out of bounds", index)
	}

	offset := 8 + index*12

	if offset+12 > len(ip.page.Data) {
		return fmt.Errorf("insufficient data at offset %d", offset)
	}

	binary.LittleEndian.PutUint32(ip.page.Data[offset:offset+4], key)
	binary.LittleEndian.PutUint64(ip.page.Data[offset+4:offset+12], pageID)

	return nil
}

// InsertEntry inserts a key-pointer pair at the correct position
func (ip *InternalPage) InsertEntry(key uint32, pageID uint64) error {
	// Check space (12 bytes per entry = 4 bytes key + 8 bytes pointer)
	maxEntries := (len(ip.page.Data) - 8) / 12 // -8 for leftmost ptr
	if int(ip.page.Header.NumKeys) >= maxEntries {
		return fmt.Errorf("internal page full")
	}

	// Find insert position (keep keys sorted)
	insertPos := ip.findInsertPosition(key)

	// Shift entries to make room
	for i := int(ip.page.Header.NumKeys) - 1; i >= insertPos; i-- {
		k, p, _ := ip.GetKeyPointer(i)
		ip.SetKeyPointer(i+1, k, p)
	}

	// Insert new entry
	ip.SetKeyPointer(insertPos, key, pageID)
	ip.page.Header.NumKeys++

	return nil
}

// findInsertPosition finds where to insert key to maintain sorted order
func (ip *InternalPage) findInsertPosition(key uint32) int {
	for i := 0; i < int(ip.page.Header.NumKeys); i++ {
		k, _, err := ip.GetKeyPointer(i)
		if err != nil {
			break
		}
		if key < k {
			return i
		}
	}
	return int(ip.page.Header.NumKeys)
}

// SearchChild finds the child page ID for a given key
// Logic: For keys [K1, K2, K3] and pointers [P0, P1, P2, P3]
// - key < K1 → P0
// - K1 <= key < K2 → P1
// - K2 <= key < K3 → P2
// - key >= K3 → P3
func (ip *InternalPage) SearchChild(key uint32) (uint64, error) {
	// If no keys, return leftmost pointer
	if ip.page.Header.NumKeys == 0 {
		return ip.GetLeftmostPointer()
	}

	// Search through keys to find the correct child
	for i := 0; i < int(ip.page.Header.NumKeys); i++ {
		k, ptr, err := ip.GetKeyPointer(i)
		if err != nil {
			return 0, err
		}

		// If search key < current key, return appropriate pointer
		if key < k {
			if i == 0 {
				// key < K1, return P0 (leftmost)
				return ip.GetLeftmostPointer()
			}
			// This case shouldn't happen because we already processed previous keys
			// But for safety, return leftmost
			return ip.GetLeftmostPointer()
		}

		// If this is the last key
		if i == int(ip.page.Header.NumKeys)-1 {
			// key >= last key, return last pointer
			return ptr, nil
		}

		// Check next key to determine range
		nextKey, _, err := ip.GetKeyPointer(i + 1)
		if err != nil {
			// If error reading next key, return current pointer
			return ptr, nil
		}

		// If key is in range [Ki, Ki+1), return Pi
		if key >= k && key < nextKey {
			return ptr, nil
		}
	}

	// Fallback: should not reach here, but return leftmost for safety
	return ip.GetLeftmostPointer()
}

// NumKeys returns number of keys
func (ip *InternalPage) NumKeys() int {
	return int(ip.page.Header.NumKeys)
}

// String returns string representation
func (ip *InternalPage) String() string {
	return fmt.Sprintf("InternalPage{NumKeys: %d}", ip.page.Header.NumKeys)
}
