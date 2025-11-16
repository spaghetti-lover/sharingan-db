package storage

import (
	"encoding/binary"
	"fmt"
)

// FreeList manage deleted page for reused purpose
type FreeList struct {
	freePageIDs []uint64
}

// NewFreeList create new free list
func NewFreeList() *FreeList {
	return &FreeList{
		freePageIDs: make([]uint64, 0),
	}
}

// Pop a page ID from free list
func (fl *FreeList) Pop() (uint64, bool) {
	if len(fl.freePageIDs) == 0 {
		return 0, false
	}

	pageID := fl.freePageIDs[len(fl.freePageIDs)-1]
	fl.freePageIDs = fl.freePageIDs[:len(fl.freePageIDs)-1] // ✅ Fixed: remove -1
	return pageID, true
}

// Push a new page ID to free list
func (fl *FreeList) Push(pageID uint64) {
	fl.freePageIDs = append(fl.freePageIDs, pageID)
}

// IsEmpty check if free list is empty
func (fl *FreeList) IsEmpty() bool {
	return len(fl.freePageIDs) == 0
}

// Size return number of free pages
func (fl *FreeList) Size() int {
	return len(fl.freePageIDs)
}

// SerializeToPage change FreeList into Page to write to disk
func (fl *FreeList) SerializeToPage() *Page {
	page := NewPage(PageTypeFree)

	binary.LittleEndian.PutUint32(page.Data[0:4], uint32(len(fl.freePageIDs)))

	offset := 4
	for _, pageID := range fl.freePageIDs {
		binary.LittleEndian.PutUint64(page.Data[offset:offset+8], pageID)
		offset += 8
	}

	return page
}

// DeserializeFreeList read FreeList from Page
func DeserializeFreeList(page *Page) (*FreeList, error) {
	if page.Header.PageType != PageTypeFree {
		return nil, fmt.Errorf("invalid page type: %v, expected Free", page.Header.PageType)
	}

	fl := NewFreeList()

	count := binary.LittleEndian.Uint32(page.Data[0:4])

	offset := 4
	for i := uint32(0); i < count; i++ {
		pageID := binary.LittleEndian.Uint64(page.Data[offset : offset+8])
		fl.freePageIDs = append(fl.freePageIDs, pageID)
		offset += 8
	}

	return fl, nil
}

// MaxFreePageIDs calculate max number of page IDs that can be store in one page
func MaxFreePageIDs() int {
	return (PageSize - PageHeaderSize - 4) / 8
}
