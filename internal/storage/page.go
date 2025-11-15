package storage

import (
	"encoding/binary"
	"fmt"
)

// PageType define type of page
type PageType uint16

const (
	PageTypeFree     PageType = 0 // Page is empty
	PageTypeInternal PageType = 1 // Internal node of B+ tree
	PageTypeLeaf     PageType = 2 // Leaf node of B+ Tree
)

func (pt PageType) String() string {
	switch pt {
	case PageTypeFree:
		return "Free"
	case PageTypeInternal:
		return "Internal"
	case PageTypeLeaf:
		return "Leaf"
	default:
		return "Unknown"
	}
}

const (
	PageHeaderSize = 16 // Header size in bytes
)

// PageHeader store metadata of page
type PageHeader struct {
	PageType PageType // 2 bytes - page type
	NumKeys  uint16   // 2 bytes - number of keys in page
	NextPage uint32   // 4 bytes - pointer to next page (used for leaf linked list)
	Parent   uint32   // 4 bytes - pointer to parent page
	// 4 bytes reserved for padding
}

// Page stand for a page 4096 byte = 4 KB
type Page struct {
	Header PageHeader
	Data   []byte
}

// NewPage create a new page with defined type
func NewPage(pageType PageType) *Page {
	return &Page{
		Header: PageHeader{
			PageType: pageType,
			NumKeys:  0,
			NextPage: 0,
			Parent:   0,
		},
		Data: make([]byte, PageSize-PageHeaderSize),
	}
}

// Serilaize change Page to []byte to write in disk
func (p *Page) Serialize() []byte {
	buf := make([]byte, PageSize)

	// Serialize header
	binary.LittleEndian.PutUint16(buf[0:2], uint16(p.Header.PageType))
	binary.LittleEndian.PutUint16(buf[2:4], p.Header.NumKeys)
	binary.LittleEndian.PutUint32(buf[4:8], p.Header.NextPage)
	binary.LittleEndian.PutUint32(buf[8:12], p.Header.Parent)
	// bytes 12-16: reserved (padding)

	// Copy data
	copy(buf[PageHeaderSize:], p.Data)

	return buf
}

// Deserialize change from []byte in disk to Page
func DeserializePage(data []byte) (*Page, error) {
	if len(data) != PageSize {
		return nil, fmt.Errorf("invalid page size: %d, expected %d", len(data), PageSize)
	}

	page := &Page{
		Data: make([]byte, PageSize-PageHeaderSize),
	}

	// Deserialize header
	page.Header.PageType = PageType(binary.LittleEndian.Uint16(data[0:2]))
	page.Header.NumKeys = binary.LittleEndian.Uint16(data[2:4])
	page.Header.NextPage = binary.LittleEndian.Uint32(data[4:8])
	page.Header.Parent = binary.LittleEndian.Uint32(data[8:12])

	// Copy data
	copy(page.Data, data[PageHeaderSize:])

	return page, nil
}

// IsFull check if page is full
func (p *Page) IsFull(recordSize int) bool {
	// Calculate remaining space in the page
	usedSpace := int(p.Header.NumKeys) * recordSize
	return usedSpace+recordSize > len(p.Data)
}

// IsLeaf check if page is leaf node
func (p *Page) IsLeaf() bool {
	return p.Header.PageType == PageTypeLeaf
}

// IsInternal check if page is internal node
func (p *Page) IsInternal() bool {
	return p.Header.PageType == PageTypeInternal
}

// IsFree check if page is free page
func (p *Page) IsFree() bool {
	return p.Header.PageType == PageTypeFree
}

// String return string representation of page
func (p *Page) String() string {
	return fmt.Sprintf("Page{Type: %s, NumKeys: %d, NextPage: %d, Parent: %d}",
		p.Header.PageType,
		p.Header.NumKeys,
		p.Header.NextPage,
		p.Header.Parent)
}
