package storage

import (
	"fmt"
	"os"
)

const (
	FreeListPageID = 0
)

// FilePager implement Pager interface using file system
type FilePager struct {
	file     *os.File
	numPages uint64
	freeList *FreeList
}

// NewFilePager create nerw or open database file
func NewFilePager(path string) (*FilePager, error) {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Take file size to calculate number of page
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := uint64(stat.Size()) / PageSize

	pager := &FilePager{
		file:     file,
		numPages: numPages,
		freeList: NewFreeList(),
	}

	if numPages == 0 {
		if err := pager.initializeFreeList(); err != nil {
			file.Close()
			return nil, err
		}
	} else {
		if err := pager.loadFreeList(); err != nil {
			file.Close()
			return nil, err
		}
	}

	return pager, nil
}

// initializeFreeList creat new free list page (page 0)
func (p *FilePager) initializeFreeList() error {
	page := p.freeList.SerializeToPage()

	// Allocate page 0 without using AllocatePage (avoid recursion)
	emptyPage := make([]byte, PageSize)
	if _, err := p.file.WriteAt(emptyPage, 0); err != nil {
		return fmt.Errorf("failed to initialize free list: %w", err)
	}

	p.numPages = 1

	// Write free list data
	return p.WritePageStruct(FreeListPageID, page)
}

// loadFreeList read free list from disk
func (p *FilePager) loadFreeList() error {
	page, err := p.ReadPageStruct(FreeListPageID)
	if err != nil {
		return fmt.Errorf("failed to read free list: %w", err)
	}

	freeList, err := DeserializeFreeList(page)
	if err != nil {
		return fmt.Errorf("failed to deserialize free list: %w", err)
	}

	p.freeList = freeList
	return nil
}

// saveFreeList write free list to disk
func (p *FilePager) saveFreeList() error {
	page := p.freeList.SerializeToPage()
	return p.WritePageStruct(FreeListPageID, page)
}

// FreePage mark page is free and add into free list
func (p *FilePager) FreePage(pageID uint64) error {
	if pageID == FreeListPageID {
		return fmt.Errorf("cannot free the free list page")
	}

	if pageID >= p.numPages {
		return fmt.Errorf("page %d out of bounds", pageID)
	}

	// Thêm vào free list
	p.freeList.Push(pageID)

	// Lưu free list
	return p.saveFreeList()
}

// FreeListSize trả về số lượng free pages
func (p *FilePager) FreeListSize() int {
	return p.freeList.Size()
}

func (p *FilePager) ReadPage(id uint64) ([]byte, error) {
	if id >= p.numPages {
		return nil, fmt.Errorf("page %d out of bounds", id)
	}

	buf := make([]byte, PageSize)
	offset := int64(id * PageSize)

	_, err := p.file.ReadAt(buf, offset)
	if err != nil {
		return nil, fmt.Errorf("failed to read page %d: %w", id, err)
	}

	return buf, nil
}

func (p *FilePager) WritePage(id uint64, data []byte) error {
	if len(data) != PageSize {
		return fmt.Errorf("invalid page size: %d, expected %d", len(data), PageSize)
	}

	offset := int64(id * PageSize)

	_, err := p.file.WriteAt(data, offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", id, err)
	}

	return p.file.Sync()
}

func (p *FilePager) AllocatePage() (uint64, error) {
	pageID := p.numPages
	p.numPages++

	emptyPage := make([]byte, PageSize)
	if err := p.WritePage(pageID, emptyPage); err != nil {
		p.numPages-- // rollback
		return 0, err
	}

	return pageID, nil
}

func (p *FilePager) Close() error {
	if p.file != nil {
		return p.file.Close()
	}

	return nil
}

// ReadPageStruct read page and deserialize into Page struct
func (p *FilePager) ReadPageStruct(id uint64) (*Page, error) {
	data, err := p.ReadPage(id)
	if err != nil {
		return nil, err
	}

	return DeserializePage(data)
}

// WritePageStruct serialize and write Page struct into disk
func (p *FilePager) WritePageStruct(id uint64, page *Page) error {
	data := page.Serialize()
	return p.WritePage(id, data)
}

// AllocatePageWithType allocate new page with defined type
func (p *FilePager) AllocatePageWithType(pageType PageType) (uint64, *Page, error) {
	pageID, err := p.AllocatePage()
	if err != nil {
		return 0, nil, err
	}

	page := NewPage(pageType)
	if err := p.WritePageStruct(pageID, page); err != nil {
		return 0, nil, err
	}

	return pageID, page, nil
}

func (p *FilePager) NumPages() uint64 {
	return p.numPages
}
