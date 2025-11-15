package storage

// Pager interface manage read/write page into databse file
type Pager interface {
	// ReadPage read a page from file according to ID
	ReadPage(id uint64) ([]byte, error)
	// WritePage write a page to file according to ID
	WritePage(id uint64, data []byte) error
	// AllocatePage allocate a new page and return ID
	AllocatePage() (uint64, error)
	// Close closes database file
	Close() error
}

const PageSize = 4096