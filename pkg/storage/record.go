package storage

import (
	"encoding/binary"
	"fmt"
)

// Record stand for a KV record
// Format: [KeySize: 4 bytes][Key: variable][ValueSize: 4 bytes][Value: variable]
type Record struct {
	Key   []byte
	Value []byte
}

func NewRecord(key, value []byte) *Record {
	return &Record{
		Key:   key,
		Value: value,
	}
}

func NewRecordFromInts(key uint32, value string) *Record {
	keyBytes := make([]byte, 4)
	binary.LittleEndian.PutUint32(keyBytes, key)
	return &Record{
		Key:   keyBytes,
		Value: []byte(value),
	}
}

func (r *Record) Size() int {
	// 4 bytes keySize + key + 4 bytes valueSize + value
	return 4 + len(r.Key) + 4 + len(r.Value)
}

func (r *Record) Serialize() []byte {
	size := r.Size()
	buf := make([]byte, size)

	offset := 0

	// Write key size
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Key)))
	offset += 4

	// Write key
	copy(buf[offset:offset+len(r.Key)], r.Key)
	offset += len(r.Key)

	// Write value size
	binary.LittleEndian.PutUint32(buf[offset:offset+4], uint32(len(r.Value)))
	offset += 4

	// Write value
	copy(buf[offset:offset+len(r.Value)], r.Value)

	return buf
}

func DeserializeRecord(data []byte) (*Record, int, error) {
	if len(data) < 8 {
		return nil, 0, fmt.Errorf("insufficient data for record header")
	}

	offset := 0

	// Read key size
	keySize := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(keySize) > len(data) {
		return nil, 0, fmt.Errorf("insufficient data for key")
	}

	// Read key
	key := make([]byte, keySize)
	copy(key, data[offset:offset+int(keySize)])
	offset += int(keySize)

	if offset+4 > len(data) {
		return nil, 0, fmt.Errorf("insufficient data for value size")
	}

	// Read value size
	valueSize := binary.LittleEndian.Uint32(data[offset : offset+4])
	offset += 4

	if offset+int(valueSize) > len(data) {
		return nil, 0, fmt.Errorf("insufficient data for value")
	}

	// Read value
	value := make([]byte, valueSize)
	copy(value, data[offset:offset+int(valueSize)])
	offset += int(valueSize)

	return &Record{
		Key:   key,
		Value: value,
	}, offset, nil
}

func (r *Record) GetKeyAsUint32() (uint32, error) {
	if len(r.Key) != 4 {
		return 0, fmt.Errorf("key is not 4 bytes")
	}
	return binary.LittleEndian.Uint32(r.Key), nil
}

func (r *Record) GetValueAsString() string {
	return string(r.Value)
}

func (r *Record) String() string {
	if len(r.Key) == 4 {
		key, _ := r.GetKeyAsUint32()
		return fmt.Sprintf("Record{Key: %d, Value: %s}", key, r.GetValueAsString())
	}
	return fmt.Sprintf("Record{Key: %x, Value: %s}", r.Key, r.GetValueAsString())
}

type RecordList struct {
	records []*Record
}

func NewRecordList() *RecordList {
	return &RecordList{
		records: make([]*Record, 0),
	}
}

func (rl *RecordList) Add(record *Record) {
	rl.records = append(rl.records, record)
}

func (rl *RecordList) Get(index int) *Record {
	if index < 0 || index >= len(rl.records) {
		return nil
	}
	return rl.records[index]
}

func (rl *RecordList) Size() int {
	return len(rl.records)
}

func (rl *RecordList) TotalSize() int {
	size := 0
	for _, record := range rl.records {
		size += record.Size()
	}
	return size
}

func (rl *RecordList) SerializeToPage(page *Page) error {
	offset := 0

	for _, record := range rl.records {
		serialized := record.Serialize()

		if offset+len(serialized) > len(page.Data) {
			return fmt.Errorf("page overflow: cannot fit all records")
		}

		copy(page.Data[offset:], serialized)
		offset += len(serialized)
	}

	page.Header.NumKeys = uint16(len(rl.records))
	return nil
}

func DeserializeRecordsFromPage(page *Page) (*RecordList, error) {
	rl := NewRecordList()
	offset := 0

	for i := uint16(0); i < page.Header.NumKeys; i++ {
		record, bytesRead, err := DeserializeRecord(page.Data[offset:])
		if err != nil {
			return nil, fmt.Errorf("failed to deserialize record %d: %w", i, err)
		}

		rl.Add(record)
		offset += bytesRead
	}

	return rl, nil
}
