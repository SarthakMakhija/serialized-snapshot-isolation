package mvcc

import (
	"serialized-snapshot-isolation/mvcc/utils"
	"sync"
)

// MemTable is an in-memory structure built on top of SkipList.
type MemTable struct {
	lock           sync.RWMutex
	head           *SkiplistNode
	levelGenerator utils.LevelGenerator
}

// NewMemTable creates a new instance of MemTable.
func NewMemTable(maxLevel uint8) *MemTable {
	return &MemTable{
		head:           newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel),
		levelGenerator: utils.NewLevelGenerator(maxLevel),
	}
}

// PutOrUpdate puts or updates the key and the value pair in the SkipList.
func (memTable *MemTable) PutOrUpdate(key VersionedKey, value Value) {
	memTable.lock.Lock()
	defer memTable.lock.Unlock()

	memTable.head.putOrUpdate(key, value, memTable.levelGenerator)
}

// Get returns a pair of (Value, bool) for the incoming key.
// It returns (Value, true) if the value exists for the incoming key, else (nil, false).
func (memTable *MemTable) Get(key VersionedKey) (Value, bool) {
	memTable.lock.RLock()
	defer memTable.lock.RUnlock()

	return memTable.head.get(key)
}
