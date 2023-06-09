package mvcc

import (
	"snapshot-isolation/mvcc/utils"
)

type MemTable struct {
	head           *SkiplistNode
	levelGenerator utils.LevelGenerator
}

func NewMemTable(maxLevel int) *MemTable {
	return &MemTable{
		head:           newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel),
		levelGenerator: utils.NewLevelGenerator(maxLevel),
	}
}

func (memTable *MemTable) Put(key VersionedKey, value Value) {
	memTable.head.put(key, value, memTable.levelGenerator)
}

func (memTable *MemTable) Update(key VersionedKey, value Value) {
	memTable.Put(key, value)
}

func (memTable *MemTable) Get(key VersionedKey) (Value, bool) {
	return memTable.head.get(key)
}
