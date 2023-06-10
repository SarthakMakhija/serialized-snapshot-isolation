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

func (memTable *MemTable) PutOrUpdate(key VersionedKey, value Value) {
	memTable.head.putOrUpdate(key, value, memTable.levelGenerator)
}

func (memTable *MemTable) Get(key VersionedKey) (Value, bool) {
	return memTable.head.get(key)
}
