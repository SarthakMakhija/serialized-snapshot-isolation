package mvcc

import "snapshot-isolation/utils"

type MemTable struct {
	head           *SkiplistNode
	levelGenerator utils.LevelGenerator
}

func newMemTable(maxLevel int) *MemTable {
	return &MemTable{
		head:           newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel),
		levelGenerator: utils.NewLevelGenerator(maxLevel),
	}
}

func (memTable *MemTable) put(key VersionedKey, value Value) {
	memTable.head.put(key, value, memTable.levelGenerator)
}

func (memTable *MemTable) get(key VersionedKey) (Value, bool) {
	return memTable.head.get(key)
}
