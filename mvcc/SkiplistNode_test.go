package mvcc

import (
	"github.com/stretchr/testify/assert"
	"snapshot-isolation/utils"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	key := newVersionedKey([]byte("HDD"), 1)
	value := newValue([]byte("Hard disk"))

	sentinelNode.Put(key, value, utils.NewLevelGenerator(maxLevel))

	value, ok := sentinelNode.Get(key)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.slice())
}

func TestPutsTheSameKeyWithADifferentVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.Get(newVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.slice())
}

func TestGetTheValueOfAKeyWithTheNearestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.Get(newVersionedKey([]byte("HDD"), 10))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.slice())
}

func TestGetTheValueOfAKeyWithLatestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("SSD"), 1), newValue([]byte("Solid state drive")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("SSD"), 2), newValue([]byte("Solid State drive")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("SSD"), 3), newValue([]byte("Solid-State-drive")), levelGenerator)

	expected := make(map[uint64][]byte)
	expected[1] = []byte("Solid state drive")
	expected[2] = []byte("Solid State drive")
	expected[3] = []byte("Solid-State-drive")
	expected[5] = []byte("Solid-State-drive")

	for version, expectedValue := range expected {
		key := newVersionedKey([]byte("SSD"), version)
		value, ok := sentinelNode.Get(key)

		assert.Equal(t, true, ok)
		assert.Equal(t, expectedValue, value.slice())
	}
}

func TestGetTheValueForNonExistingKey(t *testing.T) {
	const maxLevel = 8
	sentinelNode := NewSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.Put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)

	_, ok := sentinelNode.Get(newVersionedKey([]byte("Storage"), 1))
	assert.Equal(t, false, ok)
}
