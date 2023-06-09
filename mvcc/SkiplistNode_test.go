package mvcc

import (
	"github.com/stretchr/testify/assert"
	"snapshot-isolation/utils"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	key := newVersionedKey([]byte("HDD"), 1)
	value := newValue([]byte("Hard disk"))

	sentinelNode.put(key, value, utils.NewLevelGenerator(maxLevel))

	value, ok := sentinelNode.get(key)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.slice())
}

func TestPutsTheSameKeyWithADifferentVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.get(newVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.slice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.get(newVersionedKey([]byte("HDD"), 10))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.slice())
}

func TestGetsTheValueOfAKeyWithLatestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("SSD"), 1), newValue([]byte("Solid state drive")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("SSD"), 2), newValue([]byte("Solid State drive")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("SSD"), 3), newValue([]byte("Solid-State-drive")), levelGenerator)

	expected := make(map[uint64][]byte)
	expected[1] = []byte("Solid state drive")
	expected[2] = []byte("Solid State drive")
	expected[3] = []byte("Solid-State-drive")
	expected[5] = []byte("Solid-State-drive")

	for version, expectedValue := range expected {
		key := newVersionedKey([]byte("SSD"), version)
		value, ok := sentinelNode.get(key)

		assert.Equal(t, true, ok)
		assert.Equal(t, expectedValue, value.slice())
	}
}

func TestGetsTheValueForNonExistingKey(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")), levelGenerator)

	_, ok := sentinelNode.get(newVersionedKey([]byte("Storage"), 1))
	assert.Equal(t, false, ok)
}
