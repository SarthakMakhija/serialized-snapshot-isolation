package mvcc

import (
	"github.com/stretchr/testify/assert"
	"serialized-snapshot-isolation/mvcc/utils"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInNode(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewValue([]byte("Hard disk"))

	sentinelNode.putOrUpdate(key, value, utils.NewLevelGenerator(maxLevel))

	value, ok := sentinelNode.get(key)
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())
}

func TestUpdatesTheSameKeyWithADifferentVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)

	value, ok := sentinelNode.get(NewVersionedKey([]byte("HDD"), 10))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfAKeyWithLatestVersion(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("SSD"), 1), NewValue([]byte("Solid state drive")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("SSD"), 2), NewValue([]byte("Solid State drive")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("SSD"), 3), NewValue([]byte("Solid-State-drive")), levelGenerator)

	expected := make(map[uint64][]byte)
	expected[1] = []byte("Solid state drive")
	expected[2] = []byte("Solid State drive")
	expected[3] = []byte("Solid-State-drive")
	expected[5] = []byte("Solid-State-drive")

	for version, expectedValue := range expected {
		key := NewVersionedKey([]byte("SSD"), version)
		value, ok := sentinelNode.get(key)

		assert.Equal(t, true, ok)
		assert.Equal(t, expectedValue, value.Slice())
	}
}

func TestGetsTheValueForNonExistingKey(t *testing.T) {
	const maxLevel = 8
	sentinelNode := newSkiplistNode(emptyVersionedKey(), emptyValue(), maxLevel)

	levelGenerator := utils.NewLevelGenerator(maxLevel)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")), levelGenerator)
	sentinelNode.putOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")), levelGenerator)

	_, ok := sentinelNode.get(NewVersionedKey([]byte("Storage"), 1))
	assert.Equal(t, false, ok)
}
