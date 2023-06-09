package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := newMemTable(10)
	key := newVersionedKey([]byte("HDD"), 1)
	value := newValue([]byte("Hard disk"))
	memTable.put(key, value)

	value, ok := memTable.get(key)

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.slice())
}

func TestPutsTheSameKeyWithADifferentVersionInMemTable(t *testing.T) {
	memTable := newMemTable(10)
	memTable.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")))
	memTable.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")))

	value, ok := memTable.get(newVersionedKey([]byte("HDD"), 2))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.slice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersionInMemTable(t *testing.T) {
	memTable := newMemTable(10)
	memTable.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")))
	memTable.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")))

	value, ok := memTable.get(newVersionedKey([]byte("HDD"), 8))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.slice())
}

func TestGetsTheValueOfANonExistingKeyInMemTable(t *testing.T) {
	memTable := newMemTable(10)
	memTable.put(newVersionedKey([]byte("HDD"), 1), newValue([]byte("Hard disk")))
	memTable.put(newVersionedKey([]byte("HDD"), 2), newValue([]byte("Hard disk drive")))

	_, ok := memTable.get(newVersionedKey([]byte("Storage"), 1))

	assert.Equal(t, false, ok)
}
