package mvcc

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewValue([]byte("Hard disk"))
	memTable.Put(key, value)

	value, ok := memTable.Get(key)

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())
}

func TestPutsTheSameKeyWithADifferentVersionInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.Put(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.Put(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 2))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersionInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.Put(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.Put(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 8))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfANonExistingKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.Put(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.Put(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	_, ok := memTable.Get(NewVersionedKey([]byte("Storage"), 1))

	assert.Equal(t, false, ok)
}

func TestUpdatesAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.Put(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.Update(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = memTable.Get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}
