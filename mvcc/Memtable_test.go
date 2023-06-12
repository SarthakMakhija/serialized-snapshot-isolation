package mvcc

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
)

func TestPutsAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	key := NewVersionedKey([]byte("HDD"), 1)
	value := NewValue([]byte("Hard disk"))
	memTable.PutOrUpdate(key, value)

	value, ok := memTable.Get(key)

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())
}

func TestPutsTheSameKeyWithADifferentVersionInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 2))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfAKeyWithTheNearestVersionInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 8))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestGetsTheValueOfANonExistingKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	_, ok := memTable.Get(NewVersionedKey([]byte("Storage"), 1))

	assert.Equal(t, false, ok)
}

func TestUpdatesAKeyValueAndGetByKeyInMemTable(t *testing.T) {
	memTable := NewMemTable(10)
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = memTable.Get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())
}

func TestPutsKeysValuesConcurrentlyInMemtable(t *testing.T) {
	memTable := NewMemTable(10)
	var wg sync.WaitGroup

	wg.Add(3)
	go func() {
		defer wg.Done()
		memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 1), NewValue([]byte("Hard disk")))
	}()
	go func() {
		defer wg.Done()
		memTable.PutOrUpdate(NewVersionedKey([]byte("HDD"), 2), NewValue([]byte("Hard disk drive")))
	}()
	go func() {
		defer wg.Done()
		memTable.PutOrUpdate(NewVersionedKey([]byte("SSD"), 1), NewValue([]byte("Solid state")))
	}()

	wg.Wait()

	value, ok := memTable.Get(NewVersionedKey([]byte("HDD"), 1))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = memTable.Get(NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())

	value, ok = memTable.Get(NewVersionedKey([]byte("SSD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Solid state"), value.Slice())
}
