package txn

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestEmptyBatch(t *testing.T) {
	batch := NewBatch()
	assert.Equal(t, true, batch.IsEmpty())
}

func TestNonEmptyBatch(t *testing.T) {
	batch := NewBatch()
	batch.Add([]byte("HDD"), []byte("Hard disk"))
	assert.Equal(t, false, batch.IsEmpty())
}

func TestGetTheValueOfAKeyFromBatch(t *testing.T) {
	batch := NewBatch()
	batch.Add([]byte("HDD"), []byte("Hard disk"))

	value, ok := batch.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value)
}

func TestGetTheValueOfANonExistingKeyFromBatch(t *testing.T) {
	batch := NewBatch()
	batch.Add([]byte("HDD"), []byte("Hard disk"))

	_, ok := batch.Get([]byte("non-existing"))
	assert.Equal(t, false, ok)
}

func TestContainsTheKey(t *testing.T) {
	batch := NewBatch()
	batch.Add([]byte("HDD"), []byte("Hard disk"))

	contains := batch.Contains([]byte("HDD"))
	assert.Equal(t, true, contains)
}

func TestDoesNotContainTheKey(t *testing.T) {
	batch := NewBatch()
	batch.Add([]byte("HDD"), []byte("Hard disk"))

	contains := batch.Contains([]byte("SSD"))
	assert.Equal(t, false, contains)
}

func TestGetsTheTimestampedBatch(t *testing.T) {
	batch := NewBatch()
	batch.Add([]byte("HDD"), []byte("Hard disk"))

	timestampedBatch := batch.ToTimestampedBatch(1)
	assert.Equal(t, uint64(1), timestampedBatch.timestamp)
	assert.Equal(t, []KeyValuePair{newKeyValuePair([]byte("HDD"), []byte("Hard disk"))}, timestampedBatch.batch.pairs)
}
