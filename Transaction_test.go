package snapshot_isolation

import (
	"github.com/stretchr/testify/assert"
	"snapshot-isolation/mvcc"
	"testing"
)

func TestGetsANonExistingKeyInAReadonlyTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadonlyTransaction(1, memTable)
	_, ok := transaction.Get([]byte("non-existing"))

	assert.Equal(t, false, ok)
}

func TestGetsAnExistingKeyInAReadonlyTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	memTable.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))

	transaction := NewReadonlyTransaction(3, memTable)
	value, ok := transaction.Get([]byte("HDD"))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())
}
