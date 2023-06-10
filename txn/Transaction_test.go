package txn

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

func TestGetsAnExistingKeyInAReadWriteTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadWriteTransaction(1, memTable)
	transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.PutOrUpdate([]byte("SSD"), []byte("Solid state disk"))

	done := transaction.Commit(2)
	<-done

	readonlyTransaction := NewReadonlyTransaction(2, memTable)

	value, ok := readonlyTransaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = readonlyTransaction.Get([]byte("SSD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Solid state disk"), value.Slice())

	_, ok = readonlyTransaction.Get([]byte("non-existing"))
	assert.Equal(t, false, ok)
}

func TestGetsTheValueFromAKeyInAReadWriteTransactionFromBatch(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadWriteTransaction(1, memTable)
	transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	value, ok := transaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	done := transaction.Commit(2)
	<-done
}

func TestTracksReadsInAReadWriteTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadWriteTransaction(1, memTable)
	transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("SSD"))

	done := transaction.Commit(2)
	<-done

	assert.Equal(t, 1, len(transaction.reads))
	key := transaction.reads[0]

	assert.Equal(t, []byte("SSD"), key)
}

func TestDoesNotTrackReadsInAReadWriteTransactionIfKeysAreReadFromTheBatch(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadWriteTransaction(1, memTable)
	transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("HDD"))

	done := transaction.Commit(2)
	<-done

	assert.Equal(t, 0, len(transaction.reads))
}
