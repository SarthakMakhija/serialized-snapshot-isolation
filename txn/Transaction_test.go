package txn

import (
	"github.com/stretchr/testify/assert"
	"serialized-snapshot-isolation/mvcc"
	"serialized-snapshot-isolation/txn/errors"
	"testing"
)

func TestGetsANonExistingKeyInAReadonlyTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadonlyTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_, ok := transaction.Get([]byte("non-existing"))

	assert.Equal(t, false, ok)
}

func TestGetsAnExistingKeyInAReadonlyTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	memTable.PutOrUpdate(mvcc.NewVersionedKey([]byte("HDD"), 1), mvcc.NewValue([]byte("Hard disk")))

	transaction := NewReadonlyTransaction(NewOracle(NewTransactionExecutor(memTable)))
	value, ok := transaction.Get([]byte("HDD"))

	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())
}

func TestCommitsAnEmptyReadWriteTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	oracle := NewOracle(NewTransactionExecutor(memTable))
	transaction := NewReadWriteTransaction(oracle)

	_, err := transaction.Commit()

	assert.Error(t, err)
	assert.Equal(t, errors.EmptyTransactionErr, err)
}

func TestAttemptsToPutDuplicateKeysInATransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	oracle := NewOracle(NewTransactionExecutor(memTable))
	transaction := NewReadWriteTransaction(oracle)

	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	err := transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk drive"))

	assert.Error(t, err)
	assert.Equal(t, errors.DuplicateKeyInBatchErr, err)
}

func TestGetsAnExistingKeyInAReadWriteTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	oracle := NewOracle(NewTransactionExecutor(memTable))
	transaction := NewReadWriteTransaction(oracle)
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	_ = transaction.PutOrUpdate([]byte("SSD"), []byte("Solid state disk"))

	done, _ := transaction.Commit()
	<-done

	readonlyTransaction := NewReadonlyTransaction(oracle)

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

	transaction := NewReadWriteTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	value, ok := transaction.Get([]byte("HDD"))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	done, _ := transaction.Commit()
	<-done
}

func TestTracksReadsInAReadWriteTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadWriteTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("SSD"))

	done, _ := transaction.Commit()
	<-done

	assert.Equal(t, 1, len(transaction.reads))
	key := transaction.reads[0]

	assert.Equal(t, []byte("SSD"), key)
}

func TestDoesNotTrackReadsInAReadWriteTransactionIfKeysAreReadFromTheBatch(t *testing.T) {
	memTable := mvcc.NewMemTable(10)

	transaction := NewReadWriteTransaction(NewOracle(NewTransactionExecutor(memTable)))
	_ = transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	transaction.Get([]byte("HDD"))

	done, _ := transaction.Commit()
	<-done

	assert.Equal(t, 0, len(transaction.reads))
}
