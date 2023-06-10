package txn

import (
	"github.com/stretchr/testify/assert"
	"snapshot-isolation/mvcc"
	"testing"
)

func TestGetsCommitTimestampForTransactionGivenNoTransactionsAreCurrentlyTracked(t *testing.T) {
	oracle := NewOracle()

	transaction := NewReadWriteTransaction(1, mvcc.NewMemTable(10))
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)
}

func TestGetsCommitTimestampFor2Transactions(t *testing.T) {
	oracle := NewOracle()

	memtable := mvcc.NewMemTable(10)
	aTransaction := NewReadWriteTransaction(1, memtable)
	aTransaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(1, memtable)
	anotherTransaction.Get([]byte("SSD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestGetsCommitTimestampFor2TransactionsGivenOneTransactionReadTheKeyThatTheOtherWrites(t *testing.T) {
	oracle := NewOracle()

	memtable := mvcc.NewMemTable(10)
	aTransaction := NewReadWriteTransaction(1, memtable)
	aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(1, memtable)
	anotherTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestErrorsForOneTransaction(t *testing.T) {
	oracle := NewOracle()

	memtable := mvcc.NewMemTable(10)
	aTransaction := NewReadWriteTransaction(0, memtable)
	aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(0, memtable)
	anotherTransaction.Get([]byte("HDD"))

	_, err := oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Error(t, err)
	assert.Equal(t, ConflictErr, err)
}
