package txn

import (
	"github.com/stretchr/testify/assert"
	"serialized-snapshot-isolation/mvcc"
	"serialized-snapshot-isolation/txn/errors"
	"testing"
)

func TestGetsTheBeginTimestamp(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))
	assert.Equal(t, uint64(0), oracle.beginTimestamp())
}

func TestGetsTheBeginTimestampAfterACommit(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	assert.Equal(t, uint64(1), oracle.beginTimestamp())
}

func TestGetsCommitTimestampForTransactionGivenNoTransactionsAreCurrentlyTracked(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)
}

func TestGetsCommitTimestampFor2Transactions(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	aTransaction := NewReadWriteTransaction(oracle)
	aTransaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.Get([]byte("SSD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestGetsCommitTimestampFor2TransactionsGivenOneTransactionReadTheKeyThatTheOtherWrites(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	aTransaction := NewReadWriteTransaction(oracle)
	aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)
}

func TestErrorsForOneTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	aTransaction := NewReadWriteTransaction(oracle)
	aTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(aTransaction)
	assert.Equal(t, uint64(1), commitTimestamp)
	assert.Equal(t, 1, len(oracle.committedTransactions))

	anotherTransaction := NewReadWriteTransaction(oracle)
	anotherTransaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk drive"))
	anotherTransaction.Get([]byte("HDD"))

	thirdTransaction := NewReadWriteTransaction(oracle)
	thirdTransaction.Get([]byte("HDD"))

	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)

	_, err := oracle.mayBeCommitTimestampFor(thirdTransaction)
	assert.Error(t, err)
	assert.Equal(t, errors.ConflictErr, err)
}
