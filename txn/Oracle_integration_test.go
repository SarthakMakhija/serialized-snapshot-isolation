package txn

import (
	"github.com/stretchr/testify/assert"
	"serialized-snapshot-isolation/mvcc"
	"testing"
	"time"
)

func TestBeginTimestampMarkWithASingleTransaction(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(oracle)
	transaction.Get([]byte("HDD"))
	transaction.FinishBeginTimestampForReadWriteTransaction()

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)

	assert.Equal(t, uint64(1), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), beginMark.DoneTill())
}

func TestBeginTimestampMarkWithTwoTransactions(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)

	oracle.commitTimestampMark.Finish(commitTimestamp)
	transaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	anotherTransaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())
}

func TestCleanUpOfCommittedTransactions(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	oracle := NewOracle(NewTransactionExecutor(memTable))

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	transaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	anotherTransaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())

	thirdTransaction := NewReadWriteTransaction(oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(thirdTransaction)
	oracle.commitTimestampMark.Finish(commitTimestamp)
	thirdTransaction.FinishBeginTimestampForReadWriteTransaction()
	assert.Equal(t, uint64(3), commitTimestamp)

	time.Sleep(15 * time.Millisecond)
	assert.Equal(t, uint64(2), beginMark.DoneTill())

	committedTransactions := oracle.committedTransactions
	assert.Equal(t, 1, len(committedTransactions))
	assert.Equal(t, uint64(3), committedTransactions[0].commitTimestamp)
}
