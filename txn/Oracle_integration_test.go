package txn

import (
	"github.com/stretchr/testify/assert"
	"snapshot-isolation/mvcc"
	"testing"
	"time"
)

func TestBeginTimestampMarkWithASingleTransaction(t *testing.T) {
	oracle := NewOracle()
	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(mvcc.NewMemTable(10), oracle)
	transaction.Get([]byte("HDD"))

	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(0), beginMark.DoneTill())
}

func TestBeginTimestampMarkWithTwoTransactions(t *testing.T) {
	oracle := NewOracle()
	memTable := mvcc.NewMemTable(10)

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(memTable, oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(memTable, oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())
}

func TestCleanUpOfCommittedTransactions(t *testing.T) {
	oracle := NewOracle()
	memTable := mvcc.NewMemTable(10)

	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(memTable, oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(memTable, oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())

	thirdTransaction := NewReadWriteTransaction(memTable, oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(thirdTransaction)
	assert.Equal(t, uint64(3), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(2), beginMark.DoneTill())

	committedTransactions := oracle.committedTransactions
	assert.Equal(t, 1, len(committedTransactions))
	assert.Equal(t, uint64(3), committedTransactions[0].commitTimestamp)
}
