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
	beginMark := oracle.beginTimestampMark

	transaction := NewReadWriteTransaction(mvcc.NewMemTable(10), oracle)
	commitTimestamp, _ := oracle.mayBeCommitTimestampFor(transaction)
	assert.Equal(t, uint64(1), commitTimestamp)

	anotherTransaction := NewReadWriteTransaction(mvcc.NewMemTable(10), oracle)
	commitTimestamp, _ = oracle.mayBeCommitTimestampFor(anotherTransaction)
	assert.Equal(t, uint64(2), commitTimestamp)

	time.Sleep(10 * time.Millisecond)
	assert.Equal(t, uint64(1), beginMark.DoneTill())
}
