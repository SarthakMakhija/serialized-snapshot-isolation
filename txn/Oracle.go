package txn

import (
	txnErrors "snapshot-isolation/txn/errors"
	"sync"
)

type CommittedTransaction struct {
	commitTimestamp uint64
	transaction     *ReadWriteTransaction
}

type Oracle struct {
	lock                  sync.Mutex
	executorLock          sync.Mutex
	nextTimestamp         uint64
	beginTimestampMark    *TransactionBeginTimestampMark
	committedTransactions []CommittedTransaction
}

// NewOracle
// TODO: nextTimestamp is initialized to 1, will change later
// TODO: committedTransactions need to be cleaned up
// TODO: needs to have a TransactionBeginTimestampMark
func NewOracle() *Oracle {
	return &Oracle{
		nextTimestamp:      1,
		beginTimestampMark: NewTransactionBeginTimestampMark(),
	}
}

func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.lock.Lock()
	defer oracle.lock.Unlock()

	beginTimestamp := oracle.nextTimestamp - 1
	oracle.beginTimestampMark.Begin(beginTimestamp)
	return beginTimestamp
}

func (oracle *Oracle) mayBeCommitTimestampFor(transaction *ReadWriteTransaction) (uint64, error) {
	oracle.lock.Lock()
	defer oracle.lock.Unlock()

	if oracle.hasConflictFor(transaction) {
		return 0, txnErrors.ConflictErr
	}

	oracle.doneBeginTimestamp(transaction)

	commitTimestamp := oracle.nextTimestamp
	oracle.nextTimestamp = oracle.nextTimestamp + 1

	oracle.trackReadyToCommitTransaction(commitTimestamp, transaction)
	return commitTimestamp, nil
}

func (oracle *Oracle) hasConflictFor(transaction *ReadWriteTransaction) bool {
	for _, committedTransaction := range oracle.committedTransactions {
		if committedTransaction.commitTimestamp <= transaction.beginTimestamp {
			continue
		}

		for _, key := range transaction.reads {
			if committedTransaction.transaction.batch.Contains(key) {
				return true
			}
		}
	}
	return false
}

func (oracle *Oracle) doneBeginTimestamp(transaction *ReadWriteTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

func (oracle *Oracle) trackReadyToCommitTransaction(commitTimestamp uint64, transaction *ReadWriteTransaction) {
	oracle.committedTransactions = append(oracle.committedTransactions, CommittedTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
