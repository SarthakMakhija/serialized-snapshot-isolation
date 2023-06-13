package txn

import (
	txnErrors "serialized-snapshot-isolation/txn/errors"
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
	transactionExecutor   *TransactionExecutor
	beginTimestampMark    *TransactionBeginTimestampMark
	committedTransactions []CommittedTransaction
}

func NewOracle(transactionExecutor *TransactionExecutor) *Oracle {
	return &Oracle{
		nextTimestamp:       1,
		transactionExecutor: transactionExecutor,
		beginTimestampMark:  NewTransactionBeginTimestampMark(),
	}
}

func (oracle *Oracle) CommittedTransactionLength() int {
	return len(oracle.committedTransactions)
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

	oracle.finishBeginTimestampForReadWriteTransaction(transaction)
	oracle.cleanupCommittedTransactions()

	commitTimestamp := oracle.nextTimestamp
	oracle.nextTimestamp = oracle.nextTimestamp + 1

	oracle.trackReadyToCommitTransaction(transaction, commitTimestamp)
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

func (oracle *Oracle) finishBeginTimestampForReadWriteTransaction(transaction *ReadWriteTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

func (oracle *Oracle) finishBeginTimestampForReadonlyTransaction(transaction *ReadonlyTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

func (oracle *Oracle) cleanupCommittedTransactions() {
	updatedCommittedTransactions := oracle.committedTransactions[:0]
	maxBeginTransactionTimestamp := oracle.beginTimestampMark.DoneTill()

	for _, transaction := range oracle.committedTransactions {
		if transaction.commitTimestamp <= maxBeginTransactionTimestamp {
			continue
		}
		updatedCommittedTransactions = append(updatedCommittedTransactions, transaction)
	}
	oracle.committedTransactions = updatedCommittedTransactions
}

func (oracle *Oracle) trackReadyToCommitTransaction(transaction *ReadWriteTransaction, commitTimestamp uint64) {
	oracle.committedTransactions = append(oracle.committedTransactions, CommittedTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
