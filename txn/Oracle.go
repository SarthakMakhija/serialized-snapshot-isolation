package txn

import (
	"context"
	txnErrors "serialized-snapshot-isolation/txn/errors"
	"sync"
)

// CommittedTransaction is a concurrently running ReadWriteTransaction which is ready to be committed.
type CommittedTransaction struct {
	commitTimestamp uint64
	transaction     *ReadWriteTransaction
}

// Oracle is the central authority that assigns begin and commit timestamp to the transactions.
// Every transaction gets a beginTimestamp and only a ReadWriteTransaction gets a commit timestamp.
// According to snapshot isolation (or serialized snapshot isolation), every transaction reads the keys where:
// commitTimestampOf(Key) < beginTimestampOf(transaction).
// The current implementation uses nextTimestamp which denotes the timestamp that will be assigned as the commit timestamp
// to the next transaction. The beginTimestamp is one less than the nextTimestamp.
type Oracle struct {
	lock                  sync.Mutex
	executorLock          sync.Mutex
	nextTimestamp         uint64
	transactionExecutor   *TransactionExecutor
	beginTimestampMark    *TransactionTimestampMark
	commitTimestampMark   *TransactionTimestampMark
	committedTransactions []CommittedTransaction
}

// NewOracle creates a new instance of Oracle. It is called once in the entire application.
// Oracle is initialized with nextTimestamp as 1.
// If we were to implement durability using WAL, we would load the value of the nextTimestamp from WAL.
// Every segment of WAL can contain the last commitTimestamp. In order to recover nextTimestamp, we can read the latest
// WAL segment (only the footer where we place the last commitTimestamp), get the last commitTimestamp and add 1 to it.
func NewOracle(transactionExecutor *TransactionExecutor) *Oracle {
	oracle := &Oracle{
		nextTimestamp:       1,
		transactionExecutor: transactionExecutor,
		beginTimestampMark:  NewTransactionTimestampMark(),
		commitTimestampMark: NewTransactionTimestampMark(),
	}

	oracle.beginTimestampMark.Finish(oracle.nextTimestamp - 1)
	oracle.commitTimestampMark.Finish(oracle.nextTimestamp - 1)
	return oracle
}

// CommittedTransactionLength returns the length of all the transactions that are committed and maintained in Oracle
func (oracle *Oracle) CommittedTransactionLength() int {
	return len(oracle.committedTransactions)
}

// beginTimestamp returns the beginTimestamp of a transaction.
// beginTimestamp = nextTimestamp - 1
func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.lock.Lock()
	beginTimestamp := oracle.nextTimestamp - 1
	oracle.beginTimestampMark.Begin(beginTimestamp)
	oracle.lock.Unlock()

	_ = oracle.commitTimestampMark.WaitForMark(context.Background(), beginTimestamp)
	return beginTimestamp
}

// mayBeCommitTimestampFor returns the commitTimestamp for a  transaction if there are no conflicts.
// A ReadWriteTransaction Tx conflicts with other transaction if:
// the keys read by the transaction Tx are modified by another transaction that has the commitTimestamp > beginTimestampOf(Tx).
// If there are no conflicts:
// 1. the current transaction is marked as `beginFinished` by invoking finishBeginTimestampForReadWriteTransaction.
// 2. committedTransactions are cleaned up.
// 3. commitTimestamp is assigned to the transaction and the nextTimestamp is increased by 1
// 4. The current transaction is tracked as CommittedTransaction
// The cleanup of committedTransactions removes all the committed transactions Ti...Tj where the commitTimestamp of Ti <= maxBeginTransactionTimestamp.
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
	oracle.commitTimestampMark.Begin(commitTimestamp)
	return commitTimestamp, nil
}

// hasConflictFor determines of the transaction has a conflict with other concurrent transactions.
// A ReadWriteTransaction Tx conflicts with other transaction if:
// the keys read by the transaction Tx are modified by another transaction that has the commitTimestamp > beginTimestampOf(Tx).
// ReadWriteTransaction tracks its read keys in the `reads` property.
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

// finishBeginTimestampForReadWriteTransaction indicates that the beginTimestamp of the transaction is finished.
// This is an indication to the TransactionTimestampMark that all the transactions upto a given `beginTimestamp`
// are done. This information will be used in cleaning up the committed transactions.
func (oracle *Oracle) finishBeginTimestampForReadWriteTransaction(transaction *ReadWriteTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

// finishBeginTimestampForReadonlyTransaction indicates that the beginTimestamp of the transaction is finished.
func (oracle *Oracle) finishBeginTimestampForReadonlyTransaction(transaction *ReadonlyTransaction) {
	oracle.beginTimestampMark.Finish(transaction.beginTimestamp)
}

// cleanupCommittedTransactions cleans up the committed transactions.
// In order to clean up the committed transactions we do the following:
// 1. Get the latest beginTimestampMark
// 2. For all the committed transactions, if the transaction.commitTimestamp <= maxBeginTransactionTimestamp, skip this transaction
// 3. Create a new array (or slice) of CommittedTransaction excluding the transactions from step 2.
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

// trackReadyToCommitTransaction tracks all the transactions that are ready to be committed.
func (oracle *Oracle) trackReadyToCommitTransaction(transaction *ReadWriteTransaction, commitTimestamp uint64) {
	oracle.committedTransactions = append(oracle.committedTransactions, CommittedTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
