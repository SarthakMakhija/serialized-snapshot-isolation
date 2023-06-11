package txn

import (
	"errors"
	"sync"
)

var ConflictErr = errors.New("transaction conflicts with other")
var EmptyTransactionErr = errors.New("transaction is empty, invoke PutOrUpdate in a transaction before committing")

type CommittedTransaction struct {
	commitTimestamp uint64
	transaction     *ReadWriteTransaction
}

type Oracle struct {
	lock                  sync.Mutex
	executorLock          sync.Mutex
	nextTimestamp         uint64
	committedTransactions []CommittedTransaction
}

// NewOracle
// TODO: nextTimestamp is initialized to 1, will change later
// TODO: committedTransactions need to be cleaned up
func NewOracle() *Oracle {
	return &Oracle{
		nextTimestamp: 1,
	}
}

func (oracle *Oracle) beginTimestamp() uint64 {
	oracle.lock.Lock()
	defer oracle.lock.Unlock()

	//TODO: may be wait to ensure that the commits are done upto this point
	beginTimestamp := oracle.nextTimestamp - 1
	return beginTimestamp
}

func (oracle *Oracle) mayBeCommitTimestampFor(transaction *ReadWriteTransaction) (uint64, error) {
	oracle.lock.Lock()
	defer oracle.lock.Unlock()

	if oracle.hasConflictFor(transaction) {
		return 0, ConflictErr
	}

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

func (oracle *Oracle) trackReadyToCommitTransaction(commitTimestamp uint64, transaction *ReadWriteTransaction) {
	oracle.committedTransactions = append(oracle.committedTransactions, CommittedTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
