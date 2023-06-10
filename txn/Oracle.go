package txn

import (
	"errors"
	"sync"
)

var ConflictErr = errors.New("transaction conflicts with other")

type CommittedTransaction struct {
	commitTimestamp uint64
	transaction     *ReadWriteTransaction
}

type Oracle struct {
	lock                  sync.Mutex
	nextTimestamp         uint64
	committedTransactions []CommittedTransaction
}

// NewOracle
// TODO: nextTimestamp is initialized to 1, will change later
func NewOracle() *Oracle {
	return &Oracle{
		nextTimestamp: 1,
	}
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

func (oracle *Oracle) trackReadyToCommitTransaction(
	commitTimestamp uint64,
	transaction *ReadWriteTransaction) {

	oracle.committedTransactions = append(oracle.committedTransactions, CommittedTransaction{
		commitTimestamp: commitTimestamp,
		transaction:     transaction,
	})
}
