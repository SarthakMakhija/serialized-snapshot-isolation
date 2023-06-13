package serialized_snapshot_isolation

import (
	"serialized-snapshot-isolation/mvcc"
	"serialized-snapshot-isolation/txn"
)

// KeyValueDb represents an in-memory store backed by multi-versioned SkipList.
// It provides two behaviors: Get and PutOrUpdate which run in a transaction.
// KeyValueDb provides supports for txn.ReadonlyTransaction and txn.ReadWriteTransaction.
// All the transactions run with serialized snapshot isolation.

// KeyValueDb
// Serialized snapshot isolation:
// To provide serialized snapshot isolation, databases need to maintain multiple versions of the data.
// That means a key/value store would maintain multiple versions of each key. This implementation maintains
// multiple versions of each key in a SkipList. (Please refer to mvcc/ package).
// Each transaction gets a beginTimestamp when it starts and a commitTimestamp when it is ready to commit.
// These timestamps are provided by a central authority Oracle. Please refer to txn.Oracle.
// This implementation uses monotonically increasing numbers as timestamps.
// Serialized snapshot isolation prevents RW conflicts which means a transaction Txn will commit successfully, if its read keys
// are not written by another concurrent transaction with a commitTimestamp higher than the beginTimestamp of Txn.
// If a transaction does not have any RW conflict, it gets a commitTimestamp which is used as a version in the keys that
// get written to the SkipList.
type KeyValueDb struct {
	oracle *txn.Oracle
}

// NewKeyValueDb creates a new instance of KeyValueDb.
func NewKeyValueDb(skiplistMaxLevel uint8) *KeyValueDb {
	return &KeyValueDb{
		oracle: txn.NewOracle(
			txn.NewTransactionExecutor(mvcc.NewMemTable(skiplistMaxLevel)),
		),
	}
}

// Get takes a callback which receives a pointer to a txn.ReadonlyTransaction.
// txn.ReadonlyTransaction provides Get method to look up the value for the key.
func (db *KeyValueDb) Get(callback func(transaction *txn.ReadonlyTransaction)) {
	transaction := txn.NewReadonlyTransaction(db.oracle)
	defer transaction.FinishBeginTimestampForReadonlyTransaction()

	callback(transaction)
}

// PutOrUpdate takes a callback which receives a pointer to a txn.ReadWriteTransaction.
// ReadWriteTransaction provides Get and PutOrUpdate to perform the required operations.
// This method performs a commit as soon as the callback is done.
func (db *KeyValueDb) PutOrUpdate(callback func(transaction *txn.ReadWriteTransaction)) (<-chan struct{}, error) {
	transaction := txn.NewReadWriteTransaction(db.oracle)
	defer transaction.FinishBeginTimestampForReadWriteTransaction()

	callback(transaction)
	return transaction.Commit()
}
