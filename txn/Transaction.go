package txn

import (
	"serialized-snapshot-isolation/mvcc"
	"serialized-snapshot-isolation/txn/errors"
)

// ReadonlyTransaction represents a read-only transaction.
// A ReadonlyTransaction is assigned a beginTimestamp everytime it starts and can only perform a `get` operation.
type ReadonlyTransaction struct {
	beginTimestamp uint64
	memtable       *mvcc.MemTable
	oracle         *Oracle
}

// ReadWriteTransaction represents a read-write transaction.
// A ReadWriteTransaction is assigned a beginTimestamp everytime it starts, and a commitTimestamp every time
// it is ready to commit and there are not RW conflicts. (More on this in Oracle).
// A ReadWriteTransaction also tracks the keys that are read in `reads: [][]byte`.
// This tracking is essential to determine RW conflict.
type ReadWriteTransaction struct {
	beginTimestamp uint64
	batch          *Batch
	reads          [][]byte
	memtable       *mvcc.MemTable
	oracle         *Oracle
}

// NewReadonlyTransaction creates a new instance of ReadonlyTransaction.
func NewReadonlyTransaction(oracle *Oracle) *ReadonlyTransaction {
	return &ReadonlyTransaction{
		beginTimestamp: oracle.beginTimestamp(),
		oracle:         oracle,
		memtable:       oracle.transactionExecutor.memtable,
	}
}

// NewReadWriteTransaction creates a new instance of ReadWriteTransaction.
func NewReadWriteTransaction(oracle *Oracle) *ReadWriteTransaction {
	return &ReadWriteTransaction{
		beginTimestamp: oracle.beginTimestamp(),
		batch:          NewBatch(),
		oracle:         oracle,
		memtable:       oracle.transactionExecutor.memtable,
	}
}

// Get performs a get operation from the mvcc.MemTable.
// It returns a pair  of (mvcc.Value and true) if the value exists for the key, (nil, false) otherwise.
func (transaction *ReadonlyTransaction) Get(key []byte) (mvcc.Value, bool) {
	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memtable.Get(versionedKey)
}

// Finish indicates the end of ReadonlyTransaction.
// It is used to indicate the TransactionTimestampMark inside Oracle that all the transactions upto a given `beginTimestamp`
// are done. (More on this in Oracle).
func (transaction *ReadonlyTransaction) Finish() {
	transaction.oracle.finishBeginTimestampForReadonlyTransaction(transaction)
}

// Get performs a get operation from the mvcc.MemTable.
// It returns a pair  of (mvcc.Value and true) if the value exists for the key, (nil, false) otherwise.
// Unlike the Get of ReadonlyTransaction, reads are tracked inside the Get of ReadWriteTransaction.
func (transaction *ReadWriteTransaction) Get(key []byte) (mvcc.Value, bool) {
	if value, ok := transaction.batch.Get(key); ok {
		return mvcc.NewValue(value), true
	}
	transaction.reads = append(transaction.reads, key)

	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memtable.Get(versionedKey)
}

// PutOrUpdate adds the key/value pair to the Batch inside ReadWriteTransaction.
// It returns an error if an attempt is made to add the duplicate key to the ReadWriteTransaction.
func (transaction *ReadWriteTransaction) PutOrUpdate(key []byte, value []byte) error {
	err := transaction.batch.Add(key, value)
	if err != nil {
		return err
	}
	return nil
}

// Commit commits the ReadWriteTransaction.
// Commit involves the following:
// 1. Acquiring an executorLock to ensure that the transaction are sent to the TransactionExecutor in the order of their commitTimestamp.
// 2. Getting the commit timestamp for the transaction. Commit timestamp is only provided if the transaction does not have any RW conflict.
// 3. Submitting the TimestampedBatch to the TransactionExecutor
// More details on commitTimestamp are available in Oracle. Commits are executed serially and the details are available in TransactionExecutor.
func (transaction *ReadWriteTransaction) Commit() (<-chan struct{}, error) {
	if transaction.batch.IsEmpty() {
		return nil, errors.EmptyTransactionErr
	}

	// Send the transaction to the executor in the increasing order of the commitTimestamp.
	// If a commit with the commitTimestamp 102 is applied, it is assumed that the commit with commitTimestamp 101 is already available.
	transaction.oracle.executorLock.Lock()
	defer transaction.oracle.executorLock.Unlock()

	commitTimestamp, err := transaction.oracle.mayBeCommitTimestampFor(transaction)
	if err != nil {
		return nil, err
	}
	noCallback := func() {}
	return transaction.oracle.transactionExecutor.Submit(transaction.batch.ToTimestampedBatch(commitTimestamp, noCallback)), nil
}

// Finish indicates the end of ReadWriteTransaction.
// It is used to indicate the TransactionTimestampMark inside Oracle that all the transactions upto a given `beginTimestamp`
// are done. (More on this in Oracle).
func (transaction *ReadWriteTransaction) Finish() {
	transaction.oracle.finishBeginTimestampForReadWriteTransaction(transaction)
}
