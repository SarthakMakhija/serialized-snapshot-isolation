package txn

import (
	"snapshot-isolation/mvcc"
	"snapshot-isolation/txn/errors"
)

type ReadonlyTransaction struct {
	beginTimestamp uint64
	memtable       *mvcc.MemTable
	oracle         *Oracle
}

type ReadWriteTransaction struct {
	beginTimestamp      uint64
	batch               *Batch
	reads               [][]byte
	transactionExecutor *TransactionExecutor
	memtable            *mvcc.MemTable
	oracle              *Oracle
}

func NewReadonlyTransaction(memtable *mvcc.MemTable, oracle *Oracle) *ReadonlyTransaction {
	return &ReadonlyTransaction{
		beginTimestamp: oracle.beginTimestamp(),
		oracle:         oracle,
		memtable:       memtable,
	}
}

func NewReadWriteTransaction(memtable *mvcc.MemTable, oracle *Oracle) *ReadWriteTransaction {
	return &ReadWriteTransaction{
		beginTimestamp:      oracle.beginTimestamp(),
		batch:               NewBatch(),
		transactionExecutor: NewTransactionExecutor(memtable),
		oracle:              oracle,
		memtable:            memtable,
	}
}

func (transaction *ReadonlyTransaction) Get(key []byte) (mvcc.Value, bool) {
	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memtable.Get(versionedKey)
}

func (transaction *ReadWriteTransaction) Get(key []byte) (mvcc.Value, bool) {
	if value, ok := transaction.batch.Get(key); ok {
		return mvcc.NewValue(value), true
	}
	transaction.reads = append(transaction.reads, key)

	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memtable.Get(versionedKey)
}

func (transaction *ReadWriteTransaction) PutOrUpdate(key []byte, value []byte) {
	transaction.batch.Add(key, value)
}

func (transaction *ReadWriteTransaction) Commit() (<-chan struct{}, error) {
	if transaction.batch.IsEmpty() {
		return nil, errors.EmptyTransactionErr
	}

	//Send the transaction to the executor in the increasing order of commit timestamp
	transaction.oracle.executorLock.Lock()
	defer transaction.oracle.executorLock.Unlock()

	commitTimestamp, err := transaction.oracle.mayBeCommitTimestampFor(transaction)
	if err != nil {
		return nil, err
	}
	return transaction.transactionExecutor.Submit(transaction.batch.ToTimestampedBatch(commitTimestamp)), nil
}
