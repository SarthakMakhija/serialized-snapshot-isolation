package txn

import "snapshot-isolation/mvcc"

type ReadonlyTransaction struct {
	beginTimestamp uint64
	//TODO: will change later
	memtable *mvcc.MemTable
}

type ReadWriteTransaction struct {
	beginTimestamp      uint64
	batch               *Batch
	reads               [][]byte
	transactionExecutor *TransactionExecutor
	//TODO: will change later
	memtable *mvcc.MemTable
}

// NewReadonlyTransaction
// TODO: Decide if the signature needs a beginTimestamp or should the NewReadonlyTransaction method get the beginTimestamp from Oracle
func NewReadonlyTransaction(beginTimestamp uint64, memtable *mvcc.MemTable) *ReadonlyTransaction {
	return &ReadonlyTransaction{
		beginTimestamp: beginTimestamp,
		memtable:       memtable,
	}
}

func NewReadWriteTransaction(beginTimestamp uint64, memtable *mvcc.MemTable) *ReadWriteTransaction {
	return &ReadWriteTransaction{
		beginTimestamp:      beginTimestamp,
		batch:               NewBatch(),
		transactionExecutor: NewTransactionExecutor(memtable),
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

// Commit
// TODO: Decide if the signature needs a commitTimestamp or should the Commit method get the commitTimestamp from Oracle
// TODO: Get the commit timestamp from Oracle and ensure that the transaction go to the executor in the increasing order of commit timestamp
func (transaction *ReadWriteTransaction) Commit(commitTimestamp uint64) <-chan struct{} {
	//TODO: Identify conflicts
	if transaction.batch.IsEmpty() {
		return nil
	}
	return transaction.transactionExecutor.Submit(transaction.batch.ToTimestampedBatch(commitTimestamp))
}
