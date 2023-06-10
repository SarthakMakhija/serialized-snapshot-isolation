package snapshot_isolation

import "snapshot-isolation/mvcc"

type ReadonlyTransaction struct {
	beginTimestamp uint64
	//TODO: will change later
	memtable *mvcc.MemTable
}

type ReadWriteTransaction struct {
	beginTimestamp      uint64
	batch               *Batch
	transactionExecutor *TransactionExecutor
	//TODO: will change later
	memtable *mvcc.MemTable
}

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
	//TODO: track the key in the readSet
	versionedKey := mvcc.NewVersionedKey(key, transaction.beginTimestamp)
	return transaction.memtable.Get(versionedKey)
}

func (transaction *ReadWriteTransaction) PutOrUpdate(key []byte, value []byte) {
	transaction.batch.Add(key, value)
}

// Commit
// TODO: Decide if the signature needs a commitTimestamp or should the Commit method get the commitTimestamp
func (transaction *ReadWriteTransaction) Commit(commitTimestamp uint64) {
	//TODO: Identify conflicts
	if transaction.batch.IsEmpty() {
		return
	}
	transaction.transactionExecutor.Submit(transaction.batch.ToTimestampedBatch(commitTimestamp))
}
