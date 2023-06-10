package snapshot_isolation

import "snapshot-isolation/mvcc"

type ReadonlyTransaction struct {
	readTimestamp uint64
	//TODO: will change later
	memtable *mvcc.MemTable
}

func NewReadonlyTransaction(readTimestamp uint64, memtable *mvcc.MemTable) *ReadonlyTransaction {
	return &ReadonlyTransaction{
		readTimestamp: readTimestamp,
		memtable:      memtable,
	}
}

func (transaction *ReadonlyTransaction) Get(key []byte) (mvcc.Value, bool) {
	//TODO: If the transaction is not readonly, we need to capture the keys in the readSet
	versionedKey := mvcc.NewVersionedKey(key, transaction.readTimestamp)
	return transaction.memtable.Get(versionedKey)
}
