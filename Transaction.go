package snapshot_isolation

import "snapshot-isolation/mvcc"

type Transaction struct {
	isReadonly    bool
	readTimestamp uint64

	//TODO: will change later
	memtable *mvcc.MemTable
}

func NewReadonlyTransaction(readTimestamp uint64, memtable *mvcc.MemTable) *Transaction {
	return &Transaction{
		isReadonly:    true,
		readTimestamp: readTimestamp,
		memtable:      memtable,
	}
}

func (transaction *Transaction) Get(key []byte) (mvcc.Value, bool) {
	//TODO: If the transaction is not readonly, we need to capture the keys in the readSet
	versionedKey := mvcc.NewVersionedKey(key, transaction.readTimestamp)
	return transaction.memtable.Get(versionedKey)
}
