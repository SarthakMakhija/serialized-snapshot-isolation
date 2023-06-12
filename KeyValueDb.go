package snapshot_isolation

import (
	"snapshot-isolation/mvcc"
	"snapshot-isolation/txn"
)

type KeyValueDb struct {
	oracle *txn.Oracle
}

func NewKeyValueDb(skiplistMaxLevel int) *KeyValueDb {
	return &KeyValueDb{
		oracle: txn.NewOracle(
			txn.NewTransactionExecutor(mvcc.NewMemTable(skiplistMaxLevel)),
		),
	}
}

func (db *KeyValueDb) Get(callback func(transaction *txn.ReadonlyTransaction)) {
	transaction := txn.NewReadonlyTransaction(db.oracle)
	defer transaction.Finish()

	callback(transaction)
}

func (db *KeyValueDb) PutOrUpdate(callback func(transaction *txn.ReadWriteTransaction)) (<-chan struct{}, error) {
	transaction := txn.NewReadWriteTransaction(db.oracle)
	defer transaction.Finish()

	callback(transaction)
	return transaction.Commit()
}
