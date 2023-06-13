package txn

import "serialized-snapshot-isolation/mvcc"

// TransactionExecutor represents an implementation of [Singular Update Queue](https://martinfowler.com/articles/patterns-of-distributed-systems/singular-update-queue.html).
// TransactionExecutor applies all the commits sequentially.
//
// It is a single goroutine that reads TimestampedBatch from the `batchChannel`.
// Anytime a ReadWriteTransaction is ready to commit, its TimestampedBatch is sent to the TransactionExecutor via Submit() method.
// TransactionExecutor converts all the Keys present in the TimestampedBatch to mvcc.VersionedKey and Value to mvcc.Value and
// applies all these mvcc.VersionedKey/mvcc.Value pairs to the mvcc.MemTable.
type TransactionExecutor struct {
	batchChannel chan TimestampedBatch
	stopChannel  chan struct{}
	memtable     *mvcc.MemTable
}

// NewTransactionExecutor creates a new instance of TransactionExecutor. It is called once in the entire application.
func NewTransactionExecutor(memtable *mvcc.MemTable) *TransactionExecutor {
	transactionExecutor := &TransactionExecutor{
		batchChannel: make(chan TimestampedBatch),
		stopChannel:  make(chan struct{}),
		memtable:     memtable,
	}
	go transactionExecutor.spin()
	return transactionExecutor
}

// Submit submits the TimestampedBatch to TransactionExecutor.
// Anytime a ReadWriteTransaction is ready to commit, its TimestampedBatch is sent to the TransactionExecutor via Submit() method.
// It also returns a doneChannel that the clients of the Commit() method of the ReadWriteTransaction can wait on to
// get notified when the transaction is applied.
func (executor *TransactionExecutor) Submit(batch TimestampedBatch) <-chan struct{} {
	executor.batchChannel <- batch
	return batch.doneChannel
}

// Stop stops the TransactionExecutor.
func (executor *TransactionExecutor) Stop() {
	executor.stopChannel <- struct{}{}
}

// spin is invoked as a single goroutine [`go spin()`] and it reads either an event from `stopChannel` or a TimestampedBatch from the `batchChannel`.
// On receiving a TimestampedBatch, it converts all the Keys present in the TimestampedBatch to mvcc.VersionedKey and Value to mvcc.Value and
// applies all these mvcc.VersionedKey/mvcc.Value pairs to the mvcc.MemTable.
func (executor *TransactionExecutor) spin() {
	for {
		select {
		case timestampedBatch := <-executor.batchChannel:
			executor.apply(timestampedBatch)
			executor.markApplied(timestampedBatch)
		case <-executor.stopChannel:
			close(executor.batchChannel)
			return
		}
	}
}

// apply converts all the Keys present in the TimestampedBatch to mvcc.VersionedKey and Value to mvcc.Value and
// applies all these mvcc.VersionedKey/mvcc.Value pairs to the mvcc.MemTable.
func (executor *TransactionExecutor) apply(timestampedBatch TimestampedBatch) {
	for _, keyValuePair := range timestampedBatch.AllPairs() {
		executor.memtable.PutOrUpdate(
			mvcc.NewVersionedKey(keyValuePair.getKey(), timestampedBatch.timestamp),
			mvcc.NewValue(keyValuePair.getValue()),
		)
	}
}

// markApplied sends a notification to the doneChannel and closes the channel to indicate that the transaction is applied.
func (executor *TransactionExecutor) markApplied(batch TimestampedBatch) {
	batch.doneChannel <- struct{}{}
	close(batch.doneChannel)
}
