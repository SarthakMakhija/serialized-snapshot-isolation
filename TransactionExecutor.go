package snapshot_isolation

import "snapshot-isolation/mvcc"

type TimestampedBatch struct {
	batch       *Batch
	timestamp   uint64
	doneChannel chan struct{}
}

type TransactionExecutor struct {
	batchChannel chan TimestampedBatch
	stopChannel  chan struct{}
	memtable     *mvcc.MemTable
}

func NewTransactionExecutor(memtable *mvcc.MemTable) TransactionExecutor {
	transactionExecutor := TransactionExecutor{
		batchChannel: make(chan TimestampedBatch),
		stopChannel:  make(chan struct{}),
		memtable:     memtable,
	}
	go transactionExecutor.spin()
	return transactionExecutor
}

func (executor TransactionExecutor) Submit(batch *Batch, commitTimestamp uint64) <-chan struct{} {
	timestampedBatch := TimestampedBatch{batch: batch, timestamp: commitTimestamp, doneChannel: make(chan struct{})}
	executor.batchChannel <- timestampedBatch
	return timestampedBatch.doneChannel
}

func (executor TransactionExecutor) Stop() {
	executor.stopChannel <- struct{}{}
}

func (executor TransactionExecutor) spin() {
	for {
		select {
		case timestampedBatch := <-executor.batchChannel:
			executor.apply(timestampedBatch)
			executor.markApplied(timestampedBatch)
		case <-executor.stopChannel:
			return
		}
	}
}

func (executor TransactionExecutor) apply(timestampedBatch TimestampedBatch) {
	for _, keyValuePair := range timestampedBatch.batch.AllPairs() {
		executor.memtable.Put(
			mvcc.NewVersionedKey(keyValuePair.getKey(), timestampedBatch.timestamp),
			mvcc.NewValue(keyValuePair.getValue()),
		)
	}
}

func (executor TransactionExecutor) markApplied(batch TimestampedBatch) {
	batch.doneChannel <- struct{}{}
}
