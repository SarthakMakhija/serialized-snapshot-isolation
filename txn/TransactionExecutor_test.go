package txn

import (
	"github.com/stretchr/testify/assert"
	"serialized-snapshot-isolation/mvcc"
	"testing"
)

func TestExecutesABatch(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	executor := NewTransactionExecutor(memTable)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	doneChannel := executor.Submit(batch.ToTimestampedBatch(1))
	<-doneChannel

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), value.Slice())
}

func TestExecutes2Batches(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	executor := NewTransactionExecutor(memTable)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	doneChannel := executor.Submit(batch.ToTimestampedBatch(1))
	<-doneChannel

	anotherBatch := NewBatch()
	_ = anotherBatch.Add([]byte("HDD"), []byte("Hard disk drive"))
	_ = anotherBatch.Add([]byte("isolation"), []byte("Serialized Snapshot"))

	doneChannel = executor.Submit(anotherBatch.ToTimestampedBatch(2))
	<-doneChannel

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), value.Slice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk drive"), value.Slice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 3))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Serialized Snapshot"), value.Slice())
}

func TestExecutesABatchAndStops(t *testing.T) {
	memTable := mvcc.NewMemTable(10)
	executor := NewTransactionExecutor(memTable)

	batch := NewBatch()
	_ = batch.Add([]byte("HDD"), []byte("Hard disk"))
	_ = batch.Add([]byte("isolation"), []byte("Snapshot"))

	doneChannel := executor.Submit(batch.ToTimestampedBatch(1))
	<-doneChannel

	executor.Stop()

	value, ok := memTable.Get(mvcc.NewVersionedKey([]byte("HDD"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Hard disk"), value.Slice())

	value, ok = memTable.Get(mvcc.NewVersionedKey([]byte("isolation"), 2))
	assert.Equal(t, true, ok)
	assert.Equal(t, []byte("Snapshot"), value.Slice())
}
