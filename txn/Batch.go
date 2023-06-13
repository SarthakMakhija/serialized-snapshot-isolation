package txn

import (
	"bytes"
	"serialized-snapshot-isolation/txn/errors"
)

// KeyValuePair wraps a key and a value.
type KeyValuePair struct {
	key   []byte
	value []byte
}

func newKeyValuePair(key, value []byte) KeyValuePair {
	return KeyValuePair{
		key:   key,
		value: value,
	}
}

func (pair KeyValuePair) getKey() []byte {
	return pair.key
}

func (pair KeyValuePair) getValue() []byte {
	return pair.value
}

// Batch maintains all the key/value pairs that are a part of one RW-transaction.
// Every ReadWriteTransaction will batch the changes and when the changes are ready to be committed, the Commit() method will be invoked.
type Batch struct {
	pairs []KeyValuePair
}

// TimestampedBatch represents the Batch which is given the commit timestamp.
// When a ReadWriteTransaction is ready to commit, the batch that is a part of the transaction, is given the commit timestamp.
// The abstraction TimestampedBatch represents the Batch with the commit timestamp that is ready to commit.
type TimestampedBatch struct {
	batch       *Batch
	timestamp   uint64
	doneChannel chan struct{}
}

// NewBatch creates a new instance of Batch.
// In the current implementation a new instance of Batch is created for every ReadWriteTransaction.
// This is a good opportunity to use object-pool pattern.
func NewBatch() *Batch {
	return &Batch{}
}

// Add adds the key/value pair in the Batch. Throws an error if the key is already present in the Batch.
func (batch *Batch) Add(key, value []byte) error {
	if batch.Contains(key) {
		return errors.DuplicateKeyInBatchErr
	}
	batch.pairs = append(batch.pairs, newKeyValuePair(key, value))
	return nil
}

// Get returns the value for the key, is the value is present in the batch.
// Returns (Value, true) is the value is present in the Batch, else returns (nil, false).
func (batch *Batch) Get(key []byte) ([]byte, bool) {
	for _, pair := range batch.pairs {
		if bytes.Compare(pair.key, key) == 0 {
			return pair.value, true
		}
	}
	return nil, false
}

// Contains returns true is the key is present in the Batch, false otherwise.
func (batch *Batch) Contains(key []byte) bool {
	_, ok := batch.Get(key)
	return ok
}

// ToTimestampedBatch converts the batch to a TimestampedBatch.
func (batch *Batch) ToTimestampedBatch(commitTimestamp uint64) TimestampedBatch {
	return TimestampedBatch{
		batch:       batch,
		timestamp:   commitTimestamp,
		doneChannel: make(chan struct{}),
	}
}

// IsEmpty returns true is the Batch is empty, false otherwise
func (batch *Batch) IsEmpty() bool {
	return len(batch.pairs) == 0
}

// AllPairs returns all the Key/Value pairs that are a part of the Batch.
func (timestampedBatch TimestampedBatch) AllPairs() []KeyValuePair {
	return timestampedBatch.batch.pairs
}
