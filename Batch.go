package snapshot_isolation

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

type Batch struct {
	pairs []KeyValuePair
}

type TimestampedBatch struct {
	batch       *Batch
	timestamp   uint64
	doneChannel chan struct{}
}

func NewBatch() *Batch {
	return &Batch{}
}

func (batch *Batch) Add(key, value []byte) *Batch {
	batch.pairs = append(batch.pairs, newKeyValuePair(key, value))
	return batch
}

func (batch *Batch) ToTimestampedBatch(commitTimestamp uint64) TimestampedBatch {
	return TimestampedBatch{
		batch:       batch,
		timestamp:   commitTimestamp,
		doneChannel: make(chan struct{}),
	}
}

func (timestampedBatch TimestampedBatch) AllPairs() []KeyValuePair {
	return timestampedBatch.batch.pairs
}