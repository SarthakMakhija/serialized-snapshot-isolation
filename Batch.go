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

func NewBatch() *Batch {
	return &Batch{}
}

func (batch *Batch) Add(key, value []byte) *Batch {
	batch.pairs = append(batch.pairs, newKeyValuePair(key, value))
	return batch
}

func (batch *Batch) AllPairs() []KeyValuePair {
	return batch.pairs
}
