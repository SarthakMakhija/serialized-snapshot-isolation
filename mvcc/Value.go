package mvcc

// Value wraps a []byte which acts as a value in the MemTable.
type Value struct {
	value []byte
}

// NewValue creates a new instance of the Value.
func NewValue(value []byte) Value {
	return Value{
		value: value,
	}
}

// emptyValue returns an empty Value. Is used when the value for a key is not found.
func emptyValue() Value {
	return Value{}
}

// Slice returns the byte slice present in the Value.
func (value Value) Slice() []byte {
	return value.value
}
