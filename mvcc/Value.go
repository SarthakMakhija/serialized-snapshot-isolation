package mvcc

type Value struct {
	value []byte
}

func NewValue(value []byte) Value {
	return Value{
		value: value,
	}
}

func emptyValue() Value {
	return Value{}
}

func (value Value) Slice() []byte {
	return value.value
}
