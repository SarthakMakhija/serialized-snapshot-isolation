package mvcc

type Value struct {
	value []byte
}

func newValue(value []byte) Value {
	return Value{
		value: value,
	}
}

func emptyValue() Value {
	return Value{}
}

func (value Value) slice() []byte {
	return value.value
}
