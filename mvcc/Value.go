package mvcc

type Value struct {
	value   []byte
	deleted bool
}

func newValue(value []byte) Value {
	return Value{
		value:   value,
		deleted: false,
	}
}

func emptyValue() Value {
	return Value{}
}

func (value Value) slice() []byte {
	return value.value
}
