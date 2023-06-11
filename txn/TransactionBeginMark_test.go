package txn

import (
	"github.com/stretchr/testify/assert"
	"sync"
	"testing"
	"time"
)

func TestTransactionBeginMarkWithASingleTransaction(t *testing.T) {
	transactionBeginMark := NewTransactionBeginMark()
	transactionBeginMark.Begin(1)
	transactionBeginMark.Finish(1)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, uint64(1), transactionBeginMark.DoneTill())
}

func TestTransactionBeginMarkWithTwoTransactions(t *testing.T) {
	transactionBeginMark := NewTransactionBeginMark()
	transactionBeginMark.Begin(1)
	transactionBeginMark.Begin(2)

	transactionBeginMark.Finish(2)
	transactionBeginMark.Finish(1)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, uint64(2), transactionBeginMark.DoneTill())
}

func TestTransactionBeginMarkWithAFewTransactions(t *testing.T) {
	transactionBeginMark := NewTransactionBeginMark()
	transactionBeginMark.Begin(1)
	transactionBeginMark.Begin(1)
	transactionBeginMark.Begin(1)
	transactionBeginMark.Begin(2)

	transactionBeginMark.Finish(2)
	transactionBeginMark.Finish(1)
	transactionBeginMark.Finish(1)
	transactionBeginMark.Finish(1)

	time.Sleep(10 * time.Millisecond)

	assert.Equal(t, uint64(2), transactionBeginMark.DoneTill())
}

func TestTransactionBeginMarkWithTwoConcurrentTransactions(t *testing.T) {
	transactionBeginMark := NewTransactionBeginMark()

	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		transactionBeginMark.Begin(1)
		transactionBeginMark.Finish(1)
	}()
	time.Sleep(5 * time.Millisecond)
	go func() {
		defer wg.Done()
		transactionBeginMark.Begin(2)
		transactionBeginMark.Finish(2)
	}()

	wg.Wait()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, uint64(2), transactionBeginMark.DoneTill())
}

func TestTransactionBeginMarkWithConcurrentTransactions(t *testing.T) {
	transactionBeginMark := NewTransactionBeginMark()

	var wg sync.WaitGroup
	wg.Add(100)

	for count := 1; count <= 100; count++ {
		go func(index uint64) {
			defer wg.Done()
			transactionBeginMark.Begin(index)
			transactionBeginMark.Finish(index)
		}(uint64(count))
		time.Sleep(5 * time.Millisecond)
	}

	wg.Wait()
	time.Sleep(20 * time.Millisecond)

	assert.Equal(t, uint64(100), transactionBeginMark.DoneTill())
}
