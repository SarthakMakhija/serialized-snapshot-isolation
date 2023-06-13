package snapshot_isolation

import (
	"github.com/stretchr/testify/assert"
	"serialized-snapshot-isolation/txn"
	"serialized-snapshot-isolation/txn/errors"
	"strconv"
	"sync"
	"testing"
	"time"
)

func TestGetsTheValueOfANonExistingKey(t *testing.T) {
	db := NewKeyValueDb(10)
	db.Get(func(transaction *txn.ReadonlyTransaction) {
		_, exists := transaction.Get([]byte("non-existing"))
		assert.Equal(t, false, exists)
	})
}

func TestGetsTheValueOfAnExistingKey(t *testing.T) {
	db := NewKeyValueDb(10)
	waitChannel, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
	})
	assert.Nil(t, err)
	<-waitChannel

	db.Get(func(transaction *txn.ReadonlyTransaction) {
		value, exists := transaction.Get([]byte("HDD"))
		assert.Equal(t, true, exists)
		assert.Equal(t, []byte("Hard disk"), value.Slice())
	})
}

func TestPutsMultipleKeyValuesInATransaction(t *testing.T) {
	db := NewKeyValueDb(10)
	waitChannel, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
		for count := 1; count <= 100; count++ {
			transaction.PutOrUpdate([]byte("Key:"+strconv.Itoa(count)), []byte("Value:"+strconv.Itoa(count)))
		}
	})
	assert.Nil(t, err)
	<-waitChannel

	db.Get(func(transaction *txn.ReadonlyTransaction) {
		for count := 1; count <= 100; count++ {
			value, exists := transaction.Get([]byte("Key:" + strconv.Itoa(count)))
			assert.Equal(t, true, exists)
			assert.Equal(t, []byte("Value:"+strconv.Itoa(count)), value.Slice())
		}
	})
}

func TestInvolvesConflictingTransactions(t *testing.T) {
	db := NewKeyValueDb(10)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		_, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
			delayCommit := func() {
				time.Sleep(25 * time.Millisecond)
			}
			_, _ = transaction.Get([]byte("HDD"))
			transaction.PutOrUpdate([]byte("SSD"), []byte("Solid state drive"))
			delayCommit()
		})
		assert.Error(t, err)
		assert.Equal(t, errors.ConflictErr, err)
	}()

	go func() {
		defer wg.Done()
		waitChannelTwo, err := db.PutOrUpdate(func(transaction *txn.ReadWriteTransaction) {
			delayCommit := func() {
				time.Sleep(10 * time.Millisecond)
			}
			transaction.PutOrUpdate([]byte("HDD"), []byte("Hard disk"))
			delayCommit()
		})
		assert.Nil(t, err)
		<-waitChannelTwo
	}()
	wg.Wait()
}
