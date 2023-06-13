package txn

import (
	"container/heap"
	"context"
	"sync/atomic"
)

// TransactionTimestampHeap
// https://pkg.go.dev/container/heap
type TransactionTimestampHeap []uint64

func (h TransactionTimestampHeap) Len() int           { return len(h) }
func (h TransactionTimestampHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h TransactionTimestampHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *TransactionTimestampHeap) Push(x any)        { *h = append(*h, x.(uint64)) }
func (h *TransactionTimestampHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// Mark represents
type Mark struct {
	timestamp       uint64
	done            bool
	outNotification chan struct{}
}

// TransactionTimestampMark keeps track of the timestamps that are processed.
// It could be beginTimestamp or the commitTimestamp.
// Let's say a txn.ReadWriteTransaction begins with a timestamp = 2.
// It will invoke Begin method to indicate that a transaction with timestamp = 2 has started.
// At some later point in time, the same transaction will commit, assuming it does not have any RW conflict.
// Let's consider that is commits with the timestamp = 5. It will invoke Finish method passing 5 as the argument.
// This will indicate to the TransactionTimestampMark that transactions up till timestamp = 5 are done.
// This information can be used for blocking new transactions until transactions upto a given timestamp are done.
// The idea is from [Badger](https://github.com/dgraph-io/badger).
type TransactionTimestampMark struct {
	doneTill    atomic.Uint64
	markChannel chan Mark
	stopChannel chan struct{}
}

// NewTransactionTimestampMark creates a new instance of TransactionTimestampMark
func NewTransactionTimestampMark() *TransactionTimestampMark {
	transactionMark := &TransactionTimestampMark{
		markChannel: make(chan Mark),
		stopChannel: make(chan struct{}),
	}
	go transactionMark.spin()
	return transactionMark
}

// Begin sends a mark to the markChannel indicating that a transaction with the given timestamp has started.
func (transactionTimestampMark *TransactionTimestampMark) Begin(timestamp uint64) {
	transactionTimestampMark.markChannel <- Mark{timestamp: timestamp, done: false}
}

// Finish sends a mark to the markChannel indicating that a transaction with the given timestamp is done.
func (transactionTimestampMark *TransactionTimestampMark) Finish(timestamp uint64) {
	transactionTimestampMark.markChannel <- Mark{timestamp: timestamp, done: true}
}

// Stop stops the TransactionTimestampMark.
func (transactionTimestampMark *TransactionTimestampMark) Stop() {
	transactionTimestampMark.stopChannel <- struct{}{}
}

// DoneTill returns the timestamp till which the processing is done.
func (transactionTimestampMark *TransactionTimestampMark) DoneTill() uint64 {
	return transactionTimestampMark.doneTill.Load()
}

// WaitForMark is used to wait till the transaction timestamp >= timestamp is processed.
// It does this by sending a mark to the `markChannel` and waiting for a response on the `waitChannel`.
func (transactionTimestampMark *TransactionTimestampMark) WaitForMark(ctx context.Context, timestamp uint64) error {
	if transactionTimestampMark.DoneTill() >= timestamp {
		return nil
	}
	waitChannel := make(chan struct{})
	transactionTimestampMark.markChannel <- Mark{timestamp: timestamp, outNotification: waitChannel}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-waitChannel:
		return nil
	}
}

// spin is invoked as a single goroutine [`go spin()`].
// It processes all the marks that are received on the `markChannel`.
// Any time it receives a mark, it invokes the process function, which determines if the timestamp in the mark is done or not.
// Let's consider the following case:
// Two transactions with commitTimestamps 4, 6 are running.
// The transaction with the commitTimestamp 6 invokes Finish(), followed by the transaction with the commitTimestamp 4.
// TransactionTimestampMark can not consider the transaction with commitTimestamp = 6 as done because a transaction with the
// commitTimestamp of 4 is not done yet.
// It maintains a binary heap of transaction timestamps and anytime it identifies that a transaction is done, the transaction timestamp
// is popped off the heap and the doneTill field of TransactionTimestampMark is updated.
// This ensures that doneTill mark is updated in the following order: 4 followed by 6.
func (transactionTimestampMark *TransactionTimestampMark) spin() {
	var orderedTransactionTimestamps TransactionTimestampHeap
	pendingTransactionRequestsByTimestamp := make(map[uint64]int)
	notificationChannelsByTimestamp := make(map[uint64][]chan struct{})

	heap.Init(&orderedTransactionTimestamps)
	process := func(mark Mark) {
		previous, ok := pendingTransactionRequestsByTimestamp[mark.timestamp]
		if !ok {
			heap.Push(&orderedTransactionTimestamps, mark.timestamp)
		}

		pendingTransactionCount := 1
		if mark.done {
			pendingTransactionCount = -1
		}
		pendingTransactionRequestsByTimestamp[mark.timestamp] = previous + pendingTransactionCount

		doneTill := transactionTimestampMark.DoneTill()
		localDoneTillTimestamp := doneTill
		for len(orderedTransactionTimestamps) > 0 {
			minimumTimestamp := orderedTransactionTimestamps[0]
			if done := pendingTransactionRequestsByTimestamp[minimumTimestamp]; done > 0 {
				break
			}
			heap.Pop(&orderedTransactionTimestamps)
			delete(pendingTransactionRequestsByTimestamp, minimumTimestamp)

			localDoneTillTimestamp = minimumTimestamp
		}

		if localDoneTillTimestamp != doneTill {
			transactionTimestampMark.doneTill.CompareAndSwap(doneTill, localDoneTillTimestamp)
		}
		for timestamp, notificationChannels := range notificationChannelsByTimestamp {
			if timestamp <= localDoneTillTimestamp {
				for _, channel := range notificationChannels {
					close(channel)
				}
				delete(notificationChannelsByTimestamp, timestamp)
			}
		}
	}
	for {
		select {
		case mark := <-transactionTimestampMark.markChannel:
			if mark.outNotification != nil {
				doneTill := transactionTimestampMark.doneTill.Load()
				if doneTill >= mark.timestamp {
					close(mark.outNotification)
				} else {
					channels, ok := notificationChannelsByTimestamp[mark.timestamp]
					if !ok {
						notificationChannelsByTimestamp[mark.timestamp] = []chan struct{}{mark.outNotification}
					} else {
						notificationChannelsByTimestamp[mark.timestamp] = append(channels, mark.outNotification)
					}
				}
			} else {
				process(mark)
			}
		case <-transactionTimestampMark.stopChannel:
			close(transactionTimestampMark.markChannel)
			close(transactionTimestampMark.stopChannel)
			closeAll(notificationChannelsByTimestamp)
			return
		}
	}
}

// closeAll closes all the channels that are waiting on various timestamps.
func closeAll(notificationChannelsByTimestamp map[uint64][]chan struct{}) {
	for timestamp, notificationChannels := range notificationChannelsByTimestamp {
		for _, channel := range notificationChannels {
			close(channel)
		}
		delete(notificationChannelsByTimestamp, timestamp)
	}
}
