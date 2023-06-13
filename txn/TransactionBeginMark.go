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

type Mark struct {
	timestamp       uint64
	done            bool
	outNotification chan struct{}
}

type TransactionTimestampMark struct {
	doneTill    atomic.Uint64
	markChannel chan Mark
	stopChannel chan struct{}
}

func NewTransactionTimestampMark() *TransactionTimestampMark {
	transactionMark := &TransactionTimestampMark{
		markChannel: make(chan Mark),
		stopChannel: make(chan struct{}),
	}
	go transactionMark.spin()
	return transactionMark
}

func (transactionTimestampMark *TransactionTimestampMark) Begin(timestamp uint64) {
	transactionTimestampMark.markChannel <- Mark{timestamp: timestamp, done: false}
}

func (transactionTimestampMark *TransactionTimestampMark) Finish(timestamp uint64) {
	transactionTimestampMark.markChannel <- Mark{timestamp: timestamp, done: true}
}

func (transactionTimestampMark *TransactionTimestampMark) Stop() {
	transactionTimestampMark.stopChannel <- struct{}{}
}

func (transactionTimestampMark *TransactionTimestampMark) DoneTill() uint64 {
	return transactionTimestampMark.doneTill.Load()
}

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

func closeAll(notificationChannelsByTimestamp map[uint64][]chan struct{}) {
	for timestamp, notificationChannels := range notificationChannelsByTimestamp {
		for _, channel := range notificationChannels {
			close(channel)
		}
		delete(notificationChannelsByTimestamp, timestamp)
	}
}
