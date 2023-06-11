package txn

import (
	"container/heap"
	"sync/atomic"
)

// TransactionBeginTimestampHeap
// https://pkg.go.dev/container/heap
type TransactionBeginTimestampHeap []uint64

func (h TransactionBeginTimestampHeap) Len() int           { return len(h) }
func (h TransactionBeginTimestampHeap) Less(i, j int) bool { return h[i] < h[j] }
func (h TransactionBeginTimestampHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }
func (h *TransactionBeginTimestampHeap) Push(x any)        { *h = append(*h, x.(uint64)) }
func (h *TransactionBeginTimestampHeap) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

type Mark struct {
	timestamp uint64
	done      bool
}

type TransactionBeginMark struct {
	doneTill    atomic.Uint64
	markChannel chan Mark
	stopChannel chan struct{}
}

func NewTransactionBeginMark() *TransactionBeginMark {
	transactionMark := &TransactionBeginMark{
		markChannel: make(chan Mark),
		stopChannel: make(chan struct{}),
	}
	go transactionMark.spin()
	return transactionMark
}

func (beginMark *TransactionBeginMark) Begin(timestamp uint64) {
	beginMark.markChannel <- Mark{timestamp: timestamp, done: false}
}

func (beginMark *TransactionBeginMark) Finish(timestamp uint64) {
	beginMark.markChannel <- Mark{timestamp: timestamp, done: true}
}

func (beginMark *TransactionBeginMark) Stop() {
	beginMark.stopChannel <- struct{}{}
}

func (beginMark *TransactionBeginMark) DoneTill() uint64 {
	return beginMark.doneTill.Load()
}

func (beginMark *TransactionBeginMark) spin() {
	var orderedTransactionTimestamps TransactionBeginTimestampHeap
	pendingTransactionRequestsByTimestamp := make(map[uint64]int)

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
		doneTill := beginMark.DoneTill()

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
			beginMark.doneTill.CompareAndSwap(doneTill, localDoneTillTimestamp)
		}
	}
	for {
		select {
		case mark := <-beginMark.markChannel:
			process(mark)
		case <-beginMark.stopChannel:
			close(beginMark.markChannel)
			close(beginMark.stopChannel)
			return
		}
	}
}
