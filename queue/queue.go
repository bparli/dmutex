package queue

import "time"

type ReqHeap []*Mssg

type Mssg struct {
	Timestamp time.Time
	Node      string
	Replied   bool //to track whether the peer has replied to the request yet
}

func (h ReqHeap) Len() int           { return len(h) }
func (h ReqHeap) Less(i, j int) bool { return h[i].Timestamp.Before(h[j].Timestamp) }
func (h ReqHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *ReqHeap) Push(x interface{}) {
	*h = append(*h, x.(*Mssg))
}

func (h *ReqHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
