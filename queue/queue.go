/*
Package queue implements the heap interface in order to implement a request queue of distributed lock requests.
The heap request queue is prioritized by timestamp.
*/
package queue

import "time"

// Mssg represents the type being stored in the request queue heap.
type Mssg struct {
	Timestamp time.Time
	Node      string
	Replied   bool //to track whether the peer has replied to the request yet
}

// ReqHeap is a (simple) type implementing the request queue heap.
// Len, Swap, and Less functions are used to implement the heap.
type ReqHeap []*Mssg

// Len returns the length of the heap.
func (h ReqHeap) Len() int { return len(h) }

// Less is used for comparing nodes when building and maintaining the heap.
func (h ReqHeap) Less(i, j int) bool { return h[i].Timestamp.Before(h[j].Timestamp) }

// Swap is used for building and maintaining the heap.
func (h ReqHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

// Push adds a new Mssg into the heap.
func (h *ReqHeap) Push(x interface{}) {
	*h = append(*h, x.(*Mssg))
}

// Pop removes the oldest Mssg from the heap.
func (h *ReqHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
