package queue

import (
	"container/heap"
	"testing"
	"time"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_Queue(t *testing.T) {
	Convey("Init and test the queue (heap) implementation", t, func() {

		reqQueue := &ReqHeap{}
		heap.Init(reqQueue)

		args := &Mssg{
			Timestamp: time.Now().AddDate(-1, 0, 0),
			Node:      "10.10.10.10",
		}

		heap.Push(reqQueue, args)

		args = &Mssg{
			Timestamp: time.Now(),
			Node:      "11.11.11.11",
			Replied:   false,
		}

		heap.Push(reqQueue, args)

		check := heap.Pop(reqQueue).(*Mssg)
		So(check.Node, ShouldEqual, "10.10.10.10")

		check = heap.Pop(reqQueue).(*Mssg)
		So(check.Node, ShouldEqual, "11.11.11.11")

	})
}
