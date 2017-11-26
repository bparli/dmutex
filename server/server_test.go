package server

import (
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/bparli/dmutex/queue"
	. "github.com/smartystreets/goconvey/convey"
)

// use same RPC server for all tests in dmutex package.
var (
	testServer *RPCServer
	started    bool
)

func setupTestRPC() {
	var err error
	if !started || testServer == nil {
		testServer, err = NewRPCServer("127.0.0.1", 10, 10*time.Second)
		if err != nil {
			return
		}
	}
	started = true
	testServer.SetReady(true)
}

func Test_BasicServer(t *testing.T) {
	Convey("Init server, Send request, and Gather Reply", t, func() {

		setupTestRPC()
		testServer.SetReady(true)

		t := time.Now()
		args := &queue.Mssg{
			Timestamp: t,
			Node:      "127.0.0.1",
			Replied:   false,
		}

		var reply int
		client, err := rpc.DialHTTP("tcp", "127.0.0.1:7070")
		defer client.Close()

		So(err, ShouldBeNil)

		err = client.Call("Dsync.Request", args, &reply)

		So(err, ShouldBeNil)
		So(reply, ShouldEqual, 0)

		peers := make(map[string]bool)
		peers["127.0.0.1"] = true
		errReply := testServer.GatherReplies(args, peers)
		err = client.Call("Dsync.Reply", args, &reply)
		So(err, ShouldBeNil)
		So(errReply, ShouldBeNil)

		peers["127.0.0.1"] = false
		errReply = testServer.GatherReplies(args, peers)
		err = client.Call("Dsync.Reply", args, &reply)
		So(err, ShouldBeNil)
		So(errReply, ShouldNotBeNil)

	})
}

func Test_Inquire(t *testing.T) {
	Convey("Test Inquire", t, func() {
		d := &Dsync{
			reqQueue:  nil,
			qMutex:    &sync.RWMutex{},
			progMutex: &sync.RWMutex{},
			localAddr: "127.0.0.2",
			reqsCh:    nil,
			repliesCh: nil,
		}

		testServer.SetProgress(ProgressNotAcquired)
		reply, err := d.sendInquire("127.0.0.1")
		So(err, ShouldBeNil)
		So(reply, ShouldEqual, YieldMssg)

		testServer.SetProgress(ProgressAcquired)
		go func() {
			time.Sleep(1 * time.Second)
			testServer.SetProgress(ProgressNotAcquired)
		}()
		reply, err = d.sendInquire("127.0.0.1")
		So(err, ShouldBeNil)
		So(reply, ShouldEqual, RelinqMssg)

	})
}

func Test_Relinquish(t *testing.T) {
	Convey("Test Relinquish", t, func() {
		args := &queue.Mssg{
			Timestamp: time.Now(),
			Node:      "127.0.0.1",
			Replied:   false,
		}
		testServer.dsync.push(args)

		var reply int

		client, err := rpc.DialHTTP("tcp", "127.0.0.1:7070")
		defer client.Close()

		So(err, ShouldBeNil)

		err = client.Call("Dsync.Relinquish", args, &reply)
		So(err, ShouldBeNil)

		argsB := &queue.Mssg{
			Timestamp: time.Now().AddDate(0, 0, 1),
			Node:      "127.0.0.33",
			Replied:   false,
		}
		testServer.dsync.push(argsB)

		err = client.Call("Dsync.Relinquish", argsB, &reply)
		So(err, ShouldBeNil)

		testServer.dsync.pop()

		So(testServer.dsync.reqQueue.Len(), ShouldEqual, 0)
	})
}

func Test_Sanitize(t *testing.T) {
	Convey("Test Sanitize Queue", t, func() {
		args := &queue.Mssg{
			Timestamp: time.Now().Add(-Timeout * 2),
			Node:      "127.0.0.1",
			Replied:   false,
		}
		testServer.dsync.push(args)

		check := (*testServer.dsync.reqQueue)[0]
		So(testServer.dsync.reqQueue.Len(), ShouldEqual, 1)
		So(check.Timestamp, ShouldEqual, args.Timestamp)

		testServer.SanitizeQueue()
		So(testServer.dsync.reqQueue.Len(), ShouldEqual, 0)
	})
}
