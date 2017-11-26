package server

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/rpc"
	"sync"
	"testing"
	"time"

	"github.com/bparli/dmutex/queue"
	. "github.com/smartystreets/goconvey/convey"
)

var testServer *RPCServer

func Test_BasicServer(t *testing.T) {
	Convey("Init server, Send request, and Gather Reply", t, func() {
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			fmt.Fprintln(w, "Hello, client")
		}))
		defer ts.Close()

		testServer = NewRPCServer("127.0.0.1", 3, 10*time.Second)

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

	})
}
