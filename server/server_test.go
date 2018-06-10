package server

import (
	"context"
	"sync"
	"testing"
	"time"

	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/queue"
	"github.com/golang/protobuf/ptypes"
	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

// use same RPC server for all tests in dmutex package.
var (
	testServer *DistSyncServer
	started    bool
)

func setupTestRPC() {
	var err error
	if !started || testServer == nil {
		testServer, err = NewDistSyncServer("127.0.0.1", 10, 2*time.Second, "", "")
		if err != nil {
			return
		}
	}

	started = true
}

func Test_BasicServer(t *testing.T) {
	Convey("Init server, Send request, and Gather Reply", t, func() {

		setupTestRPC()

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		conn, _ := grpc.Dial("127.0.0.1:7070", opts...)

		defer conn.Close()
		client := pb.NewDistSyncClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		reply, err := client.Request(ctx, &pb.LockReq{Node: "127.0.0.1", Tstmp: ptypes.TimestampNow()})
		So(err, ShouldBeNil)
		So(reply, ShouldHaveSameTypeAs, &pb.Node{})

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		reply, err = client.Reply(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)
		So(reply, ShouldNotBeNil)

		currPeers := make(map[string]bool)
		currPeers["127.0.0.1"] = true
		Peers.ResetProgress(currPeers)
		errReply := testServer.GatherReplies()
		So(errReply, ShouldBeNil)
	})
}

func Test_ErrorGatherReplies(t *testing.T) {
	Convey("Init server, Send request, and Gather Reply (with error condition)", t, func() {

		setupTestRPC()

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		conn, _ := grpc.Dial("127.0.0.1:7070", opts...)

		defer conn.Close()
		client := pb.NewDistSyncClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		reply, err := client.Request(ctx, &pb.LockReq{Node: "127.0.0.1", Tstmp: ptypes.TimestampNow()})
		So(err, ShouldBeNil)
		So(reply, ShouldHaveSameTypeAs, &pb.Node{})

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		reply, err = client.Reply(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)
		So(reply, ShouldNotBeNil)

		currPeers := make(map[string]bool)
		currPeers["127.0.0.99"] = true
		Peers.ResetProgress(currPeers)
		errReply := testServer.GatherReplies()
		So(errReply, ShouldNotBeNil)
	})
}

func Test_Relinquish(t *testing.T) {
	Convey("Test Relinquish", t, func() {
		args := &queue.Mssg{
			Node:    "127.0.0.1",
			Replied: false,
		}
		testServer.pop()
		testServer.push(args)

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		conn, _ := grpc.Dial("127.0.0.1:7070", opts...)
		defer conn.Close()
		client := pb.NewDistSyncClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Relinquish(ctx, &pb.Node{Node: "127.0.0.1"})
		So(testServer.reqQueue.Len(), ShouldEqual, 0)
		So(err, ShouldBeNil)
		So(testServer.reqQueue.Len(), ShouldEqual, 0)

		argsB := &queue.Mssg{
			Node:    "127.0.0.33",
			Replied: false,
		}
		testServer.push(argsB)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = client.Relinquish(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)
		So(testServer.reqQueue.Len(), ShouldEqual, 1)

		testServer.pop()
		So(testServer.reqQueue.Len(), ShouldEqual, 0)
	})
}

func Test_Inquire(t *testing.T) {
	Convey("Test Inquire", t, func() {
		So(testServer.reqQueue.Len(), ShouldEqual, 0)
		Peers = &peersMap{
			mutex:   &sync.RWMutex{},
			replies: make(map[string]bool),
		}
		Peers.replies["127.0.0.10"] = true
		Peers.replies["127.0.0.11"] = false

		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err := testServer.Inquire(ctx, &pb.Node{Node: "127.0.0.10"})
		So(err, ShouldBeNil)
		So(rep.Yield, ShouldBeTrue)
		So(Peers.replies["127.0.0.10"], ShouldBeFalse)

		Peers.replies["127.0.0.10"] = true
		Peers.replies["127.0.0.11"] = true

		go func() {
			time.Sleep(1 * time.Second)
			Peers.mutex.Lock()
			Peers.replies["127.0.0.10"] = false
			Peers.replies["127.0.0.11"] = false
			Peers.mutex.Unlock()
		}()

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		rep, err = testServer.Inquire(ctx, &pb.Node{Node: "127.0.0.10"})
		So(err, ShouldBeNil)
		So(rep.Relinquish, ShouldBeTrue)

	})
}

func Test_UndoReplies(t *testing.T) {
	Convey("Test undo Lock request Replies", t, func() {
		args := &queue.Mssg{
			Node:    "127.0.0.10",
			Replied: true,
		}
		testServer.push(args)
		args.Node = "127.0.0.11"
		testServer.push(args)

		So(testServer.readMin().Replied, ShouldBeTrue)
		testServer.undoReplies()
		So(testServer.readMin().Replied, ShouldBeFalse)
		testServer.pop()
		So(testServer.readMin().Replied, ShouldBeFalse)
	})
}

func Test_DrainRepliesChannel(t *testing.T) {
	Convey("Test draining Replies channel", t, func() {
		testServer.repliesCh <- &pb.Node{Node: "127.0.0.10"}
		testServer.repliesCh <- &pb.Node{Node: "127.0.0.11"}
		testServer.repliesCh <- &pb.Node{Node: "127.0.0.12"}
		testServer.repliesCh <- &pb.Node{Node: "127.0.0.13"}
		So(len(testServer.repliesCh), ShouldEqual, 4)
		testServer.DrainRepliesCh()
		So(len(testServer.repliesCh), ShouldEqual, 0)
	})
}
