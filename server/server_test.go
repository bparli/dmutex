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
		testServer, err = NewDistSyncServer("127.0.0.1", 10, 10*time.Second)
		if err != nil {
			return
		}
	}
	started = true
}

func Test_BasicServer(t *testing.T) {
	Convey("Init server, Send request, and Gather Reply", t, func() {

		setupTestRPC()

		args := &queue.Mssg{
			Node:    "127.0.0.1",
			Replied: false,
		}

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

		peers := make(map[string]bool)
		peers["127.0.0.1"] = true
		errReply := testServer.GatherReplies(args, peers)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		reply, err = client.Reply(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)
		So(reply, ShouldNotBeNil)

		peers["127.0.0.1"] = false
		errReply = testServer.GatherReplies(args, peers)
		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = client.Reply(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)
		So(errReply, ShouldNotBeNil)

	})
}

func Test_Inquire(t *testing.T) {
	Convey("Test Inquire", t, func() {
		d := &DistSyncServer{
			reqQueue:  nil,
			qMutex:    &sync.RWMutex{},
			progMutex: &sync.RWMutex{},
			localAddr: "127.0.0.2",
			reqsCh:    nil,
			repliesCh: nil,
		}

		testServer.SetProgress(ProgressNotAcquired)
		err := d.sendInquire("127.0.0.1")
		So(err, ShouldBeNil)

		testServer.SetProgress(ProgressAcquired)
		go func() {
			time.Sleep(1 * time.Second)
			testServer.SetProgress(ProgressNotAcquired)
		}()
		err = d.sendInquire("127.0.0.1")
		So(err, ShouldBeNil)

	})
}

func Test_Relinquish(t *testing.T) {
	Convey("Test Relinquish", t, func() {
		args := &queue.Mssg{
			Node:    "127.0.0.1",
			Replied: false,
		}
		testServer.push(args)

		var opts []grpc.DialOption
		opts = append(opts, grpc.WithInsecure())
		conn, _ := grpc.Dial("127.0.0.1:7070", opts...)
		defer conn.Close()
		client := pb.NewDistSyncClient(conn)
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()

		_, err := client.Relinquish(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)

		argsB := &queue.Mssg{
			Node:    "127.0.0.33",
			Replied: false,
		}
		testServer.push(argsB)

		ctx, cancel = context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		_, err = client.Relinquish(ctx, &pb.Node{Node: "127.0.0.1"})
		So(err, ShouldBeNil)

		testServer.pop()

		So(testServer.reqQueue.Len(), ShouldEqual, 0)
	})
}

func Test_Sanitize(t *testing.T) {
	Convey("Test Sanitize Queue", t, func() {
		args := &queue.Mssg{
			Timestamp: time.Now().Add(-Timeout * 2),
			Node:      "127.0.0.1",
			Replied:   false,
		}
		testServer.push(args)

		check := (*testServer.reqQueue)[0]
		So(testServer.reqQueue.Len(), ShouldEqual, 1)
		So(check.Timestamp, ShouldEqual, args.Timestamp)

		testServer.SanitizeQueue()
		So(testServer.reqQueue.Len(), ShouldEqual, 0)
	})
}
