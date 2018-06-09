package dmutex

import (
	"testing"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/client"
	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"
	"github.com/golang/protobuf/ptypes"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	testServer *server.DistSyncServer
	started    bool
)

func setupTestRPC() {
	var err error
	if !started || testServer == nil {
		testServer, err = server.NewDistSyncServer("127.0.0.1", 1, 3*time.Second, "", "")
		if err != nil {
			return
		}
	}
	clientConfig = &client.Config{
		LocalAddr:  localAddr,
		RPCPort:    server.RPCPort,
		RPCTimeout: 3 * time.Second,
	}

	started = true
}

func Test_Dmutex(t *testing.T) {
	Convey("Test Send Request and Relinquish", t, func() {

		dmutex = &Dmutex{}

		nodes := []string{"127.0.0.1"}
		t, _ := bintree.NewTree(nodes)
		dmutex.Quorums = quorums.NewQuorums(t, nodes, "127.0.0.1")

		setupTestRPC()
		server.Peers.ResetProgress(dmutex.Quorums.Peers)
		dmutex.rpcServer = testServer

		lockReq := &pb.LockReq{Node: "127.0.0.1", Tstmp: ptypes.TimestampNow()}
		err := dmutex.sendRequests(dmutex.Quorums.Peers, lockReq)
		So(err, ShouldBeNil)

		dmutex.relinquish()
	})
}
