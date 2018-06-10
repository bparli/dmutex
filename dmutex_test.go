package dmutex

import (
	"testing"
	"time"

	"github.com/bparli/dmutex/client"
	"github.com/bparli/dmutex/server"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	testServer *server.DistSyncServer
	started    bool
)

func setupTestRPC() {
	var err error
	if !started || testServer == nil {
		testServer, err = server.NewDistSyncServer("127.0.0.2", 1, 3*time.Second, "", "")
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

func Test_DmutexError(t *testing.T) {
	Convey("Test lock timeout error", t, func() {
		testdm := NewDMutex("127.0.0.2", []string{"127.0.0.2", "127.0.0.100"}, 2*time.Second, "", "")
		err := testdm.Lock()
		So(err, ShouldNotBeNil)
	})
}

func Test_Dmutex(t *testing.T) {
	Convey("Test dmutex lock and unlock", t, func() {
		testdm := NewDMutex("127.0.0.1", []string{"127.0.0.1"}, 2*time.Second, "", "")
		err := testdm.Lock()
		So(err, ShouldBeNil)
		testdm.UnLock()

		// should be able to lock again
		err = testdm.Lock()
		So(err, ShouldBeNil)
	})
}

// func Test_Dmutex(t *testing.T) {
// 	Convey("Test lock and unlock", t, func() {
// 		dmutex = &Dmutex{}
//
// 		nodes := []string{"127.0.0.1"}
// 		t, _ := bintree.NewTree(nodes)
// 		dmutex.Quorums = quorums.NewQuorums(t, nodes, "127.0.0.1")
//
// 		setupTestRPC()
// 		server.Peers.ResetProgress(dmutex.Quorums.Peers)
// 		dmutex.rpcServer = testServer
//
// 		lockReq := &pb.LockReq{Node: "127.0.0.1", Tstmp: ptypes.TimestampNow()}
// 		err := dmutex.sendRequests(dmutex.Quorums.Peers, lockReq)
// 		So(err, ShouldBeNil)
//
// 		dmutex.relinquish()
// 	})
// }
