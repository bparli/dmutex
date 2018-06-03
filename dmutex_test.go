package dmutex

import (
	"testing"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/client"
	"github.com/bparli/dmutex/quorums"
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
		testServer, err = server.NewDistSyncServer("127.0.0.1", 1, 3*time.Second)
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

		err := dmutex.sendRequests(dmutex.Quorums.Peers)
		So(err, ShouldBeNil)

		ch := make(chan *client.LockError, 1)
		dmutex.relinquish(ch)
		close(ch)
	})
}
