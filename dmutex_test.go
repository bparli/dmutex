package dmutex

import (
	"sync"
	"testing"
	"time"

	"github.com/bparli/dmutex/bintree"
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
	started = true
}

func Test_Dmutex(t *testing.T) {
	Convey("Test Send Request and Relinquish", t, func() {

		peersMap := make(map[string]bool)
		dmutex = &Dmutex{
			currPeers: &peers{
				peersMap: peersMap,
				mutex:    &sync.RWMutex{},
			},
		}

		nodes := []string{"127.0.0.1"}
		t, _ := bintree.NewTree(nodes)
		dmutex.Quorums = quorums.NewQuorums(t, nodes, "127.0.0.1")
		dmutex.currPeers.peersMap = dmutex.Quorums.Peers

		setupTestRPC()
		dmutex.rpcServer = testServer

		err := dmutex.sendRequests(dmutex.Quorums.Peers)
		So(err, ShouldBeNil)

		ch := make(chan *lockError, 1)
		dmutex.relinquish(ch)
		close(ch)
	})
}
