package dmutex

import (
	"sync"
	"testing"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/queue"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	testServer *server.RPCServer
	started    bool
)

func setupTestRPC() {
	var err error
	if !started || testServer == nil {
		testServer, err = server.NewRPCServer("127.0.0.1", 10, 10*time.Second)
		if err != nil {
			return
		}
	}
	started = true
	testServer.SetReady(true)
}

func Test_Dmutex(t *testing.T) {
	Convey("Test Send Request and Relinquish", t, func() {

		dmutex = &Dmutex{}

		nodes := []string{"127.0.0.1"}
		t, _ := bintree.NewTree(nodes)
		dmutex.Quorums = quorums.NewQuorums(t, nodes, "127.0.0.1")
		for _, member := range nodes {
			dmutex.Quorums.CurrMembers[member] = true
		}

		err := dmutex.Quorums.BuildCurrQuorums()
		So(err, ShouldBeNil)

		setupTestRPC()
		dmutex.rpcServer = testServer

		dmutex.Quorums.Ready = true
		dmutex.Quorums.Healthy = true

		ch := make(chan error, 1)
		var wg sync.WaitGroup
		args := &queue.Mssg{
			Timestamp: time.Now(),
			Node:      "127.0.0.1",
			Replied:   false,
		}

		wg.Add(1)
		go sendRequest(args, "127.0.0.1", &wg, ch)
		wg.Wait()

		err = dmutex.checkForError(ch)
		So(err, ShouldBeNil)

		dmutex.relinquish(args, ch)

		err = dmutex.checkForError(ch)
		So(err, ShouldBeNil)

		close(ch)
	})
}
