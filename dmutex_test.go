package dmutex

import (
	"sync"
	"testing"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/queue"
	"github.com/bparli/dmutex/server"
	. "github.com/smartystreets/goconvey/convey"
)

// use same RPC server for all tests in dmutex package.
var testServer *server.RPCServer

func setupTestRPC() {
	if testServer == nil {
		testServer = server.NewRPCServer("127.0.0.1", 10, 1*time.Second)
	}
}

func Test_Dmutex(t *testing.T) {
	Convey("Test Send Request and Relinquish", t, func() {

		d := &Dmutex{
			rpcServer: testServer,
		}

		nodes := []string{"127.0.0.1"}
		t, _ := bintree.NewTree(nodes)
		dmutex.Quorums = newQuorums(t, nodes, "127.0.0.1")
		for _, member := range nodes {
			dmutex.Quorums.currMembers[member] = true
		}

		err := dmutex.Quorums.buildCurrQuorums()
		So(err, ShouldBeNil)

		setupTestRPC()

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

		err = d.checkForError(ch)
		So(err, ShouldBeNil)

		d.relinquish(args, ch)

		err = d.checkForError(ch)
		So(err, ShouldBeNil)

		close(ch)
	})
}
