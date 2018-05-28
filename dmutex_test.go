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
		testServer, err = server.NewDistSyncServer("127.0.0.1", 10, 10*time.Second)
		if err != nil {
			return
		}
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
		dmutex.rpcServer = testServer

		ch := make(chan error, 1)
		var wg sync.WaitGroup

		wg.Add(1)
		go sendRequest("127.0.0.1", &wg, ch)
		wg.Wait()

		err := dmutex.checkForError(ch)
		So(err, ShouldBeNil)

		dmutex.relinquish(ch)

		err = dmutex.checkForError(ch)
		So(err, ShouldBeNil)

		close(ch)
	})
}
