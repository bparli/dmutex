package dmutex

import (
	"net"
	"sync"
	"testing"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/queue"
	"github.com/bparli/dmutex/quorums"
	"github.com/hashicorp/memberlist"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Delegates(t *testing.T) {
	Convey("Test Delegates", t, func() {
		nodes := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"}
		t, _ := bintree.NewTree(nodes)
		qms := quorums.NewQuorums(t, nodes, "127.0.0.3")
		for _, member := range nodes {
			qms.CurrMembers[member] = true
		}

		err := qms.BuildCurrQuorums()
		So(err, ShouldBeNil)

		testDel := newEventDelegate()
		ip, _, err := net.ParseCIDR("127.0.0.5/32")
		So(err, ShouldBeNil)
		testNode := &memberlist.Node{Name: "127.0.0.5", Addr: ip}

		setupTestRPC()

		dmutex = &Dmutex{
			Quorums:      qms,
			rpcServer:    testServer,
			localRequest: &queue.Mssg{},
			gateway:      &sync.Mutex{},
		}

		So(qms.CurrPeers["127.0.0.5"], ShouldBeTrue)
		So(qms.CurrMembers["127.0.0.5"], ShouldBeTrue)

		qms.Ready = true
		testDel.NotifyLeave(testNode)
		So(qms.CurrPeers["127.0.0.5"], ShouldBeFalse)
		So(qms.CurrMembers["127.0.0.5"], ShouldBeFalse)

		testDel.NotifyJoin(testNode)
		So(qms.CurrPeers["127.0.0.5"], ShouldBeTrue)
		So(qms.CurrMembers["127.0.0.5"], ShouldBeTrue)

	})
}
