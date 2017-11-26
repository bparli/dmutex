package dmutex

import (
	"net"
	"testing"

	"github.com/bparli/dmutex/bintree"
	"github.com/hashicorp/memberlist"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_Delegates(t *testing.T) {
	Convey("Test Delegates", t, func() {
		nodes := []string{"127.0.0.1", "127.0.0.2", "127.0.0.3", "127.0.0.4", "127.0.0.5"}
		t, _ := bintree.NewTree(nodes)
		quorums := newQuorums(t, nodes, "127.0.0.3")
		for _, member := range nodes {
			quorums.currMembers[member] = true
		}

		err := quorums.buildCurrQuorums()
		So(err, ShouldBeNil)

		testDel := newEventDelegate()
		ip, _, err := net.ParseCIDR("127.0.0.5/32")
		So(err, ShouldBeNil)
		testNode := &memberlist.Node{Name: "127.0.0.5", Addr: ip}

		setupTestRPC()

		quorums.Ready = true
		testDel.NotifyLeave(testNode)
		So(quorums.currPeers["127.0.0.5"], ShouldBeFalse)
		So(quorums.currMembers["127.0.0.5"], ShouldBeFalse)

		testDel.NotifyJoin(testNode)
		So(quorums.currPeers["127.0.0.5"], ShouldBeTrue)
		So(quorums.currMembers["127.0.0.5"], ShouldBeTrue)

	})
}
