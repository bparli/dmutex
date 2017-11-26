package dmutex

import (
	"testing"

	"github.com/bparli/dmutex/bintree"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_NewQuorums(t *testing.T) {
	Convey("Test init Quorums and build current quorums", t, func() {
		nodes := []string{"192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"}
		t, _ := bintree.NewTree(nodes)
		testQuorum := newQuorums(t, nodes, "192.168.1.14")
		So(len(testQuorum.myCurrQuorums), ShouldEqual, 0)
		So(len(testQuorum.myQuorums), ShouldEqual, 2)
		So(len(testQuorum.currPeers), ShouldEqual, 5)
		So(testQuorum.currPeers["192.168.1.12"], ShouldBeFalse)
		So(testQuorum.Healthy, ShouldBeFalse)

		So(len(testQuorum.currMembers), ShouldEqual, 5)
		So(testQuorum.currMembers["192.168.1.16"], ShouldBeFalse)

		for _, member := range nodes {
			testQuorum.currMembers[member] = true
		}

		err := testQuorum.buildCurrQuorums()
		So(err, ShouldBeNil)
		So(testQuorum.currPeers["192.168.1.12"], ShouldEqual, true)
		So(testQuorum.currPeers["192.168.1.16"], ShouldEqual, true)
		comp := make(map[int][]string)
		comp[0] = []string{"192.168.1.14", "192.168.1.15", "192.168.1.16"}
		comp[1] = []string{"192.168.1.12", "192.168.1.13", "192.168.1.14"}
		So(testQuorum.myCurrQuorums[0][1], ShouldEqual, comp[0][1])
		So(testQuorum.myCurrQuorums[1][2], ShouldEqual, comp[1][2])

		So(testQuorum.Healthy, ShouldBeFalse)
		testQuorum.checkHealth()
		So(testQuorum.Healthy, ShouldBeTrue)

	})
}
