package quorums

import (
	"testing"

	"github.com/bparli/dmutex/bintree"
	. "github.com/smartystreets/goconvey/convey"
)

func Test_NewQuorums(t *testing.T) {
	Convey("Test init Quorums and build current quorums", t, func() {
		nodes := []string{"192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"}
		t, _ := bintree.NewTree(nodes)
		testQuorum := NewQuorums(t, nodes, "192.168.1.14")
		So(len(testQuorum.MyCurrQuorums), ShouldEqual, 0)
		So(len(testQuorum.MyQuorums), ShouldEqual, 2)
		So(len(testQuorum.CurrPeers), ShouldEqual, 5)
		So(testQuorum.CurrPeers["192.168.1.12"], ShouldBeFalse)
		So(testQuorum.Healthy, ShouldBeFalse)

		So(len(testQuorum.CurrMembers), ShouldEqual, 5)
		So(testQuorum.CurrMembers["192.168.1.16"], ShouldBeFalse)

		for _, member := range nodes {
			testQuorum.CurrMembers[member] = true
		}

		err := testQuorum.BuildCurrQuorums()
		So(err, ShouldBeNil)
		So(testQuorum.CurrPeers["192.168.1.12"], ShouldEqual, true)
		So(testQuorum.CurrPeers["192.168.1.16"], ShouldEqual, true)
		comp := make(map[int][]string)
		comp[0] = []string{"192.168.1.14", "192.168.1.15", "192.168.1.16"}
		comp[1] = []string{"192.168.1.12", "192.168.1.13", "192.168.1.14"}
		So(testQuorum.MyCurrQuorums[0][1], ShouldEqual, comp[0][1])
		So(testQuorum.MyCurrQuorums[1][2], ShouldEqual, comp[1][2])

		So(testQuorum.Healthy, ShouldBeFalse)
		testQuorum.CheckHealth()
		So(testQuorum.Healthy, ShouldBeTrue)

	})
}
