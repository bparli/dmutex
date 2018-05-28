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
		So(len(testQuorum.MyQuorums), ShouldEqual, 2)
		So(testQuorum.NumPeers, ShouldEqual, 4)
	})
}

func Test_SubstitutePaths(t *testing.T) {
	Convey("Test substituting failed nodes with two of their paths", t, func() {
		nodes := []string{"192.168.1.8", "192.168.1.9", "192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16", "192.168.1.17", "192.168.1.18", "192.168.1.19", "192.168.1.20"}
		t, _ := bintree.NewTree(nodes)
		testQuorum := NewQuorums(t, nodes, "192.168.1.14")
		So(testQuorum.SubstitutePaths("192.168.1.17"), ShouldHaveLength, 4)
		So(testQuorum.SubstitutePaths("192.168.1.17"), ShouldContain, "192.168.1.19")
		So(testQuorum.SubstitutePaths("192.168.1.17"), ShouldContain, "192.168.1.15")
		So(testQuorum.SubstitutePaths("192.168.1.8"), ShouldHaveLength, 1)
	})
}

func Test_SubstitutePaths_NoChildren(t *testing.T) {
	Convey("Test substituting failed nodes with with no children", t, func() {
		nodes := []string{"192.168.1.8", "192.168.1.9", "192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16", "192.168.1.17", "192.168.1.18", "192.168.1.19", "192.168.1.20"}
		t, _ := bintree.NewTree(nodes)
		testQuorum := NewQuorums(t, nodes, "192.168.1.9")
		So(testQuorum.SubstitutePaths("192.168.1.11"), ShouldBeNil)
	})
}

func Test_Peers(t *testing.T) {
	Convey("Test substituting failed nodes with with no children", t, func() {
		nodes := []string{"192.168.1.8", "192.168.1.9", "192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16", "192.168.1.17", "192.168.1.18", "192.168.1.19", "192.168.1.20"}
		t, _ := bintree.NewTree(nodes)
		testQuorum := NewQuorums(t, nodes, "192.168.1.10")
		So(testQuorum.Peers, ShouldContainKey, "192.168.1.8")
		So(testQuorum.Peers, ShouldContainKey, "192.168.1.14")
		So(testQuorum.Peers, ShouldContainKey, "192.168.1.12")
		So(testQuorum.Peers, ShouldContainKey, "192.168.1.11")
	})
}
