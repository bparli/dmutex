package bintree

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_BinTree(t *testing.T) {
	Convey("Init a bintree and traverse a subnode's paths", t, func() {
		nodes := []string{"192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"}
		tr, err := NewTree(nodes)

		So(err, ShouldBeNil)
		So(tr.NumLeaves, ShouldEqual, 2)
		So(tr.root.val, ShouldEqual, "192.168.1.14")
		So(tr.AllPaths(), ShouldContain, []string{"192.168.1.14", "192.168.1.15", "192.168.1.16"})
		So(tr.AllPaths(), ShouldContain, []string{"192.168.1.14", "192.168.1.12", "192.168.1.13"})

		paths := tr.NodePaths("192.168.1.15")
		So(paths, ShouldContain, []string{"192.168.1.15", "192.168.1.16"})
		So(paths, ShouldHaveLength, 1)

	})
}
