package server

import (
	"sync"
	"testing"

	. "github.com/smartystreets/goconvey/convey"
)

func Test_CheckProgress(t *testing.T) {
	Convey("Test checking lock progress", t, func() {
		testPeers := &peersMap{
			mutex:   &sync.RWMutex{},
			replies: make(map[string]bool),
		}
		testPeers.replies["127.0.0.10"] = true
		testPeers.replies["127.0.0.11"] = false

		So(testPeers.checkProgress(), ShouldEqual, ProgressNotAcquired)

		testPeers.replies["127.0.0.11"] = true
		So(testPeers.checkProgress(), ShouldEqual, ProgressAcquired)
	})
}

func Test_ResetProgress(t *testing.T) {
	Convey("Test reseting lock progress", t, func() {
		testPeers := &peersMap{
			mutex:   &sync.RWMutex{},
			replies: make(map[string]bool),
		}
		testPeers.replies["127.0.0.10"] = true
		testPeers.replies["127.0.0.11"] = true
		newreplies := make(map[string]bool)
		newreplies["127.0.0.12"] = true
		newreplies["127.0.0.13"] = true
		testPeers.ResetProgress(newreplies)

		_, ok := testPeers.replies["127.0.0.10"]
		So(ok, ShouldBeFalse)
		So(testPeers.replies["127.0.0.13"], ShouldBeFalse)
		So(testPeers.replies["127.0.0.12"], ShouldBeFalse)
	})
}

func Test_SubstitutePeers(t *testing.T) {
	Convey("Test reseting lock progress", t, func() {
		testPeers := &peersMap{
			mutex:   &sync.RWMutex{},
			replies: make(map[string]bool),
		}
		testPeers.replies["127.0.0.10"] = false
		testPeers.replies["127.0.0.11"] = false

		repPeers := make(map[string]bool)
		repPeers["127.0.0.12"] = false
		repPeers["127.0.0.13"] = false

		testPeers.SubstitutePeer("127.0.0.10", repPeers)
		currPeers := testPeers.GetPeers()
		_, ok := currPeers["127.0.0.10"]
		So(ok, ShouldBeFalse)
		So(currPeers["127.0.0.11"], ShouldBeFalse)
		So(currPeers["127.0.0.12"], ShouldBeFalse)
		So(currPeers["127.0.0.13"], ShouldBeFalse)
	})
}
