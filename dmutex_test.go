package dmutex

import (
	"os"
	"testing"
	"time"

	"github.com/bparli/dmutex/bintree"
	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"

	. "github.com/smartystreets/goconvey/convey"
)

var (
	testServer *server.DistSyncServer
	started    bool
)

func Test_DmutexError(t *testing.T) {
	Convey("Test lock timeout error", t, func() {
		testdm := NewDMutex("127.0.0.2", []string{"127.0.0.2", "127.0.0.1", "127.0.0.3"}, 2*time.Second, "", "")
		err := testdm.Lock()
		So(err, ShouldNotBeNil)
	})
}

func Test_Dmutex(t *testing.T) {
	Convey("Test dmutex lock and unlock", t, func() {
		testdm := NewDMutex("127.0.0.1", []string{"127.0.0.1"}, 2*time.Second, "", "")
		err := testdm.Lock()
		So(err, ShouldBeNil)
		testdm.UnLock()

		// should be able to lock again
		err = testdm.Lock()
		So(err, ShouldBeNil)
	})
}

func Test_ValidateAddr(t *testing.T) {
	Convey("Test validating addresses", t, func() {
		testAddr, err := validateAddr("127.0.0.99")
		So(err, ShouldBeNil)
		So(testAddr, ShouldEqual, "127.0.0.99")

		testAddr, err = validateAddr("localhost")
		So(err, ShouldBeNil)
		So(testAddr, ShouldEqual, "127.0.0.1")

		_, err = validateAddr("Some.Dummy.Address.notlocalhost")
		So(err, ShouldNotBeNil)
	})
}

func Test_Broadcast(t *testing.T) {
	Convey("Test broadcast", t, func() {
		testdm := NewDMutex("127.0.0.4", []string{"127.0.0.4", "127.0.0.5"}, 2*time.Second, "", "")

		// trick into thinking 127.0.0.4 should be broadcast to and replied from
		testMap := make(map[string]bool)
		testMap["127.0.0.5"] = false
		testMap["127.0.0.4"] = false

		testdm.broadcast(&pb.LockReq{Node: "127.0.0.4", Tstmp: ptypes.TimestampNow()})

		server.Peers.ResetProgress(testMap)
		err := testdm.rpcServer.GatherReplies(1)
		So(err, ShouldBeNil)
	})
}

func Test_ReplacementMap(t *testing.T) {
	Convey("Test replacement map for a node", t, func() {
		nodes := []string{"192.168.1.8", "192.168.1.9", "192.168.1.10", "192.168.1.11", "192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16", "192.168.1.17", "192.168.1.18", "192.168.1.19", "192.168.1.20"}
		t, _ := bintree.NewTree(nodes)
		testQuorum := quorums.NewQuorums(t, "192.168.1.9")

		// [[192.168.1.10 192.168.1.12 192.168.1.13] [192.168.1.10 192.168.1.12 192.168.1.11] [192.168.1.10 192.168.1.8 192.168.1.9]]
		subPaths := testQuorum.SubstitutePaths("192.168.1.10")

		newPaths := genReplacementMap(testQuorum.Peers, subPaths)
		// map[192.168.1.12:false 192.168.1.13:false 192.168.1.11:false]
		So(len(newPaths), ShouldEqual, 3)
		So(newPaths, ShouldContainKey, "192.168.1.12")
		So(newPaths, ShouldContainKey, "192.168.1.13")
		So(newPaths, ShouldContainKey, "192.168.1.11")
	})
}

func Test_LogLevel(t *testing.T) {
	Convey("Test setting log level", t, func() {
		setLogLevel()
		So(log.GetLevel(), ShouldEqual, log.InfoLevel)

		os.Setenv("LOG_LEVEL", "deBUg")
		setLogLevel()
		So(log.GetLevel(), ShouldEqual, log.DebugLevel)

		os.Setenv("LOG_LEVEL", "panic")
		setLogLevel()
		So(log.GetLevel(), ShouldEqual, log.PanicLevel)

		os.Setenv("LOG_LEVEL", "sklsjlksd")
		setLogLevel()
		So(log.GetLevel(), ShouldEqual, log.InfoLevel)
	})
}
