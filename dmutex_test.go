package dmutex

import (
	"testing"
	"time"

	"github.com/bparli/dmutex/server"

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
