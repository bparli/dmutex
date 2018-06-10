package client

import (
	"testing"

	. "github.com/smartystreets/goconvey/convey"
	"google.golang.org/grpc"
)

func Test_SetupConnection(t *testing.T) {
	Convey("Test setup grpc connection", t, func() {
		testConfig := &Config{
			RPCPort: "7000",
			TLSCRT:  "",
		}
		conn, err := setupConn("127.0.0.1", testConfig)
		So(err, ShouldBeNil)
		So(conn, ShouldHaveSameTypeAs, &grpc.ClientConn{})

		testConfig.TLSCRT = "missing.pem"
		_, err = setupConn("127.0.0.1", testConfig)
		So(err, ShouldNotBeNil)

		testConfig.TLSCRT = "../fixtures/dmutex.crt"
		_, err = setupConn("127.0.0.1", testConfig)
		So(err, ShouldBeNil)
	})
}
