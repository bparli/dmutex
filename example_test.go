package dmutex_test

import (
	"time"

	"github.com/bparli/dmutex"
	log "github.com/sirupsen/logrus"
)

// Example initializing and using dmutex
func Example() {
	nodes := []string{"192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"}
	dm := dmutex.NewDMutex("192.168.1.12", nodes, 3*time.Second, "server.crt", "server.key")

	if err := dm.Lock(); err != nil {
		log.Errorln("Lock error", err)
	} else {
		log.Debugln("Acquired Lock")
		time.Sleep(time.Duration(100) * time.Millisecond)
		dm.UnLock()
		log.Debugln("Released Lock")
	}
}
