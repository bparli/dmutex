# Dmutex

Dmutex is a distributed mutex package written in Go.  It takes a quorum based approach to managing locks across n distributed nodes.

Overview
===============
Dmutex implements the [Agarwal-El Abbadi quorum-based algorithm](https://users.soe.ucsc.edu/~scott/courses/Fall11/221/Papers/Sync/agrawal-tocs91.pdf).  A good overview of the algorithm can be found [here](https://www.cs.uic.edu/~ajayk/Chapter9.pdf) starting on page 50.

Example
===============
Dmutex is initialized with the local node's address, the addresses of the entire cluster, and a timeout for the gRPC calls.  Optional file locations of a TLS certificate and key can be passed in order to secure cluster traffic.
```
import (
  "github.com/bparli/dmutex"
)

func dLockTest() {
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
}
```

The timeout setting is used solely for acquiring the lock and not for holding the lock.  A supplemental method for sanity/health checking the lock owner is still alive and means to still hold the lock.  Its important to set the timeout for longer than the maximum time the lock is expected to be held for. 
