# Dmutex
[![Go Report Card](https://goreportcard.com/badge/github.com/bparli/dmutex)](https://goreportcard.com/report/github.com/bparli/dmutex)
[![GoDoc](https://img.shields.io/badge/godoc-reference-blue.svg?style=flat-square)](http://godoc.org/github.com/bparli/dmutex)

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

Note the timeout setting is used solely for acquiring the lock and not for holding the lock.  A supplemental method for sanity/health checking the lock owner is still alive and means to still hold the lock.  Its important to set the timeout for longer than the maximum time the lock is expected to be held for.

Approach
===============

### Lock process
The lock process largely follows the Agarwal-El Abbadi quorum-based algorithm

The steps in the lock process are as follows:
- broadcast lock Request message to all peers in the quorums
- collect all responses within certain time-out window
  - responses from all peers in the local node's quorums are expected
- if messages to some of the nodes in the quorum failed
  - calculate replacement paths based on the algorithm
  - generate unique substitute nodes and send the lock Request to them
  - if substitutes are not possible (according to the algorithm)
    - fallback to the naive approach and broadcast the lock Request to all remaining nodes in the cluster
    - if quorum met (at least `n/2 + 1` responded) then grant lock
- each node sends a Reply to the request at the head of its queue, indicating consent to the lock
- if requests arrive out of order (according to timestamps)
  - an Inquire message is sent to the head of the queue
  - when a node receives an Inquire message it:
    - blocks on the Inquire if it has already been fully granted the lock
    - Yields the lock if it has not yet been fully granted the lock
- if the lock was not granted within the timeout window Relinquish any lock Replies


### Unlock process

To unlock a Relinquish message is sent to all nodes that were originally sent lock Requests.
This is ideally only peers in the local node's quorums, but in a degraded state could be more, or even all of the nodes in the cluster (as described above).

### Dealing with Stale Locks

A 'stale' lock is a lock that has been granted to a node, but for some reason that node has not released it in a reasonable timeframe.  This could happen for any number of reasons; the node crashed, the process paused, the network, etc.

Each time a new lock request arrives the node sends a Validate message to the node at the head of its request queue since thats the one it believes has the lock.  If the grpc message fails or the reply indicates that node no longer owns the lock (for whatever reason), the lock request is purged from the head of the queue and the next in line is processed.

Again, note there is no timeout for holding the lock.  The Validate message is meant to clear stale locks

Logging
=============
Log level is set by the environment variable `LOG_LEVEL`
