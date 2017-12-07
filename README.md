# Dmutex

Dmutex is a distributed mutex package written in Go.  It takes a quorum based approach to managing locks across n distributed nodes.

Overview
===============
Dmutex is inspired by the [Agarwal-El Abbadi quorum-based algorithm](http://www.dcc.fc.up.pt/~INES/aulas/1314/SDM/papers/FaultToleranceDMEagrawal.pdf) with a notable difference; instead of running at a degradation anytime a node leaves the cluster, [Memberlist](https://github.com/hashicorp/memberlist) is used to pro-actively detect a node failure and re-compute the quorums.  There is a period where the cluster must fully converge to the new quorums, however.  A good overview of the algorithm can be found [here](https://www.cs.uic.edu/~ajayk/Chapter9.pdf) starting on page 50.

Additionally, the Lock()/Unlock() functions don't behave exactly like a traditional mutex to the local process.  If another thread tries to call the already locked distributed mutex, dmuex will return an error rather than block.  This allows more control for the calling code (i.e. retry, do something else and try again later, etc)

In addition to memberlist, Dmutex uses the native Go rpc package

Example
===============
Dmutex is initialized with the local node's address, the address of the entire cluster, and a timeout for the RPC calls.

```
import (
  "github.com/bparli/dmutex"
)

func dLockTest() {
  nodes := []string{"192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"}
  dm := dmutex.NewDMutex("192.168.1.12", nodes, 3*time.Second)

  if err := d.Lock(); err != nil {
				log.Errorln("Lock error", err)
			} else {
				log.Debugln("Acquired Lock")
				time.Sleep(time.Duration(100) * time.Millisecond)
				if err := d.UnLock(); err != nil {
					log.Errorln("Unlock Error", err)
				} else {
					log.Debugln("Released Lock")
				}
			}

}
```

Since some applications may want to use Memberlist themselves, the memberlist.Memberlist type can be reused like so:

```
import (
  "github.com/bparli/dmutex"
)

func dLockTest() {
  nodes := []string{"192.168.1.12", "192.168.1.13", "192.168.1.14", "192.168.1.15", "192.168.1.16"}
  dm := dmutex.NewDMutex("192.168.1.12", nodes, 3*time.Second)

  exportedMemberlist := dm.Quorums.ExportMemberlist()
  myMemberList, ok := exportedMemberlist.(*quorums.MemList)
	if !ok {
		log.Fatalln("Oops")
	}

  // use myMemberList as you would with your own memberlist.Memberlist
```

TODO
===========
* Replace Go native rpc with grpc-go
* Decide whether using memberlist is worth the added complexity and logic
