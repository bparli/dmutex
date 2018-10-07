/*
Package dmutex is a distributed mutex package written in Go.
It takes a quorum based approach to managing shared locks across n distributed nodes.
Dmutex is initialized with the local node's address(it can be either the IP address or even the hostname), the addresses of the entire cluster, and a timeout for the gRPC calls.
For simplicity it uses port 7070 for all nodes in the cluster.
Optional file locations of a TLS certificate and key can be passed in order to secure cluster traffic.

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
*/
package dmutex

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/client"
	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
)

var (
	dmutex       *Dmutex
	localAddr    string
	clientConfig *client.Config
)

// Dmutex is the main struct encompassing everything the local node needs to request and release the distributed shared lock
type Dmutex struct {
	Quorums   *quorums.Quorums
	rpcServer *server.DistSyncServer
	gateway   *sync.Mutex
}

// NewDMutex is the public function for initializing the distributed lock from the local node's perspective.
// It takes as arguments:
//  - the local node's address (in either hotname or IP address form)
//  - the entire cluster's individual addresses, again in either hostname or IP address form
//  - a timeout specifying grpc timeouts
//  - optional Certificate and Key files for encrypting connections between nodes
// It calculates the tree, quorums, initializes grpc client and server and returns the initialized distributed mutex
func NewDMutex(nodeAddr string, nodeAddrs []string, timeout time.Duration, tlsCrtFile string, tlsKeyFile string) *Dmutex {
	log.SetLevel(log.DebugLevel)

	var nodes []string
	for _, node := range nodeAddrs {
		ipAddr, err := validateAddr(node)
		if err != nil {
			log.Errorf("Not adding node to cluster %s", err.Error())
		} else {
			nodes = append(nodes, ipAddr)
		}
	}

	var err error
	localAddr, err = validateAddr(nodeAddr)
	if err != nil {
		log.Fatalf("Exiting.  Unable to add local node to cluster %s", err.Error())
	}
	if len(nodes) < 1 {
		log.Fatalf("Exiting. Not enough nodes for a cluster.  Nodes: %s", nodes)
	}

	t, err := bintree.NewTree(nodes)
	if err != nil {
		log.Fatalln(err)
	}

	qms := quorums.NewQuorums(t, localAddr)
	log.Debugln("Using Quorums: ", qms.MyQuorums)
	log.Debugln("Using Peers: ", qms.Peers)

	dmutex = &Dmutex{
		Quorums:   qms,
		rpcServer: &server.DistSyncServer{},
		gateway:   &sync.Mutex{},
	}

	dmutex.rpcServer, err = server.NewDistSyncServer(localAddr, len(nodes), timeout, tlsCrtFile, tlsKeyFile)
	if err != nil {
		log.Fatalln(err)
	}

	clientConfig = &client.Config{
		LocalAddr:  localAddr,
		RPCPort:    server.RPCPort,
		RPCTimeout: timeout,
		TLSCRT:     tlsCrtFile,
	}

	return dmutex
}

// Lock is a public function to request the distributed mutex from the rest of the cluster
// Like a local lock, it blocks until it has locked the distributed mutex
func (d *Dmutex) Lock() error {
	// set the lock in case this Lock() gets called again
	d.gateway.Lock()

	// reset Progress
	server.Peers.ResetProgress(d.Quorums.Peers)
	lockReq := &pb.LockReq{Node: clientConfig.LocalAddr, Tstmp: ptypes.TimestampNow()}
	err := d.sendRequests(server.Peers.GetPeers(), lockReq)
	if err != nil {
		log.Errorln("Error with lock:", err)
		d.rpcServer.DrainRepliesCh()
		d.UnLock()
		return err
	}

	// wait for replies from others in the quorum(s)
	if err = d.rpcServer.GatherReplies(); err != nil {
		d.rpcServer.DrainRepliesCh()
		d.UnLock()
		return err
	}
	return nil
}

// UnLock is a public function to release the lock on the distributed mutex
// It notifies the rest of the quorum of the release and cleans up
func (d *Dmutex) UnLock() {
	// unlock the gateway
	defer d.gateway.Unlock()
	d.relinquish()
	server.Peers.ResetProgress(d.Quorums.Peers)
}

func (d *Dmutex) sendRequests(peers map[string]bool, lockReq *pb.LockReq) error {
	ch := make(chan *client.LockError, len(peers))
	var wg sync.WaitGroup
	defer close(ch)
	for peer := range peers {
		wg.Add(1)
		go client.SendRequest(peer, clientConfig, ch, &wg, lockReq)
	}
	wg.Wait()
	for _ = range peers {
		req := <-ch
		if req.Err != nil {
			log.Errorf("Error sending request to node %s.  Trying substitutes.  Error: %s", req.Node, req.Err.Error())
			subPaths := d.Quorums.SubstitutePaths(req.Node)

			for ind, path := range subPaths {
				// check each node in the path except the root
				// since that is the node to be replaced in this case
				for _, node := range path[1:] {
					// can't use this as a replacement since this node is already
					// in that quorum so remove path
					if node == localAddr {
						if ind < len(subPaths)-1 {
							subPaths = append(subPaths[:ind], subPaths[ind+1:]...)
						} else {
							subPaths = subPaths[:ind]
						}
					}
				}
			}

			// if failed node has no children or only 1 child, return error condition.
			if len(subPaths) < 2 {
				return fmt.Errorf("Error: Node %s has failed and not able to substitute", req.Node)
			}

			// Otherwise, generate the replacements for the failed nodes.  This will be the new quorum for this lock request
			repPeersMap := genReplacementMap(peers, subPaths)
			server.Peers.SubstitutePeer(req.Node, repPeersMap)
			if len(repPeersMap) > 0 {
				log.Infof("Using substitute paths for failed node %s:", req.Node, subPaths)
				// recurse with replacement path/peers
				return d.sendRequests(repPeersMap, lockReq)
				// return fmt.Errorf("Error: Node %s has failed and not able to substitute", req.Node)
			}
		}
	}
	return nil
}

// genReplacementMap is called in the case of a filed node.
// It creates a new map with the replacements for the failed node
func genReplacementMap(peers map[string]bool, replacementPaths [][]string) map[string]bool {
	var newPeers []string
	for _, p := range replacementPaths {
		for _, node := range p {
			// only add the replacement peer if its not already a peer node from another path
			if _, ok := peers[node]; !ok {
				newPeers = append(newPeers, node)
			}
		}
	}

	repPeersMap := make(map[string]bool, len(newPeers))

	// update current peers to replace failed node with its substitution paths
	for _, n := range newPeers {
		repPeersMap[n] = false
	}
	return repPeersMap
}

// relinquish notifies the quorum(s) the lock has been released
func (d *Dmutex) relinquish() {
	var wg sync.WaitGroup
	peers := server.Peers.GetPeers()
	for peer := range peers {
		wg.Add(1)
		go client.SendRelinquish(peer, clientConfig, &wg)
	}
	wg.Wait()
}

// validateAddr checks the given argument to verify it is an Ip Address.
// If it is not, it tries to lookup the string to convert it into one
func validateAddr(addr string) (string, error) {
	if net.ParseIP(addr).To4() == nil {
		// try converting from string to Ip Addr
		ipAddrs, err := net.LookupHost(addr)
		if err != nil || len(ipAddrs) < 1 {
			return "", fmt.Errorf("Unable to use or resolve node %s", addr)
		}
		return ipAddrs[0], nil
	}
	return addr, nil
}
