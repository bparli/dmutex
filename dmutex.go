package dmutex

import (
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/client"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"
	log "github.com/sirupsen/logrus"
)

var (
	dmutex       *Dmutex
	localAddr    string
	clientConfig *client.Config
)

type Dmutex struct {
	Quorums   *quorums.Quorums
	rpcServer *server.DistSyncServer
	gateway   *sync.Mutex
}

func NewDMutex(nodeAddr string, nodes []string, timeout time.Duration) *Dmutex {
	log.SetLevel(log.DebugLevel)

	var nodeIPs []string
	for _, node := range nodes {
		nodeIP := strings.Split(node, ":")[0]
		nodeIPs = append(nodeIPs, nodeIP)
	}
	localIP := strings.Split(nodeAddr, ":")[0]
	localAddr = nodeAddr

	t, err := bintree.NewTree(nodeIPs)
	if err != nil {
		log.Fatalln(err)
	}

	qms := quorums.NewQuorums(t, nodeIPs, localIP)
	log.Debugln("Using Quorums: ", qms.MyQuorums)
	log.Debugln("Using Peers: ", qms.Peers)

	dmutex = &Dmutex{
		Quorums:   qms,
		rpcServer: &server.DistSyncServer{},
		gateway:   &sync.Mutex{},
	}

	dmutex.rpcServer, err = server.NewDistSyncServer(localAddr, len(nodes), timeout)
	if err != nil {
		log.Fatalln(err)
	}

	clientConfig = &client.Config{
		LocalAddr:  localAddr,
		RPCPort:    server.RPCPort,
		RPCTimeout: timeout,
	}

	return dmutex
}

func (d *Dmutex) Lock() error {
	// set the lock in case this Lock() gets called again
	d.gateway.Lock()

	// reset Progress
	server.Peers.ResetProgress(d.Quorums.Peers)
	err := d.sendRequests(server.Peers.GetPeers())

	if err != nil {
		log.Errorln("Error with lock:", err)
		d.UnLock()
		return err
	}

	if err = d.rpcServer.GatherReplies(); err != nil {
		d.UnLock()
		return err
	}
	return nil
}

func (d *Dmutex) UnLock() {
	// unlock the gateway
	defer d.gateway.Unlock()
	server.Peers.ResetProgress(d.Quorums.Peers)

	d.relinquish()
}

func (d *Dmutex) sendRequests(peers map[string]bool) error {
	ch := make(chan *client.LockError, len(peers))
	var wg sync.WaitGroup
	defer close(ch)
	for peer := range peers {
		wg.Add(1)
		go client.SendRequest(peer, clientConfig, ch, &wg)
	}
	wg.Wait()
	for _ = range peers {
		req := <-ch
		if req.Err != nil {
			log.Errorf("Error sending request to node %s.  Trying substitutes.  Error: %s", req.Node, req.Err.Error())
			repPeers := d.Quorums.SubstitutePaths(req.Node)
			// if failed node has no children or only 1 child, return error condition.
			if repPeers == nil || len(repPeers) == 1 {
				return fmt.Errorf("Error: Node %s has failed and not able to substitute", req.Node)
			}

			repPeersMap := make(map[string]bool, len(repPeers))

			// update current peers to replace failed node with its substitution paths
			for _, n := range repPeers {
				repPeersMap[n] = false
			}
			server.Peers.SubstitutePeer(req.Node, repPeers)

			// recurse with replacement path/peers
			if err := d.sendRequests(repPeersMap); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *Dmutex) recover() {
	d.rpcServer.PurgeNodeFromQueue(localAddr)
	d.rpcServer.TriggerQueueProcess()
}

func (d *Dmutex) relinquish() {
	var wg sync.WaitGroup
	for peer := range server.Peers.GetPeers() {
		wg.Add(1)
		go client.SendRelinquish(peer, clientConfig, &wg)
	}
	wg.Wait()
}

// func checkForError(ch chan *client.LockError, peers map[string]bool) error {
// 	for _ = range peers {
// 		reqErr := <-ch
// 		if reqErr.Err != nil {
// 			return reqErr.Err
// 		}
// 	}
// 	return nil
// }
