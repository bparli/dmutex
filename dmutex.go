package dmutex

import (
	"fmt"
	"strings"
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
	lockReq := &pb.LockReq{Node: clientConfig.LocalAddr, Tstmp: ptypes.TimestampNow()}
	err := d.sendRequests(server.Peers.GetPeers(), lockReq)
	if err != nil {
		log.Errorln("Error with lock:", err)
		d.rpcServer.DrainRepliesCh()
		d.UnLock()
		return err
	}
	if err = d.rpcServer.GatherReplies(); err != nil {
		d.rpcServer.DrainRepliesCh()
		d.UnLock()
		return err
	}
	return nil
}

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

func (d *Dmutex) relinquish() {
	var wg sync.WaitGroup
	peers := server.Peers.GetPeers()
	for peer := range peers {
		wg.Add(1)
		go client.SendRelinquish(peer, clientConfig, &wg)
	}
	wg.Wait()
}
