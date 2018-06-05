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
			replacementPeers := d.Quorums.SubstitutePaths(req.Node)
			// if failed node has no children or only 1 child, return error condition.
			if replacementPeers == nil || len(replacementPeers) == 1 {
				return fmt.Errorf("Error: Node %s has failed and not able to substitute", req.Node)
			}

			repPeersMap := genReplacementMap(peers, replacementPeers)
			if len(repPeersMap) == 0 {
				return fmt.Errorf("Error: Node %s has failed and not able to substitute", req.Node)
			}

			server.Peers.SubstitutePeer(req.Node, repPeersMap)

			// recurse with replacement path/peers
			return d.sendRequests(repPeersMap, lockReq)
		}
	}
	return nil
}

func genReplacementMap(peers map[string]bool, replacementPeers []string) map[string]bool {
	var newPeers []string
	for _, p := range replacementPeers {
		if _, ok := peers[p]; !ok {
			newPeers = append(newPeers, p)
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
