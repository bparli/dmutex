package dmutex

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bparli/dmutex/bintree"
	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

var (
	dmutex    *Dmutex
	localAddr string
)

type Dmutex struct {
	Quorums   *quorums.Quorums
	rpcServer *server.DistSyncServer
	gateway   *sync.Mutex
	currPeers *peers
}

type peers struct {
	peersMap map[string]bool
	mutex    *sync.RWMutex
}

type lockError struct {
	node string
	err  error
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
	peersMap := make(map[string]bool)
	fmt.Println(qms.Peers)

	dmutex = &Dmutex{
		Quorums:   qms,
		rpcServer: &server.DistSyncServer{},
		gateway:   &sync.Mutex{},
		currPeers: &peers{
			peersMap: peersMap,
			mutex:    &sync.RWMutex{},
		},
	}

	dmutex.rpcServer, err = server.NewDistSyncServer(localAddr, len(nodes), timeout)
	if err != nil {
		log.Fatalln(err)
	}
	return dmutex
}

func sendRequest(peer string, ch chan<- *lockError, wg *sync.WaitGroup) {
	log.Debugln("Sending Request for lock to", peer)
	defer wg.Done()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", peer, server.RPCPort), opts...)
	if err != nil {
		log.Errorln("Error dialing peer:", err)
		ch <- &lockError{
			node: peer,
			err:  err,
		}
		return
	}
	defer conn.Close()
	client := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), server.Timeout)
	defer cancel()

	reply, err := client.Request(ctx, &pb.LockReq{Node: localAddr, Tstmp: ptypes.TimestampNow()})
	ch <- &lockError{
		node: peer,
		err:  err,
	}
	if err != nil {
		log.Errorln("Error requesting:", err)
	} else {
		log.Debugln("Acknowledgement from:", reply)
	}
}

func sendRelinquish(peer string, wg *sync.WaitGroup, ch chan<- *lockError) {
	log.Debugln("sending Relinquish lock to", peer)
	defer wg.Done()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", peer, server.RPCPort), opts...)
	if err != nil {
		log.Errorln("Error dialing peer:", err)
		ch <- &lockError{
			node: peer,
			err:  err,
		}
		return
	}
	defer conn.Close()
	client := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), server.Timeout)
	defer cancel()
	reply, err := client.Relinquish(ctx, &pb.Node{Node: localAddr})
	ch <- &lockError{
		node: peer,
		err:  err,
	}
	if err != nil {
		log.Errorln("Error relinquishing:", err)
	} else {
		log.Debugln("Reply from:", reply)
	}
}

func (d *Dmutex) Lock() error {
	// set the lock in case this Lock() gets called again
	d.gateway.Lock()

	// reset Progress
	d.rpcServer.SetProgress(server.ProgressNotAcquired)

	// reset peers mapping
	d.currPeers.peersMap = d.Quorums.Peers
	for k, v := range d.Quorums.Peers {
		d.currPeers.peersMap[k] = v
	}
	fmt.Println(d.currPeers.peersMap)
	fmt.Println(d.Quorums.Peers)

	err := d.sendRequests(d.currPeers.peersMap)

	if err != nil {
		log.Errorln("Error with lock:", err)
		d.UnLock()
		return err
	}

	if err = d.rpcServer.GatherReplies(d.currPeers.peersMap); err != nil {
		d.UnLock()
		return err
	}
	d.rpcServer.SetProgress(server.ProgressAcquired)
	return nil
}

func (d *Dmutex) UnLock() error {
	// unlock the gateway
	defer d.gateway.Unlock()
	d.rpcServer.SetProgress(server.ProgressNotAcquired)

	ch := make(chan *lockError, d.currPeers.numPeers())

	d.relinquish(ch)
	err := checkForError(ch, d.currPeers.getPeers())

	close(ch)
	return err
}

func (d *Dmutex) sendRequests(peers map[string]bool) error {
	ch := make(chan *lockError, len(peers))
	var wg sync.WaitGroup
	defer close(ch)
	for peer := range peers {
		wg.Add(1)
		go sendRequest(peer, ch, &wg)
	}
	wg.Wait()
	for _ = range peers {
		req := <-ch
		if req.err != nil {
			log.Errorf("Error sending request to node %s.  Trying substitutes.  Error: %s", req.node, req.err.Error())
			repPeers := d.Quorums.SubstitutePaths(req.node)
			// if failed node has no children or only 1 child, return error condition.
			if repPeers == nil || len(repPeers) == 1 {
				return fmt.Errorf("Error: Node %s has failed and not able to substitute", req.node)
			}

			repPeersMap := make(map[string]bool, len(repPeers))

			// update current peers to replace failed node with its substitution paths
			d.currPeers.mutex.Lock()
			delete(d.currPeers.peersMap, req.node)
			for _, n := range repPeers {
				d.currPeers.peersMap[n] = true
				repPeersMap[n] = true
			}
			d.currPeers.mutex.Unlock()

			// recurse with replacement path/peers
			if err := d.sendRequests(repPeersMap); err != nil {
				return err
			}
		}
	}

	// err := checkForError(ch, peers)
	return nil
}

func (d *Dmutex) recover() {
	d.rpcServer.PurgeNodeFromQueue(localAddr)
	d.rpcServer.TriggerQueueProcess()
}

func (d *Dmutex) relinquish(ch chan *lockError) {
	d.rpcServer.SetProgress(server.ProgressNotAcquired)
	var wg sync.WaitGroup
	for peer := range d.currPeers.getPeers() {
		wg.Add(1)
		go sendRelinquish(peer, &wg, ch)
	}
	wg.Wait()
}

func checkForError(ch chan *lockError, peers map[string]bool) error {
	for _ = range peers {
		reqErr := <-ch
		if reqErr.err != nil {
			return reqErr.err
		}
	}
	return nil
}

func (p *peers) getPeers() map[string]bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.peersMap
}

func (p *peers) numPeers() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.peersMap)
}
