package server

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/bparli/dmutex/client"
	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/queue"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	ProgressNotAcquired int = 0
	ProgressAcquired    int = 1
)

var (
	rpcSrv *DistSyncServer

	// Timeout for acquiring enough replies
	Timeout      time.Duration
	RPCPort      = "7070"
	Peers        *peersMap
	clientConfig *client.Config
)

// PeersMap tracks which of the current peers have replied to a lock request
// also maintains an up to date view of current peers based on failed nodes
type peersMap struct {
	replies map[string]bool
	mutex   *sync.RWMutex
}

// DistSyncServer manages RPC server and request queue
type DistSyncServer struct {
	localAddr string
	reqQueue  *queue.ReqHeap
	qMutex    *sync.RWMutex
	reqsCh    chan *pb.LockReq
	repliesCh chan *pb.Node
}

func (r *DistSyncServer) processQueue() {
	var min *queue.Mssg
	r.qMutex.Lock()
	defer r.qMutex.Unlock()

	if r.reqQueue.Len() > 0 {
		min = (*r.reqQueue)[0]
	} else {
		return
	}

	if !min.Replied {
		if err := client.SendReply(min, clientConfig); err != nil {
			log.Errorln("Error sending reply to node.  Removing from queue", min.Node, err)
			go r.PurgeNodeFromQueue(min.Node)
			go r.processQueue()
		} else {
			log.Debugf("Sent Reply to node %s for lock", min.Node)
			min.Replied = true
		}
	}
}

func (r *DistSyncServer) serveRequests() {
	for {
		req := <-r.reqsCh

		// there should only be one outstanding request in queue from a given node
		r.PurgeNodeFromQueue(req.Node)
		min := r.readMin()

		timeStamp, err := ptypes.Timestamp(req.Tstmp)
		if err != nil {
			log.Errorln("Error converting request timestamp.  Skipping request " + req.Node)
			continue
		}
		queueMessage := &queue.Mssg{
			Timestamp: timeStamp,
			Node:      req.Node,
			Replied:   false,
		}

		if min != nil && min.Timestamp.After(timeStamp) {
			log.Infoln("Sending Inquire to", min)
			answer, err := client.SendInquire(min.Node, clientConfig)
			if err != nil {
				log.Errorln("Error sending Inquire", err)
				r.PurgeNodeFromQueue(min.Node)
			} else {
				if answer.Relinquish {
					r.PurgeNodeFromQueue(min.Node)
				} else if answer.Yield {
					r.undoReplies()
				}
			}
			log.Infoln("Inquire Result was:", answer)
		}
		r.push(queueMessage)
		r.processQueue()
	}
}

func (r *DistSyncServer) Reply(ctx context.Context, reply *pb.Node) (*pb.Node, error) {
	node := &pb.Node{Node: r.localAddr}
	log.Debugln("Incoming Reply from", reply.Node)
	r.repliesCh <- reply
	return node, nil
}

func (r *DistSyncServer) GatherReplies() error {
	log.Debugln("Gathering Replies, waiting on ", Peers.replies)

	// Set the timeout clock
	startTime := time.Now()

outer:
	for {
		select {
		case reply := <-r.repliesCh:
			Peers.mutex.Lock()
			if _, ok := Peers.replies[reply.Node]; ok {
				// set node to true since we've received a Reply
				// when all are true we have acquired the lock
				Peers.replies[reply.Node] = true
				log.Debugf("Received Lock Reply from %s", reply.Node)

				// Check the rest of the nodes.  if we don't have all the Replies, continue
				for _, node := range Peers.replies {
					if !node {
						Peers.mutex.Unlock()
						continue outer
					}
				}
				Peers.mutex.Unlock()
				break outer
			}
			Peers.mutex.Unlock()
		default:
			if startTime.Add(time.Duration(Peers.NumPeers()) * Timeout).Before(time.Now()) {
				return fmt.Errorf("Gathering replies timed out. Peers Replies state is:%v", Peers.replies)
			}
		}
	}
	return nil
}

// DrainRepliesCh removes outstanding replies from the replies channel
// only to be used in error condition
func (r *DistSyncServer) DrainRepliesCh() {
	for {
		select {
		case <-r.repliesCh:
			continue
		default:
			return
		}
	}
}

func (r *DistSyncServer) Request(ctx context.Context, req *pb.LockReq) (*pb.Node, error) {
	if timeStamp, err := ptypes.Timestamp(req.Tstmp); err != nil {
		return nil, errors.New("Error converting request timestamp")
	} else if timeStamp.Add(Timeout).Before(time.Now()) {
		return nil, errors.New("Request has already timed out")
	}

	log.Debugln("Incoming Request from", req.Node)
	r.reqsCh <- req
	return &pb.Node{Node: r.localAddr}, nil
}

func (r *DistSyncServer) Inquire(ctx context.Context, inq *pb.Node) (*pb.InquireReply, error) {
	reply := &pb.InquireReply{
		Yield:      false,
		Relinquish: false,
	}
	if Peers.checkProgress() == ProgressAcquired {
		log.Debugln("Lock already acquired, block on Inquire", inq.Node)
		// block until the lock is ready to be released
		for {
			if Peers.checkProgress() != ProgressAcquired {
				reply.Relinquish = true
				break
			}
		}
	} else {
		log.Debugln("Progress Not Acquired, Yield to", inq.Node)
		Peers.mutex.Lock()
		if _, ok := Peers.replies[inq.Node]; ok {
			Peers.replies[inq.Node] = false
		}
		Peers.mutex.Unlock()
		reply.Yield = true
	}
	go r.processQueue()
	return reply, nil
}

func (r *DistSyncServer) Relinquish(ctx context.Context, relinquish *pb.Node) (*pb.Node, error) {
	node := &pb.Node{Node: r.localAddr}
	log.Debugln("Received relinquish from node:", relinquish.Node)
	r.qMutex.RLock()
	qlen := r.reqQueue.Len()

	// search entire queue since Relinquish may be called from nodes giving up their request
	// before acquiring the Lock
	for i := 0; i < qlen; i++ {
		if (*r.reqQueue)[i].Node == relinquish.Node {
			r.qMutex.RUnlock()
			r.remove(i)
			go r.processQueue()
			return node, nil
		}
	}
	r.qMutex.RUnlock()

	// if we get here, the node was not found.  it was already removed via the Inquire -> Relinquish flow
	return node, nil
}

func NewDistSyncServer(addr string, numMembers int, timeout time.Duration) (*DistSyncServer, error) {
	reqQueue := &queue.ReqHeap{}
	heap.Init(reqQueue)

	reqsCh := make(chan *pb.LockReq, 100*numMembers)
	repliesCh := make(chan *pb.Node, 100*numMembers)

	// This timeout is the timeout for all RPC calls, some of which are intentially blocking
	// if another process is holding the distributed mutex
	Timeout = timeout

	rpcSrv = &DistSyncServer{
		reqQueue:  reqQueue,
		qMutex:    &sync.RWMutex{},
		localAddr: addr,
		reqsCh:    reqsCh,
		repliesCh: repliesCh,
	}

	clientConfig = &client.Config{
		RPCPort:    RPCPort,
		RPCTimeout: Timeout,
		LocalAddr:  addr,
	}

	Peers = &peersMap{
		mutex: &sync.RWMutex{},
	}

	lis, err := net.Listen("tcp", addr+":"+RPCPort)
	if err != nil {
		return nil, errors.New("Unable to initialize RPC server: " + err.Error())
	}

	var opts []grpc.ServerOption
	DistSyncServer := grpc.NewServer(opts...)
	pb.RegisterDistSyncServer(DistSyncServer, rpcSrv)
	go DistSyncServer.Serve(lis)
	go rpcSrv.serveRequests()

	return rpcSrv, nil
}

func (r *DistSyncServer) pop() {
	r.remove(0)
}

func (r *DistSyncServer) remove(index int) {
	r.qMutex.Lock()
	defer r.qMutex.Unlock()
	if r.reqQueue.Len() >= index+1 {
		heap.Remove(r.reqQueue, index)
	}
}

func (r *DistSyncServer) undoReplies() {
	r.qMutex.Lock()
	defer r.qMutex.Unlock()
	for i := 0; i < r.reqQueue.Len(); i++ {
		(*r.reqQueue)[i].Replied = false
	}
}

func (r *DistSyncServer) push(args *queue.Mssg) {
	r.qMutex.Lock()
	defer r.qMutex.Unlock()
	heap.Push(r.reqQueue, args)
}

func (r *DistSyncServer) readMin() *queue.Mssg {
	r.qMutex.RLock()
	defer r.qMutex.RUnlock()
	if r.reqQueue.Len() > 0 {
		msg := (*r.reqQueue)[0]
		return msg
	}
	return nil
}

func (r *DistSyncServer) PurgeNodeFromQueue(node string) {
	r.qMutex.Lock()
	defer r.qMutex.Unlock()
	for i := 0; i < r.reqQueue.Len(); i++ {
		if (*r.reqQueue)[i].Node == node {
			heap.Remove(r.reqQueue, i)
		}
	}
}

func (r *DistSyncServer) TriggerQueueProcess() {
	r.processQueue()
}

func (p *peersMap) checkProgress() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, replied := range p.replies {
		if !replied {
			return ProgressNotAcquired
		}
	}
	return ProgressAcquired
}

func (p *peersMap) ResetProgress(currPeers map[string]bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.replies = make(map[string]bool)
	// Init each peer to false since we haven't received a Reply yet
	for peer := range currPeers {
		p.replies[peer] = false
	}
}

func (p *peersMap) SubstitutePeer(peer string, replace []string) {
	log.Infof("Substituting node %s with %s", peer, replace)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.replies, peer)
	for _, n := range replace {
		p.replies[n] = false
	}
}

func (p *peersMap) GetPeers() map[string]bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.replies
}

func (p *peersMap) NumPeers() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.replies)
}
