/*
Package server is the grpc server for processing distributed lock requests.
It contains the grpc methods as well as the request queue (dependent on dmutex/bintree and dmutex/quorums)
*/
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
	"google.golang.org/grpc/credentials"
)

const (
	progressNotAcquired int = 0
	progressAcquired    int = 1
)

var (
	rpcSrv *DistSyncServer

	// Timeout for acquiring enough replies
	Timeout time.Duration
	// RPCPort is the port used by all nodes in the cluster for grpc messages
	RPCPort = "7070"
	// Peers is a mapping of nodes which are in the same quorum(s) as the local node.
	// Its used for tracking which peers have responded to a lock request
	Peers        *peersMap
	clientConfig *client.Config
)

// DistSyncServer manages grpc server and request queue
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
			go r.purgeNodeFromQueue(min.Node)

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
		r.purgeNodeFromQueue(req.Node)
		min := r.readMin()
		if min != nil && min.Replied {
			// Validate/health check the node holding the lock for 2 conditions:
			// 	- If the grpc returns an error consider the node failed.  Purge the node so rest of loop can process next in line
			// 	- If the Validate message returns false, we might have lost the relinquish message.  Purge the node
			if valid, err := client.SendValidate(min.Node, clientConfig); err != nil || !valid.GetHolding() {
				r.purgeNodeFromQueue(min.Node)
			}
		}

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
				r.purgeNodeFromQueue(min.Node)
			} else {
				if answer.Relinquish {
					r.purgeNodeFromQueue(min.Node)
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

// Reply is a grpc method representing the Reply message of the algorithm (granting consent to enter the critical section)
func (r *DistSyncServer) Reply(ctx context.Context, reply *pb.Node) (*pb.Node, error) {
	node := &pb.Node{Node: r.localAddr}
	log.Debugln("Incoming Reply from", reply.Node)
	r.repliesCh <- reply
	return node, nil
}

// GatherReplies waits for and tracks replies from each peer node.
// Its called when the local node has sent lock requests its quorum(s) and
// returns either when all nodes have replied or the timeout has passed.
// It takes an int argument representing how many of the quorum need to have replied
// (in the common case this will be all local quorum).
func (r *DistSyncServer) GatherReplies(quorumNum int) error {
	log.Debugln("Gathering Replies, waiting on ", Peers.replies)

	// Set the timeout clock
	startTime := time.Now()
	count := 0

outer:
	for {
		select {
		case reply := <-r.repliesCh:
			Peers.mutex.Lock()
			if result, ok := Peers.replies[reply.Node]; ok {
				// set node to true and increment count since we've received a new Reply
				if !result {
					count++
					Peers.replies[reply.Node] = true
					log.Debugf("Received Lock Reply from %s", reply.Node)
				}
				// when we've seen replies from all peers we have acquired the lock
				if count >= quorumNum {
					Peers.mutex.Unlock()
					break outer
				}
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

// Request is a grpc method representing the Request message of the algorithm accepting lock requests from other nodes in the cluster
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

// Inquire is a grpc method representing the Inquire message of the algorithm.
// It should either block on the request if the local node has acquired the lock
// or Yield if it has not yet acquired the lock
func (r *DistSyncServer) Inquire(ctx context.Context, inq *pb.Node) (*pb.InquireReply, error) {
	reply := &pb.InquireReply{
		Yield:      false,
		Relinquish: false,
	}
	if Peers.checkProgress() == progressAcquired {
		log.Debugln("Lock already acquired, block on Inquire", inq.Node)
		// block until the lock is ready to be released
		for {
			if Peers.checkProgress() != progressAcquired {
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
	return reply, nil
}

// Relinquish is a grpc method representing the Relinquish message of the algorithm (releasing the lock)
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
	go r.processQueue()
	// if we get here, the node was not found.  it was already removed via the Inquire -> Relinquish flow
	return node, nil
}

// Validate is a grpc method verifying the health of the node holding the lock.
// Returns true if the local node is still holding the lock
func (r *DistSyncServer) Validate(ctx context.Context, validate *pb.Node) (*pb.ValidateReply, error) {
	log.Debugln("Received Validate from node:", validate.Node)
	min := r.readMin()
	if min != nil && min.Node == r.localAddr {
		return &pb.ValidateReply{Holding: true}, nil
	}
	return &pb.ValidateReply{Holding: false}, nil
}

// NewDistSyncServer initializes the grpc server and mutex processing queue.
// It takes as arguments:
//  - the local node's IP address
//  - the number of members for creating message channels
//  - a timeout specifying grpc timeouts
//  - optional Certificate and Key files for encrypting connections between nodes
func NewDistSyncServer(addr string, numMembers int, timeout time.Duration, tlsCrtFile string, tlsKeyFile string) (*DistSyncServer, error) {
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
	if tlsCrtFile != "" && tlsKeyFile != "" {
		creds, err := credentials.NewServerTLSFromFile(tlsCrtFile, tlsKeyFile)
		if err != nil {
			log.Fatalf("Failed to generate credentials %v", err)
		}
		opts = []grpc.ServerOption{grpc.Creds(creds)}
	}
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

func (r *DistSyncServer) purgeNodeFromQueue(node string) {
	r.qMutex.Lock()
	defer r.qMutex.Unlock()
	for i := 0; i < r.reqQueue.Len(); i++ {
		if (*r.reqQueue)[i].Node == node {
			heap.Remove(r.reqQueue, i)
		}
	}
}
