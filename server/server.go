package server

import (
	"container/heap"
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/queue"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

const (
	ProgressNotAcquired int = 0
	ProgressAcquired    int = 1
	ProgressYielded     int = 2
)

var (
	rpcSrv *DistSyncServer

	// Timeout for acquiring enough replies
	Timeout time.Duration
	RPCPort = "7070"
	replies *peerReplies
)

type peerReplies struct {
	currPeers map[string]bool
	mutex     *sync.Mutex
}

// DistSyncServer manages RPC server and request queue
type DistSyncServer struct {
	localAddr   string
	reqQueue    *queue.ReqHeap
	qMutex      *sync.RWMutex
	progMutex   *sync.RWMutex
	ReqProgress int
	reqsCh      chan *pb.LockReq
	repliesCh   chan *pb.Node
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
		if err := r.sendReply(min); err != nil {
			log.Errorln("Error sending reply to node", min.Node, err)
		} else {
			min.Replied = true
		}
	}
}

func (r *DistSyncServer) serveRequests() {
	for {
		req := <-r.reqsCh
		min := r.readMin()

		// there should only be one outstanding request in queue from a given node
		r.PurgeNodeFromQueue(req.Node)

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
			err := r.sendInquire(min.Node)
			if err != nil {
				log.Errorln("Error sending Inquire", err)
			}
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

func (r *DistSyncServer) GatherReplies(peers map[string]bool) error {
	log.Debugln("Gathering Replies, waiting on ", peers)
	replies = &peerReplies{
		mutex:     &sync.Mutex{},
		currPeers: peers,
	}
	// Set each peer to false since we haven't received a Reply yet
	for p := range peers {
		peers[p] = false
	}

	// Set the timeout clock
	startTime := time.Now()

outer:
	for {
		select {
		case reply := <-r.repliesCh:
			if _, ok := replies.currPeers[reply.Node]; ok {
				replies.mutex.Lock()
				// set node to true since we've received a Reply
				replies.currPeers[reply.Node] = true
				// Check the rest of the nodes.  if we don't have all the Replies, continue
				for _, node := range replies.currPeers {
					if !node {
						replies.mutex.Unlock()
						continue outer
					}
				}
				replies.mutex.Unlock()
				break outer
			}
		default:
			if startTime.Add(time.Duration(len(peers)) * Timeout).Before(time.Now()) {
				return errors.New("Gathering replies timed out")
			}
		}
	}
	return nil
}

func (r *DistSyncServer) sendReply(reqArgs *queue.Mssg) error {
	// send reply to head of queue
	log.Debugln("Sending Reply to", reqArgs)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", reqArgs.Node, RPCPort), opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	_, err = client.Reply(ctx, &pb.Node{Node: r.localAddr})
	if err != nil {
		return err
	}
	return nil
}

func (r *DistSyncServer) Request(ctx context.Context, req *pb.LockReq) (*pb.Node, error) {
	if timeStamp, err := ptypes.Timestamp(req.Tstmp); err != nil {
		return nil, errors.New("Error converting request timestamp")
	} else if timeStamp.Add(Timeout).Before(time.Now()) {
		return nil, errors.New("Request has already timed out")
	}

	log.Debugln("Incoming Request from", req.Node)
	r.reqsCh <- req
	log.Debugln("Sent req over channel")
	return &pb.Node{Node: r.localAddr}, nil
}

func (r *DistSyncServer) sendInquire(node string) error {
	log.Debugln("Sending Inquire to", node)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", node, RPCPort), opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	client := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), Timeout)
	defer cancel()
	answer, err := client.Inquire(ctx, &pb.Node{Node: r.localAddr})
	if err != nil {
		return err
	}
	if answer.Relinquish {
		r.PurgeNodeFromQueue(node)
	}
	return nil
}

func (r *DistSyncServer) Inquire(ctx context.Context, inq *pb.Node) (*pb.InquireReply, error) {
	reply := &pb.InquireReply{
		Yield:      false,
		Relinquish: false,
	}
	if r.checkProgress() == ProgressAcquired {
		log.Debugln("Lock already acquired, block on Inquire")
		// block until the lock is ready to be released
		for {
			if rpcSrv.checkProgress() != ProgressAcquired {
				reply.Relinquish = true
				break
			}
		}
	} else {
		log.Debugln("Progress Not Acquired, Yield")
		replies.mutex.Lock()
		replies.currPeers[inq.Node] = false
		replies.mutex.Unlock()
		reply.Yield = true
	}
	return reply, nil
}

func (r *DistSyncServer) checkProgress() int {
	r.progMutex.RLock()
	defer r.progMutex.RUnlock()
	return r.ReqProgress
}

func (r *DistSyncServer) SetProgress(setting int) {
	r.progMutex.Lock()
	defer r.progMutex.Unlock()
	r.ReqProgress = setting
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

	reqsCh := make(chan *pb.LockReq, numMembers)
	repliesCh := make(chan *pb.Node, numMembers)

	// This timeout is the timeout for all RPC calls, some of which are intentially blocking
	// if another process is holding the distributed mutex
	Timeout = timeout

	rpcSrv = &DistSyncServer{
		reqQueue:  reqQueue,
		qMutex:    &sync.RWMutex{},
		progMutex: &sync.RWMutex{},
		localAddr: addr,
		reqsCh:    reqsCh,
		repliesCh: repliesCh,
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
