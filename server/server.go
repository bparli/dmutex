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
	rpcSrv       *DistSyncServer
	totalMembers int
	Timeout      time.Duration
	RPCPort      = "7070"
	ready        *Ready
)

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

type Ready struct {
	Flag  bool
	Mutex *sync.RWMutex
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
		PurgeNodeFromQueue(req.Node)

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
		rpcSrv.SanitizeQueue()
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

func (r *DistSyncServer) GatherReplies(reqArgs *queue.Mssg, peers map[string]bool, numMembers int) error {
	log.Debugln("Gathering Replies, waiting on ", peers, reqArgs)

	startTime := time.Now()
	count := 0

outer:
	for {
		select {
		case <-r.repliesCh:
			res, ok := peers[reqArgs.Node]
			if ok && res {
				count++
			}
			if count >= len(peers) {
				break outer
			}
		default:
			if startTime.Add(time.Duration(numMembers) * Timeout).Before(time.Now()) {
				return errors.New("Gathering replies timed out")
			}
			// be sure not to cpu starve other threads
			time.Sleep(100 * time.Millisecond)
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
	for {
		if !rpcSrv.CheckReady() {
			// be sure not to cpu starve memberlist threads
			time.Sleep(time.Duration(100*totalMembers) * time.Millisecond)
			continue
		} else {
			break
		}
	}

	if timeStamp, err := ptypes.Timestamp(req.Tstmp); err != nil {
		return nil, errors.New("Error converting request timestamp")
	} else if timeStamp.Add(Timeout).Before(time.Now()) {
		return nil, errors.New("Request has already timed out")
	}

	log.Debugln("Incoming Request from", req.Node)
	r.reqsCh <- req
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
	_, err = client.Inquire(ctx, &pb.Node{Node: r.localAddr})
	if err != nil {
		return err
	}
	return nil
}

func (r *DistSyncServer) Inquire(ctx context.Context, req *pb.Node) (*pb.InquireReply, error) {
	reply := &pb.InquireReply{
		Yield:      false,
		Relinquish: false,
	}
	if rpcSrv.checkProgress() == ProgressAcquired {
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

func (r *DistSyncServer) CheckReady() bool {
	ready.Mutex.RLock()
	defer ready.Mutex.RUnlock()
	return ready.Flag
}

func (r *DistSyncServer) SetReady(setting bool) {
	ready.Mutex.Lock()
	defer ready.Mutex.Unlock()
	ready.Flag = setting
}

func (r *DistSyncServer) Relinquish(ctx context.Context, relinquish *pb.Node) (*pb.Node, error) {
	node := &pb.Node{Node: r.localAddr}
	log.Debugln("Received relinquish", relinquish.Node)
	item := r.readMin()
	if item == nil {
		log.Errorln("Error with Relinquish.  Minimum item on queue is nil")
		return node, nil // timeout may have occured while relinquish was in flight.  still want to log it though
	}
	// search the rest of the queue for the right relinquish
	// messages might have arrived out of order
	if item.Node != relinquish.Node {
		r.qMutex.RLock()
		qlen := r.reqQueue.Len()
		if qlen >= 2 {
			for i := 1; i < qlen; i++ {
				if (*r.reqQueue)[i].Node == relinquish.Node {
					r.qMutex.RUnlock()
					r.remove(i)
					r.processQueue()
					return node, nil
				}
			}
			r.qMutex.RUnlock()
			return nil, errors.New("Error with Relinquish.  Minimum item from node not found")
		}
	}
	r.pop()
	r.processQueue()
	return node, nil
}

func NewDistSyncServer(addr string, numMembers int, timeout time.Duration) (*DistSyncServer, error) {
	reqQueue := &queue.ReqHeap{}
	heap.Init(reqQueue)

	// set channel buffers to be very high for recovery scenarios
	reqsCh := make(chan *pb.LockReq, numMembers)
	repliesCh := make(chan *pb.Node, numMembers)
	totalMembers = numMembers

	// This timeout is the timeout for all RPC calls, some of which are intentially blocking
	// if another process is holding the distributed mutex
	Timeout = timeout

	ready = &Ready{
		Mutex: &sync.RWMutex{},
		Flag:  false,
	}

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

func (d *DistSyncServer) pop() {
	d.remove(0)
}

func (d *DistSyncServer) remove(index int) {
	d.qMutex.Lock()
	defer d.qMutex.Unlock()
	if d.reqQueue.Len() >= index+1 {
		heap.Remove(d.reqQueue, index)
	}
}

func (d *DistSyncServer) push(args *queue.Mssg) {
	d.qMutex.Lock()
	defer d.qMutex.Unlock()
	heap.Push(d.reqQueue, args)
}

func (d *DistSyncServer) readMin() *queue.Mssg {
	d.qMutex.RLock()
	defer d.qMutex.RUnlock()
	if d.reqQueue.Len() > 0 {
		msg := (*d.reqQueue)[0]
		return msg
	}
	return nil
}

func PurgeNodeFromQueue(node string) {
	rpcSrv.qMutex.Lock()
	defer rpcSrv.qMutex.Unlock()
	purge(node)
}

func purge(node string) {
	for i := 0; i < rpcSrv.reqQueue.Len(); i++ {
		if (*rpcSrv.reqQueue)[i].Node == node {
			heap.Remove(rpcSrv.reqQueue, i)
		}
	}
}

func (r *DistSyncServer) SanitizeQueue() {
	r.qMutex.Lock()
	defer r.qMutex.Unlock()

	for i := 0; i < r.reqQueue.Len(); i++ {
		check := (*r.reqQueue)[i]
		if check.Timestamp.Add(2 * Timeout).Before(time.Now()) {
			heap.Remove(r.reqQueue, i)
			log.Errorln("Removed timeout from queue at time", time.Now(), check)
		}
	}
}

func (r *DistSyncServer) TriggerQueueProcess() {
	r.processQueue()
}
