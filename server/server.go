package server

import (
	"container/heap"
	"errors"
	"net"
	"net/http"
	"net/rpc"
	"reflect"
	"sync"
	"time"

	"github.com/bparli/dmutex/queue"
	log "github.com/sirupsen/logrus"
)

const (
	YieldMssg           int = 1
	RelinqMssg          int = 2
	ProgressNotAcquired int = 0
	ProgressAcquired    int = 1
	ProgressYielded     int = 2
)

var (
	rpcSrv       *RPCServer
	totalMembers int
	Timeout      time.Duration
	RPCPort      = "7070"
)

// Dsync manages RPC server and request queue
type Dsync struct {
	localAddr   string
	reqQueue    *queue.ReqHeap
	qMutex      *sync.RWMutex
	progMutex   *sync.RWMutex
	ReqProgress int
	reqsCh      chan *queue.Mssg
	repliesCh   chan *queue.Mssg
	Ready       bool
}

type RPCServer struct {
	dsync *Dsync
	srv   *rpc.Server
	lis   net.Listener
}

func (d *Dsync) processQueue() {
	var min *queue.Mssg
	d.qMutex.Lock()
	defer d.qMutex.Unlock()

	if d.reqQueue.Len() > 0 {
		min = (*d.reqQueue)[0]
	} else {
		return
	}
	if !min.Replied {
		if err := d.sendReply(min); err != nil {
			log.Errorln("Error sending reply to node", min.Node, err)
		} else {
			min.Replied = true
		}
	}
}

func (d *Dsync) serveRequests() {
	for {
		req := <-d.reqsCh
		min := d.readMin()

		// there should only be one outstanding request in queue from a given node
		PurgeNodeFromQueue(req.Node)

		if min != nil && min.Timestamp.After(req.Timestamp) {
			_, err := d.sendInquire(min.Node)
			if err != nil {
				log.Errorln("Error sending Inquire", err)
			}
		}
		rpcSrv.SanitizeQueue()
		d.push(req)
		d.processQueue()
	}
}

func (d *Dsync) Reply(args *queue.Mssg, reply *int) error {
	log.Debugln("Incoming Reply from", args)
	d.repliesCh <- args
	return nil
}

func (r *RPCServer) GatherReplies(reqArgs *queue.Mssg, peers map[string]bool) error {
	track := make(map[string]bool)
	log.Debugln("Gathering Replies, waiting on ", peers)

	count := 0
	for {
		reply := <-r.dsync.repliesCh
		if reply.Timestamp.Equal(reqArgs.Timestamp) {
			track[reply.Node] = true
			count++
		}
		if count >= len(peers) {
			break
		}
	}

	// should have all replies by now, double check
	if !reflect.DeepEqual(track, peers) {
		log.Debugln(track, peers)
		r.SetProgress(ProgressNotAcquired)
		return errors.New("Error gathering replies from all quorums peers")
	} else {
		r.SetProgress(ProgressAcquired)
		return nil
	}
}

func (d *Dsync) sendReply(reqArgs *queue.Mssg) error {
	// send reply to head of queue
	log.Debugln("Sending Reply to", reqArgs)
	var reply int
	args := &queue.Mssg{
		Timestamp: reqArgs.Timestamp,
		Node:      d.localAddr,
		Replied:   false,
	}
	client, err := rpc.DialHTTP("tcp", reqArgs.Node+":"+RPCPort)
	if err != nil {
		return err
	}
	defer client.Close()

	c := make(chan error, 1)
	go func() { c <- client.Call("Dsync.Reply", args, &reply) }()
	select {
	case err := <-c:
		return err
	case <-time.After(Timeout):
		return errors.New("Reply rpc timed out to node" + reqArgs.Node)
	}
}

func (d *Dsync) Request(args *queue.Mssg, reply *int) error {
	for {
		if !d.Ready {
			// be sure not to cpu starve memberlist threads
			time.Sleep(time.Duration(100*totalMembers) * time.Millisecond)
			continue
		} else {
			break
		}
	}
	if args.Timestamp.Add(Timeout).Before(time.Now()) {
		return errors.New("Request has already timed out")
	}

	log.Debugln("Incoming Request from", args)
	d.reqsCh <- args
	return nil
}

func (d *Dsync) sendInquire(node string) (int, error) {
	log.Debugln("Sending Inquire to", node)
	var reply int
	client, err := rpc.DialHTTP("tcp", node+":"+RPCPort)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	args := &queue.Mssg{
		Node: d.localAddr,
	}

	c := make(chan error, 1)
	go func() { c <- client.Call("Dsync.Inquire", args, &reply) }()
	select {
	case err := <-c:
		return reply, err
	case <-time.After(Timeout):
		return 0, errors.New("Inquire rpc timed out to node" + node)
	}
}

func (d *Dsync) Inquire(args *queue.Mssg, reply *int) error {
	if rpcSrv.checkProgress() == ProgressAcquired {
		log.Debugln("Lock already acquired, block on Inquire")
		// block until the lock is ready to be released
		for {
			if rpcSrv.checkProgress() != ProgressAcquired {
				*reply = RelinqMssg
				break
			}
		}
	} else {
		log.Debugln("Progress Not Acquired, Yield")
		*reply = YieldMssg
	}
	return nil
}

func (r *RPCServer) checkProgress() int {
	r.dsync.progMutex.RLock()
	defer r.dsync.progMutex.RUnlock()
	return r.dsync.ReqProgress
}

func (r *RPCServer) SetProgress(setting int) {
	r.dsync.progMutex.Lock()
	defer r.dsync.progMutex.Unlock()
	r.dsync.ReqProgress = setting
}

func (r *RPCServer) SetReady(ready bool) {
	r.dsync.Ready = ready
}

func (d *Dsync) Relinquish(args *queue.Mssg, reply *int) error {
	log.Debugln("Received relinquish", args)
	item := d.readMin()
	if item == nil {
		log.Errorln("Error with Relinquish.  Minimum item on queue is nil")
		return nil // timeout may have occured while relinquish was in flight.  still want to log it though
	}
	// search the rest of the queue for the right relinquish
	// messages might have arrived out of order
	if item.Node != args.Node {
		d.qMutex.RLock()
		qlen := d.reqQueue.Len()
		if qlen >= 2 {
			for i := 1; i < qlen; i++ {
				if (*d.reqQueue)[i].Node == args.Node {
					d.qMutex.RUnlock()
					d.remove(i)
					d.processQueue()
					return nil
				}
			}
			d.qMutex.RUnlock()
			return errors.New("Error with Relinquish.  Minimum item from node not found")
		}
	}
	d.pop()
	d.processQueue()
	return nil
}

func NewRPCServer(addr string, numMembers int, timeout time.Duration) (*RPCServer, error) {
	reqQueue := &queue.ReqHeap{}
	heap.Init(reqQueue)

	srv := rpc.NewServer()

	// set channel buffers to be very high for recovery scenarios
	reqsCh := make(chan *queue.Mssg, numMembers)
	repliesCh := make(chan *queue.Mssg, numMembers)
	totalMembers = numMembers

	// This timeout is the timeout for all RPC calls, some of which are intentially blocking
	// if another process is holding the distributed mutex
	Timeout = timeout

	dsync := &Dsync{
		reqQueue:  reqQueue,
		qMutex:    &sync.RWMutex{},
		progMutex: &sync.RWMutex{},
		localAddr: addr,
		reqsCh:    reqsCh,
		repliesCh: repliesCh,
		Ready:     false,
	}

	srv.Register(dsync)
	srv.HandleHTTP("/", "/debug")

	lis, err := net.Listen("tcp", addr+":"+RPCPort)
	if err != nil {
		return nil, errors.New("Unable to initialize RPC server: " + err.Error())
	}

	go http.Serve(lis, nil)
	go dsync.serveRequests()

	rpcSrv = &RPCServer{
		dsync: dsync,
		srv:   srv,
		lis:   lis,
	}
	return rpcSrv, nil
}

func (d *Dsync) pop() {
	d.remove(0)
}

func (d *Dsync) remove(index int) {
	d.qMutex.Lock()
	defer d.qMutex.Unlock()
	if d.reqQueue.Len() >= index+1 {
		heap.Remove(d.reqQueue, index)
	}
}

func (d *Dsync) push(args *queue.Mssg) {
	d.qMutex.Lock()
	defer d.qMutex.Unlock()
	heap.Push(d.reqQueue, args)
}

func (d *Dsync) readMin() *queue.Mssg {
	d.qMutex.RLock()
	defer d.qMutex.RUnlock()
	if d.reqQueue.Len() > 0 {
		msg := (*d.reqQueue)[0]
		return msg
	}
	return nil
}

func PurgeNodeFromQueue(node string) {
	rpcSrv.dsync.qMutex.Lock()
	defer rpcSrv.dsync.qMutex.Unlock()
	purge(node)
}

func purge(node string) {
	for i := 0; i < rpcSrv.dsync.reqQueue.Len(); i++ {
		if (*rpcSrv.dsync.reqQueue)[i].Node == node {
			heap.Remove(rpcSrv.dsync.reqQueue, i)
		}
	}
}

func (r *RPCServer) SanitizeQueue() {
	r.dsync.qMutex.Lock()
	defer r.dsync.qMutex.Unlock()

	for i := 0; i < r.dsync.reqQueue.Len(); i++ {
		check := (*r.dsync.reqQueue)[i]
		if check.Timestamp.Add(Timeout).Before(time.Now()) && !check.Replied {
			heap.Remove(r.dsync.reqQueue, i)
			log.Errorln("Removed timeout from queue at time", time.Now(), check)
		}
	}
}

func (r *RPCServer) TriggerQueueProcess() {
	r.dsync.processQueue()
}
