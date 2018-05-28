package dmutex

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/bparli/dmutex/bintree"
	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/queue"
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
}

func NewDMutex(nodeAddr string, nodes []string, timeout time.Duration) *Dmutex {
	log.SetLevel(log.DebugLevel)

	var nodeIPs []string
	for _, node := range nodes {
		nodeIP := strings.Split(node, ":")[0]
		nodeIPs = append(nodeIPs, nodeIP)
	}
	localIP := strings.Split(localAddr, ":")[0]
	localAddr = nodeAddr

	t, err := bintree.NewTree(nodeIPs)
	if err != nil {
		log.Fatalln(err)
	}

	qms := quorums.NewQuorums(t, nodeIPs, localIP)

	dmutex = &Dmutex{
		Quorums:   qms,
		rpcServer: &server.DistSyncServer{},
		gateway:   &sync.Mutex{},
	}

	dmutex.rpcServer, err = server.NewDistSyncServer(localAddr, len(nodes), timeout)
	if err != nil {
		log.Fatalln(err)
	}

	return dmutex
}

func sendRequest(peer string, wg *sync.WaitGroup, ch chan<- error) {
	log.Debugln("sending Request for lock to", peer)
	defer wg.Done()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", peer, server.RPCPort), opts...)
	if err != nil {
		log.Errorln("Error dialing peer:", err)
		ch <- err
		return
	}
	defer conn.Close()
	client := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), server.Timeout)
	defer cancel()

	reply, err := client.Request(ctx, &pb.LockReq{Node: localAddr, Tstmp: ptypes.TimestampNow()})
	ch <- err
	if err != nil {
		log.Errorln("Error requesting:", err)
	} else {
		log.Debugln("Acknowledgement from:", reply)
	}
}

func sendRelinquish(peer string, wg *sync.WaitGroup, ch chan<- error) {
	log.Debugln("sending Relinquish lock to", peer)
	defer wg.Done()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", peer, server.RPCPort), opts...)
	if err != nil {
		log.Errorln("Error dialing peer:", err)
		ch <- err
		return
	}
	defer conn.Close()
	client := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), server.Timeout)
	defer cancel()
	reply, err := client.Relinquish(ctx, &pb.Node{Node: localAddr})
	ch <- err
	if err != nil {
		log.Errorln("Error relinquishing:", err)
	} else {
		log.Debugln("Reply from:", reply)
	}
}

func (d *Dmutex) Lock() error {
	// set the lock in case this Lock() gets called again
	d.gateway.Lock()

	ch := make(chan error, d.Quorums.NumPeers)
	var wg sync.WaitGroup
	args := &queue.Mssg{
		Node:    localAddr,
		Replied: false,
	}

	d.rpcServer.SetProgress(server.ProgressNotAcquired)

	for peer := range d.Quorums.Peers {
		wg.Add(1)
		go sendRequest(peer, &wg, ch)
	}
	wg.Wait()

	err := d.checkForError(ch)
	close(ch)
	if err != nil {
		log.Errorln("Error with lock:", err)
		defer d.gateway.Unlock()
		d.recover()
		return err
	}

	if err = d.rpcServer.GatherReplies(args, d.Quorums.Peers); err != nil {
		defer d.gateway.Unlock()
		d.recover()
	}
	return err
}

func (d *Dmutex) UnLock() error {
	// unlock the gateway
	defer d.gateway.Unlock()

	ch := make(chan error, d.Quorums.NumPeers)

	d.relinquish(ch)
	err := d.checkForError(ch)

	close(ch)
	return err
}

func (d *Dmutex) recover() {
	server.PurgeNodeFromQueue(localAddr)
	d.rpcServer.SanitizeQueue()
	d.rpcServer.TriggerQueueProcess()
}

func (d *Dmutex) relinquish(ch chan error) {
	var wg sync.WaitGroup
	for peer := range d.Quorums.Peers {
		wg.Add(1)
		go sendRelinquish(peer, &wg, ch)
	}
	wg.Wait()
	d.rpcServer.SetProgress(server.ProgressNotAcquired)
}

func (d *Dmutex) checkForError(ch chan error) error {
	for _ = range d.Quorums.Peers {
		err := <-ch
		if err != nil {
			return err
		}
	}
	return nil
}
