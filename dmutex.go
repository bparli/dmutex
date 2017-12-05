package dmutex

import (
	"errors"
	"net/rpc"
	"strings"
	"sync"
	"time"

	"github.com/bparli/dmutex/bintree"
	"github.com/bparli/dmutex/queue"
	"github.com/bparli/dmutex/quorums"
	"github.com/bparli/dmutex/server"
	log "github.com/sirupsen/logrus"
)

var (
	dmutex   *Dmutex
	nodeAddr string
)

type Dmutex struct {
	Quorums      *quorums.QState
	rpcServer    *server.RPCServer
	gateway      *sync.Mutex
	localRequest *queue.Mssg
	set          bool
}

func NewDMutex(localAddr string, nodes []string, timeout time.Duration) *Dmutex {
	log.SetLevel(log.DebugLevel)

	var nodeIPs []string
	for _, node := range nodes {
		nodeIP := strings.Split(node, ":")[0]
		nodeIPs = append(nodeIPs, nodeIP)
	}
	localIP := strings.Split(localAddr, ":")[0]
	nodeAddr = localAddr

	t, err := bintree.NewTree(nodeIPs)
	if err != nil {
		log.Fatalln(err)
	}

	qms := quorums.NewQuorums(t, nodeIPs, localIP)

	dmutex = &Dmutex{
		Quorums:      qms,
		rpcServer:    &server.RPCServer{},
		localRequest: &queue.Mssg{},
		gateway:      &sync.Mutex{},
		set:          false,
	}

	dmutex.Quorums.Mlist, err = InitMembersList(localAddr, nodes)
	if err != nil {
		log.Errorln("Error initializing memberlist", err.Error())
	}

	dmutex.rpcServer, err = server.NewRPCServer(localAddr, len(nodes), timeout)
	if err != nil {
		log.Fatalln(err)
	}

	for {
		log.Debugln("Quorums not ready for node:", localAddr, "Current Quorums:", qms.MyCurrQuorums, "Ideal Quorums", qms.MyQuorums)
		if !dmutex.Quorums.ValidateMyQuorums() || dmutex.Quorums.Mlist.NumMembers() != len(nodes) {
			time.Sleep(1 * time.Second)
		} else {
			dmutex.Quorums.Ready = true
			dmutex.Quorums.Healthy = true
			break
		}
	}

	dmutex.rpcServer.SetReady(true)

	log.Debugln("Quorums Validated, testing distributed mutex", qms.MyCurrQuorums)

	if err := dmutex.Lock(); err != nil {
		log.Fatalln("Unable to Acquire test mutex", err)
	}

	if err := dmutex.UnLock(); err != nil {
		log.Fatalln("Unable to Relinquish test mutex", err)
	}

	return dmutex
}

func sendRequest(args *queue.Mssg, peer string, wg *sync.WaitGroup, ch chan<- error) {
	defer wg.Done()
	log.Debugln("sending Request for lock to", peer, args)
	_, err := sendToPeer("Dsync.Request", args, peer)
	ch <- err
}

func sendRelinquish(args *queue.Mssg, peer string, wg *sync.WaitGroup, ch chan<- error) {
	defer wg.Done()
	log.Debugln("sending Relinquish lock to", peer, args)
	_, err := sendToPeer("Dsync.Relinquish", args, peer)
	ch <- err
}

func sendToPeer(method string, args *queue.Mssg, peer string) (int, error) {
	var reply int
	client, err := rpc.DialHTTP("tcp", peer+":"+server.RPCPort)
	if err != nil {
		return 0, err
	}
	defer client.Close()

	c := make(chan error, 1)
	go func() { c <- client.Call(method, args, &reply) }()
	select {
	case err := <-c:
		return reply, err
	case <-time.After(server.Timeout):
		return 0, errors.New(method + "rpc timed out to node" + peer)
	}
}

func (d *Dmutex) Lock() error {
	if !d.Quorums.Healthy || !d.Quorums.Ready {
		return errors.New("Unable to obtain lock, quorums are not healthy")
	} else if d.set {
		return errors.New("Unable to obtain lock, already set")
	} else {
		// set the lock in case this Lock() gets called again
		d.gateway.Lock()
		defer d.gateway.Unlock()
		d.set = true

		ch := make(chan error, len(d.Quorums.CurrMembers))
		var wg sync.WaitGroup
		args := &queue.Mssg{
			Timestamp: time.Now(),
			Node:      nodeAddr,
			Replied:   false,
		}

		d.localRequest = args

		d.rpcServer.SetProgress(server.ProgressNotAcquired)

		for peer := range d.Quorums.CurrPeers {
			if d.Quorums.CurrPeers[peer] {
				wg.Add(1)
				go sendRequest(args, peer, &wg, ch)
			}
		}
		wg.Wait()

		err := d.checkForError(ch)
		close(ch)
		if err != nil {
			d.recover()
			return err
		}

		if err = d.rpcServer.GatherReplies(args, d.Quorums.CurrPeers, len(d.Quorums.CurrMembers)); err != nil {
			d.recover()
		}
		return err
	}
}

func (d *Dmutex) UnLock() error {
	if !d.set {
		return errors.New("Dmutex Unlock Error: no outstanding lock detected")
	}
	// set the lock in case this Unlock() gets called again
	d.gateway.Lock()
	defer d.gateway.Unlock()

	ch := make(chan error, len(d.Quorums.CurrMembers))

	args := d.localRequest

	d.relinquish(args, ch)
	err := d.checkForError(ch)

	close(ch)
	d.localRequest = &queue.Mssg{}
	d.set = false
	return err
}

func (d *Dmutex) recover() {
	server.PurgeNodeFromQueue(nodeAddr)
	d.rpcServer.SanitizeQueue()
	d.rpcServer.TriggerQueueProcess()
	d.localRequest = &queue.Mssg{}
	d.set = false
}

func (d *Dmutex) relinquish(args *queue.Mssg, ch chan error) {
	var wg sync.WaitGroup
	for peer := range d.Quorums.CurrPeers {
		if d.Quorums.CurrPeers[peer] {
			wg.Add(1)
			go sendRelinquish(args, peer, &wg, ch)
		}
	}
	wg.Wait()
	d.rpcServer.SetProgress(server.ProgressNotAcquired)
}

func (d *Dmutex) checkForError(ch chan error) error {
	for peer := range d.Quorums.CurrPeers {
		if d.Quorums.CurrPeers[peer] {
			err := <-ch
			if err != nil {
				return err
			}
		}
	}
	return nil
}
