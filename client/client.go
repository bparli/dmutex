package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/queue"
	"github.com/golang/protobuf/ptypes"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
)

// LockError captures the rpc error and associated peer Node
type LockError struct {
	Node string
	Err  error
}

type Config struct {
	RPCPort    string
	RPCTimeout time.Duration
	LocalAddr  string
}

func SendRequest(peer string, config *Config, ch chan<- *LockError, wg *sync.WaitGroup) {
	log.Debugln("Sending Request for lock to", peer)
	defer wg.Done()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", peer, config.RPCPort), opts...)
	if err != nil {
		log.Errorln("Error dialing peer:", err)
		ch <- &LockError{
			Node: peer,
			Err:  err,
		}
		return
	}
	defer conn.Close()
	cli := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), config.RPCTimeout)
	defer cancel()

	reply, err := cli.Request(ctx, &pb.LockReq{Node: config.LocalAddr, Tstmp: ptypes.TimestampNow()})
	ch <- &LockError{
		Node: peer,
		Err:  err,
	}
	if err != nil {
		log.Errorln("Error requesting:", err)
	} else {
		log.Debugln("Acknowledgement for Lock Request from:", reply)
	}
}

func SendRelinquish(peer string, config *Config, wg *sync.WaitGroup) {
	log.Debugln("sending Relinquish lock to", peer)
	defer wg.Done()
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", peer, config.RPCPort), opts...)
	if err != nil {
		log.Errorln("Error dialing peer:", err)
		return
	}
	defer conn.Close()
	cli := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), config.RPCTimeout)
	defer cancel()
	reply, err := cli.Relinquish(ctx, &pb.Node{Node: config.LocalAddr})
	if err != nil {
		log.Errorln("Error relinquishing:", err)
	} else {
		log.Debugln("Acknowledgement for Lock Relinquish from:", reply)
	}
}

func SendInquire(node string, config *Config) (*pb.InquireReply, error) {
	log.Debugln("Sending Inquire to", node)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", node, config.RPCPort), opts...)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), config.RPCTimeout)
	defer cancel()
	return cli.Inquire(ctx, &pb.Node{Node: config.LocalAddr})
}

func SendReply(reqArgs *queue.Mssg, config *Config) error {
	// send reply to head of queue
	log.Debugln("Sending Reply to", reqArgs)
	var opts []grpc.DialOption
	opts = append(opts, grpc.WithInsecure())
	conn, err := grpc.Dial(fmt.Sprintf("%s:%s", reqArgs.Node, config.RPCPort), opts...)
	if err != nil {
		return err
	}
	defer conn.Close()
	cli := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), config.RPCTimeout)
	defer cancel()
	_, err = cli.Reply(ctx, &pb.Node{Node: config.LocalAddr})
	if err != nil {
		return err
	}
	return nil
}
