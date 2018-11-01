/*
Package client is a package for the grpc calls to other nodes in the cluster.
*/
package client

import (
	"context"
	"fmt"
	"sync"
	"time"

	pb "github.com/bparli/dmutex/dsync"
	"github.com/bparli/dmutex/queue"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

// LockError captures the rpc error and associated peer Node.
type LockError struct {
	Node string
	Err  error
}

// Config is a type for keeping the necessary configurations when making grpc calls to other nodes in the cluster.
type Config struct {
	RPCPort    string
	RPCTimeout time.Duration
	LocalAddr  string
	TLSCRT     string
}

func setupConn(peer string, config *Config) (*grpc.ClientConn, error) {
	var opts []grpc.DialOption
	if config.TLSCRT != "" {
		creds, err := credentials.NewClientTLSFromFile(config.TLSCRT, "")
		if err != nil {
			log.Errorf("Error creating client TLS credentials: %s", err)
			return nil, err
		}
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithInsecure())
	}
	return grpc.Dial(fmt.Sprintf("%s:%s", peer, config.RPCPort), opts...)
}

// SendRequest is a grpc method for sending a lock request message to another node in the quorum(s).
func SendRequest(peer string, config *Config, ch chan<- *LockError, wg *sync.WaitGroup, lockReq *pb.LockReq) {
	log.Debugln("Sending Request for lock to", peer)
	defer wg.Done()
	conn, err := setupConn(peer, config)
	if err != nil {
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

	reply, err := cli.Request(ctx, lockReq)
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

// SendRelinquish is a grpc method for sending a lock release notification message to another node in the quorum(s).
func SendRelinquish(peer string, config *Config, wg *sync.WaitGroup) {
	log.Debugln("sending Relinquish lock to", peer)
	defer wg.Done()
	conn, err := setupConn(peer, config)
	if err != nil {
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

// SendInquire is a grpc method for sending an inquire message to another node in the quorum(s).
func SendInquire(peer string, config *Config) (*pb.InquireReply, error) {
	log.Debugln("Sending Inquire to", peer)
	conn, err := setupConn(peer, config)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), config.RPCTimeout)
	defer cancel()
	return cli.Inquire(ctx, &pb.Node{Node: config.LocalAddr})
}

// SendReply is a grpc method for sending a Reply message to another node in the quorum(s).
func SendReply(reqArgs *queue.Mssg, config *Config) error {
	// send reply to head of queue
	log.Debugln("Sending Reply to", reqArgs)
	conn, err := setupConn(reqArgs.Node, config)
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

// SendValidate is a grpc method for sending a Validate message to another node in the quorum(s).
func SendValidate(peer string, config *Config) (*pb.ValidateReply, error) {
	log.Debugln("Sending Validate to", peer)
	conn, err := setupConn(peer, config)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := pb.NewDistSyncClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), config.RPCTimeout)
	defer cancel()
	return cli.Validate(ctx, &pb.Node{Node: config.LocalAddr})
}
