package dmutex

import (
	"github.com/bparli/dmutex/server"
	"github.com/hashicorp/memberlist"
	log "github.com/sirupsen/logrus"
)

type quorumEvents struct{}

func newEventDelegate() *quorumEvents {
	return &quorumEvents{}
}

func (q *quorumEvents) NotifyJoin(node *memberlist.Node) {
	log.Debugf("NotifyJoin(): %s", node.Name)
	joinHandler(node)
}

func (q *quorumEvents) NotifyLeave(node *memberlist.Node) {
	log.Debugf("NotifyLeave(): %s", node.Name)
	leaveHandler(node)
}

func (q *quorumEvents) NotifyUpdate(node *memberlist.Node) {
	log.Debugf("NotifyUpdate(): %s", node.Name)
}

func joinHandler(node *memberlist.Node) {
	if dmutex.Quorums.Ready == true {
		dmutex.gateway.Lock()
		defer dmutex.gateway.Unlock()

		recover(node, true)

		log.Infoln("Node", node.Addr.String(), "joined.  Current quorums are now ", dmutex.Quorums.myCurrQuorums, "Health is", dmutex.Quorums.Healthy)
	}
}

func leaveHandler(node *memberlist.Node) {
	if dmutex.Quorums.Ready == true {
		dmutex.gateway.Lock()
		defer dmutex.gateway.Unlock()

		recover(node, false)

		log.Infoln("Node", node.Addr.String(), "left.  Current quorums are now ", dmutex.Quorums.myCurrQuorums, "Health is", dmutex.Quorums.Healthy)
	}
}

func recover(node *memberlist.Node, joined bool) {
	dmutex.rpcServer.SetReady(false)

	dmutex.Quorums.currMembers[node.Addr.String()] = joined
	if err := dmutex.Quorums.buildCurrQuorums(); err != nil {
		log.Errorln("Error re-building current quorums", err.Error())
	}

	server.PurgeNodeFromQueue(node.Addr.String())

	dmutex.Quorums.checkHealth()
	dmutex.rpcServer.SanitizeQueue()
	dmutex.rpcServer.SetReady(true)
}
