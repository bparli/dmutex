package dmutex

import (
	"math"
	"sort"
	"sync"

	"github.com/bparli/dmutex/bintree"
	"github.com/hashicorp/memberlist"
	log "github.com/sirupsen/logrus"
)

type QuorumsState struct {
	myQuorums     map[int][]string // Ideal state of the local node's quorums when all nodes are healthy
	myCurrQuorums map[int][]string // Current state of the quorums.  Same as myQuorums when all nodes are healthy
	mutex         *sync.RWMutex
	fullTree      *bintree.Tree
	currTree      *bintree.Tree
	Mlist         *memberlist.Memberlist
	currMembers   map[string]bool
	currPeers     map[string]bool
	localAddr     string
	Ready         bool
	Healthy       bool
}

func newQuorums(t *bintree.Tree, nodes []string, localAddr string) *QuorumsState {
	treePaths := t.AllPaths()
	myQuorums := make(map[int][]string)
	myCurrQuorums := make(map[int][]string)
	currMembers := make(map[string]bool)
	currPeers := make(map[string]bool)
	for _, member := range nodes {
		currMembers[member] = false
	}

	for _, member := range nodes {
		currPeers[member] = false
	}

	count := 0
	for _, qm := range treePaths {
		member := false
		var quorum []string
		for _, n := range qm {
			quorum = append(quorum, n)
			if n == localAddr {
				member = true
			}
		}
		if member == true {
			sort.Sort(bintree.ByIp(quorum))
			myQuorums[count] = quorum
			count++
		}
	}
	return &QuorumsState{
		myQuorums:     myQuorums,
		myCurrQuorums: myCurrQuorums,
		mutex:         &sync.RWMutex{},
		fullTree:      t,
		currMembers:   currMembers, // Members are all nodes in the cluster.  Not all nodes will neccessarily communicate with each other
		currPeers:     currPeers,   // Peers are other nodes in common quorums with which this node should be exchaning messages
		localAddr:     localAddr,
		Ready:         false,
		Healthy:       false}
}

// build the tree, but only from healthy members in the cluster
func (q *QuorumsState) buildCurrTree() error {
	var ipAddrs []string
	for member, ok := range q.currMembers {
		if ok {
			ipAddrs = append(ipAddrs, member)
		}
	}
	currTree, err := bintree.NewTree(ipAddrs)
	if err != nil {
		return err
	}
	q.currTree = currTree
	return nil
}

// re-build set of quorum peers
// Only run from within existing mutex like buildCurrQuorums()
func (q *QuorumsState) buildCurrPeers() {
	q.currPeers = make(map[string]bool)
	for _, qm := range q.myCurrQuorums {
		for _, peer := range qm {
			q.currPeers[peer] = true
		}
	}
}

// build current state of the local node's quorums from the current state bin tree
func (q *QuorumsState) buildCurrQuorums() error {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.myCurrQuorums = make(map[int][]string)

	err := q.buildCurrTree()
	if err != nil {
		return err
	}
	treePaths := q.currTree.AllPaths()
	count := 0
	for _, qm := range treePaths {
		member := false
		var quorum []string
		for _, n := range qm {
			quorum = append(quorum, n)
			if n == q.localAddr {
				member = true
			}
		}
		if member == true {
			sort.Sort(bintree.ByIp(quorum))
			q.myCurrQuorums[count] = quorum
			count++
		}
	}

	q.buildCurrPeers()
	return nil
}

func (q *QuorumsState) checkHealth() {
	alive := 0
	for n := range q.currMembers {
		if q.currMembers[n] {
			alive++
		}
	}
	if float64(len(q.currMembers)-alive) > math.Log2(float64(q.fullTree.NumLeaves)) {
		q.Healthy = false
	} else {
		q.Healthy = true
	}
}

func (q *QuorumsState) validateMyQuorums(idealQuorums map[int][]string) bool {
	if q.Mlist != nil || q.Mlist.NumMembers() > 0 {
		for member := range q.currMembers {
			q.currMembers[member] = false
		}

		for _, member := range q.Mlist.Members() {
			q.currMembers[member.Addr.String()] = true
		}

		if err := q.buildCurrQuorums(); err != nil {
			log.Errorln("Error re-building current quorums", err.Error())
			return false
		}
	} else {
		return false
	}

	if len(q.myCurrQuorums) == len(idealQuorums) && len(q.myCurrQuorums) > 0 {
		for ind, curr := range q.myCurrQuorums {
			comp := idealQuorums[ind]
			if len(comp) != len(curr) {
				return false
			}
			for hop := range comp {
				if comp[hop] != curr[hop] {
					log.Debugf("Node %s not yet active", comp[hop])
					return false
				}
			}
		}
	} else {
		return false
	}
	return true
}
