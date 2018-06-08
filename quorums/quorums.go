package quorums

import (
	"sort"

	"github.com/bparli/dmutex/bintree"
)

type Quorums struct {
	MyQuorums map[int][]string
	Peers     map[string]bool
	NumPeers  int
	fullTree  *bintree.Tree
	localAddr string
}

func NewQuorums(t *bintree.Tree, nodes []string, localAddr string) *Quorums {
	treePaths := t.AllPaths()
	myQuorums := make(map[int][]string)
	peers := make(map[string]bool)

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

	for _, qm := range myQuorums {
		for _, peer := range qm {
			peers[peer] = true
		}
	}
	return &Quorums{
		MyQuorums: myQuorums,
		Peers:     peers,
		NumPeers:  len(peers),
		fullTree:  t,
		localAddr: localAddr,
	}
}

// SubstitutePaths returns possible paths starting
// from the site’s two children and ending in leaf node
func (q *Quorums) SubstitutePaths(node string) [][]string {
	var paths [][]string
	for _, treePath := range q.fullTree.NodePaths(node) {
		var path []string
		for _, node := range treePath {
			path = append(path, node)
		}
		paths = append(paths, path)
	}
	return paths
}
