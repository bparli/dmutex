/*
Package quorums is a package for initializing and maintaining the distribted mutex's quorums from the local node's perspective.
It creates the quorums based on a binary tree of IP addresses (dependent on the dmutex/bintree package).
The package also exposes a function to calculate and return substitute paths for a failed node in the quorum.
*/
package quorums

import (
	"sort"

	"github.com/bparli/dmutex/bintree"
)

// Quorums type builds and maintains the cluster quorums using the bintree package to maintain the tree.
type Quorums struct {
	MyQuorums map[int][]string
	Peers     map[string]bool
	NumPeers  int
	fullTree  *bintree.Tree
	localAddr string
}

// NewQuorums initializes the quorums from the local node's perspective based on a bintree of IP addresses and the local address.
func NewQuorums(t *bintree.Tree, localAddr string) *Quorums {
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
			sort.Sort(bintree.ByIP(quorum))
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

// SubstitutePaths returns possible paths starting from the site’s two children and ending in leaf node.
// Used in the distributed mutex algorithm when a node in the quorum has failed.
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
