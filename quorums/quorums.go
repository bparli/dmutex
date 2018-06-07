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

// SubstitutePaths returns two possible paths starting
// from the site’s two children and ending in leaf node
func (q *Quorums) SubstitutePeers(node string) []string {
	var final []string
	var childI string
	var childII string

	// since each node's path starts with itself as root, check if
	// the node is the only entry in the path
	if allPaths := q.fullTree.NodePaths(node); len(allPaths[0]) > 1 {
		for _, path := range allPaths {
			if childI != path[1] && childII != path[1] {
				if childI == "" {
					childI = path[1]
					for _, node := range path[1:] {
						final = append(final, node)
					}
				} else if childII == "" {
					childII = path[1]
					for _, node := range path[1:] {
						final = append(final, node)
					}
				}
			}
		}
	}
	return final
}
