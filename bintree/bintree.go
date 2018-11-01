/*
Package bintree creates a binary tree of Ip Addresses for use in the distributed mutex algorithm.
*/
package bintree

import (
	"bytes"
	"fmt"
	"net"
	"sort"

	log "github.com/sirupsen/logrus"
)

// Tree is used to store the root of the binary tree.
// Each node, including the root has pointers to its descendants so the rest of the tree is reached from this point.
type Tree struct {
	root      *node
	NumLeaves int
}

type node struct {
	val   string
	left  *node
	right *node
	Paths [][]string
}

// ByIP is a special type used for building the binary tree, specifically the initial sorting.
// Len, Swap, and Less functions are used in sorting the Ipaddrs prior to building the tree.
type ByIP []string

// Len returns the length of the binary tree and is used for building the binary tree.
func (a ByIP) Len() int { return len(a) }

// Swap is used for building the binary tree.
func (a ByIP) Swap(i, j int) { a[i], a[j] = a[j], a[i] }

// Less is used for comparing nodes when building the binary tree.
func (a ByIP) Less(i, j int) bool {
	if res := compare(a[i], a[j]); res == -1 {
		return true
	}
	return false
}

// NewTree initializes the binary tree.  It takes an array of strings representing Ip addresses.
// It will verify the entries in the array are Ip addresses, and then builds the tree and tree paths, returning the Tree type.
func NewTree(ipAddrs []string) (*Tree, error) {
	// first check all are valid IPv4 addresses
	for _, ipAddr := range ipAddrs {
		ip := net.ParseIP(ipAddr)
		if ip.To4() == nil {
			return nil, fmt.Errorf("Ip Address %s is not an IPv4", ipAddr)
		}
	}

	// sort list so we can build a balanced tree
	sort.Sort(ByIP(ipAddrs))

	newT := &Tree{NumLeaves: 0}

	// build the tree utilizing the already sorted list
	newT.root = newT.buildTree(ipAddrs, 0, len(ipAddrs)-1)

	newT.getNodePaths(newT.root, newT.root, []string{})

	return newT, nil
}

// AllPaths returns all possible paths in the tree starting from the root and ending at a leaf.
func (t *Tree) AllPaths() [][]string {
	return t.root.Paths
}

// NodePaths returns all possible paths in the tree starting from the node at ipAddr and ending at a leaf.
func (t *Tree) NodePaths(ipAddr string) [][]string {
	ch := make(chan *node, 1)
	defer close(ch)
	go t.findNode(ipAddr, t.root, ch)
	n := <-ch
	if n != nil {
		if len(n.Paths) == 0 {
			t.getNodePaths(n, n, []string{})
		}
		return n.Paths
	}
	log.Errorf("Unable to find node for IP %s", ipAddr)
	return nil
}

func (t *Tree) findNode(ipAddr string, n *node, ch chan<- *node) {
	if n.val == ipAddr {
		ch <- n
		return
	}
	if n.left != nil {
		t.findNode(ipAddr, n.left, ch)
	}
	if n.right != nil {
		t.findNode(ipAddr, n.right, ch)
	}
	// if we didn't find the node
	if n.val == t.root.val {
		ch <- nil
	}
}

func (t *Tree) getNodePaths(r *node, n *node, path []string) {
	if n.left == nil && n.right == nil {
		finalPath := append(path, n.val)
		r.Paths = append(r.Paths, finalPath)
		return
	}
	if n.right != nil {
		rightPath := append(path, n.val)
		t.getNodePaths(r, n.right, rightPath)
	}
	if n.left != nil {
		leftPath := append(path, n.val)
		t.getNodePaths(r, n.left, leftPath)
	}
}

func (t *Tree) buildTree(ipAddrs []string, start int, end int) *node {
	if start > end {
		return nil
	}
	mid := (start + end) / 2
	newBT := &node{val: ipAddrs[mid]}
	newBT.left = t.buildTree(ipAddrs, start, mid-1)
	newBT.right = t.buildTree(ipAddrs, mid+1, end)
	if newBT.left == nil && newBT.right == nil {
		t.NumLeaves++
	}
	return newBT
}

func compare(ip1 string, ip2 string) int {
	return bytes.Compare(net.ParseIP(ip1), net.ParseIP(ip2))
}
