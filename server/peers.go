package server

import (
	"sync"

	log "github.com/sirupsen/logrus"
)

// peersMap tracks which of the current peers have replied to a lock request
// also maintains an up to date view of current peers based on failed nodes
type peersMap struct {
	replies map[string]bool
	mutex   *sync.RWMutex
}

func (p *peersMap) checkProgress() int {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	for _, replied := range p.replies {
		if !replied {
			return ProgressNotAcquired
		}
	}
	return ProgressAcquired
}

func (p *peersMap) ResetProgress(currPeers map[string]bool) {
	p.mutex.Lock()
	defer p.mutex.Unlock()
	p.replies = make(map[string]bool)
	// Init each peer to false since we haven't received a Reply yet
	for peer := range currPeers {
		p.replies[peer] = false
	}
}

func (p *peersMap) SubstitutePeer(peer string, replace map[string]bool) {
	log.Infof("Substituting node %s with %s", peer, replace)
	p.mutex.Lock()
	defer p.mutex.Unlock()
	delete(p.replies, peer)
	for n := range replace {
		p.replies[n] = false
	}
}

func (p *peersMap) GetPeers() map[string]bool {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return p.replies
}

func (p *peersMap) NumPeers() int {
	p.mutex.RLock()
	defer p.mutex.RUnlock()
	return len(p.replies)
}
