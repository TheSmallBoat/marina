package marina

import (
	"sync"

	"github.com/lithdew/kademlia"
)

type twinsPool struct {
	mu sync.RWMutex
	sp sync.Pool

	// One remote service provider paired with one twin which own the same KadID.
	mpt map[kademlia.PublicKey]*twin
	mpp map[kademlia.PublicKey]*twinServiceProvider
}

func newTwinsPool() *twinsPool {
	return &twinsPool{
		mu:  sync.RWMutex{},
		sp:  sync.Pool{},
		mpt: make(map[kademlia.PublicKey]*twin),
		mpp: make(map[kademlia.PublicKey]*twinServiceProvider),
	}
}

func (tp *twinsPool) length() (int, int) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	return len(tp.mpt), len(tp.mpp)
}

// return the number of the append providers
func (tp *twinsPool) appendProviders(providers ...*twinServiceProvider) int {
	var pNum = 0
	for i := range providers {
		kadId := (*providers[i]).KadID()
		_, pExist := tp.existServiceProvider(kadId)
		if !pExist {
			tp.mu.Lock()
			tp.mpp[kadId.Pub] = providers[i]
			tp.mu.Unlock()
			pNum++
		}
		_ = tp.acquire(kadId)
		// todo: twin run go routine task
	}
	return pNum
}

//return the number of offline-twins, the number of missed-twins
func (tp *twinsPool) checkTwinsProvidersPairStatus() (int, int) {
	var pNum, offlineTwinNum = 0, 0
	for k, tw := range tp.mpt {
		tp.mu.RLock()
		_, exist := tp.mpp[k]
		tp.mu.RUnlock()

		if !exist {
			// todo: some of them maybe release
			if tw.onlineStatus() {
				tw.turnToOffline()
			}
			offlineTwinNum++
		} else {
			pNum++
		}
	}

	missedTwinNum := len(tp.mpp) - pNum
	if missedTwinNum > 0 {
		//  means some of providers haven't the pair twins.
		for _, pd := range tp.mpp {
			_ = tp.acquire((*pd).KadID())
		}
	}

	return offlineTwinNum, missedTwinNum
}

func (tp *twinsPool) pairStatus(peerNodeId *kademlia.ID) bool {
	_, pExist := tp.existServiceProvider(peerNodeId)
	tw, tExist := tp.existTwin(peerNodeId)
	if pExist && tExist {
		if !tw.onlineStatus() {
			tw.turnToOnline()
		}
		return true
	} else {
		return false
	}
}

func (tp *twinsPool) existServiceProvider(peerNodeId *kademlia.ID) (*twinServiceProvider, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	p, exist := tp.mpp[peerNodeId.Pub]
	return p, exist
}

func (tp *twinsPool) existTwin(peerNodeId *kademlia.ID) (*twin, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	tw, exist := tp.mpt[peerNodeId.Pub]
	return tw, exist
}

func (tp *twinsPool) acquire(peerNodeId *kademlia.ID) *twin {
	tw, exist := tp.existTwin(peerNodeId)
	if exist {
		if !tw.onlineStatus() {
			tw.turnToOnline()
		}
		return tw
	}

	v := tp.sp.Get()
	if v == nil {
		v = newTwin(peerNodeId)
	} else {
		v.(*twin).initWithOnline(peerNodeId)
	}
	tw = v.(*twin)

	tp.mu.Lock()
	tp.mpt[tw.kadId.Pub] = tw
	tp.mu.Unlock()

	return tw
}

func (tp *twinsPool) release(t *twin) {
	tp.mu.RLock()
	_, exist := tp.mpt[t.kadId.Pub]
	tp.mu.RUnlock()

	if exist {
		tp.mu.Lock()
		delete(tp.mpt, t.kadId.Pub)
		tp.mu.Unlock()
	}

	t.reset()
	tp.sp.Put(t)
}
