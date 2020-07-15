package marina

import (
	"github.com/lithdew/kademlia"
	"sync"
)

const defaultMaxTwinWorkers = 32

type twinsPool struct {
	mu sync.RWMutex
	sp sync.Pool

	ttp *taskPool
	// One remote service provider paired with one twin which own the same KadID.
	mpt map[kademlia.PublicKey]*twin
	mpp map[kademlia.PublicKey]*twinServiceProvider
}

func newTwinsPool() *twinsPool {
	return &twinsPool{
		mu:  sync.RWMutex{},
		sp:  sync.Pool{},
		ttp: NewTaskPool(defaultMaxTwinWorkers),
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
		pubK := (*providers[i]).KadID().Pub
		_, pExist := tp.existServiceProvider(pubK)
		if !pExist {
			tp.mu.Lock()
			tp.mpp[pubK] = providers[i]
			tp.mu.Unlock()
			pNum++
		}
		_ = tp.acquire(providers[i])
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
			if tw.onlineStatus() {
				tw.turnToOffline()
			}
			// todo: some of them maybe release, need to check expire time first.
			offlineTwinNum++
		} else {
			pNum++
		}
	}

	missedTwinNum := len(tp.mpp) - pNum
	if missedTwinNum > 0 {
		//  means some of providers haven't the pair twins.
		for _, pd := range tp.mpp {
			_ = tp.acquire(pd)
		}
	}

	return offlineTwinNum, missedTwinNum
}

func (tp *twinsPool) pairStatus(pub kademlia.PublicKey) bool {
	_, pExist := tp.existServiceProvider(pub)
	tw, tExist := tp.existTwin(pub)
	if pExist && tExist {
		if !tw.onlineStatus() {
			tw.turnToOnline()
		}
		return true
	} else {
		return false
	}
}

func (tp *twinsPool) existServiceProvider(pubK kademlia.PublicKey) (*twinServiceProvider, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	p, exist := tp.mpp[pubK]
	return p, exist
}

func (tp *twinsPool) existTwin(pubK kademlia.PublicKey) (*twin, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	tw, exist := tp.mpt[pubK]
	return tw, exist
}

func (tp *twinsPool) acquire(provider *twinServiceProvider) *twin {
	pubK := (*provider).KadID().Pub
	tw, exist := tp.existTwin(pubK)
	if exist {
		if !tw.onlineStatus() {
			tw.turnToOnline()
		}
		return tw
	}

	v := tp.sp.Get()
	if v == nil {
		v = newTwin(provider)
	} else {
		v.(*twin).initWithOnline(provider)
	}
	tw = v.(*twin)

	tp.mu.Lock()
	tp.mpt[pubK] = tw
	tp.mu.Unlock()

	return tw
}

func (tp *twinsPool) release(tw *twin) {
	pubK := (*tw.prd).KadID().Pub
	tp.mu.RLock()
	_, exist := tp.mpt[pubK]
	tp.mu.RUnlock()

	if exist {
		tp.mu.Lock()
		delete(tp.mpt, pubK)
		tp.mu.Unlock()
	}

	tw.reset()
	tp.sp.Put(tw)
}
