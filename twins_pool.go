package marina

import (
	"sync"
	"time"

	"github.com/lithdew/kademlia"
)

const defaultMaxTwinWorkers = 32
const defaultMaxTwinOfflineTimeDuration = time.Duration(5 * time.Minute)

type TwinsPool struct {
	mu sync.RWMutex
	sp sync.Pool

	ttp *taskPool
	// One remote service provider paired with one twin which own the same KadID.
	mpt map[kademlia.PublicKey]*twin
	mpp map[kademlia.PublicKey]*TwinServiceProvider

	maxOfflineTimeDuration time.Duration
}

func NewTwinsPool() *TwinsPool {
	return &TwinsPool{
		mu:                     sync.RWMutex{},
		sp:                     sync.Pool{},
		ttp:                    newTaskPool(defaultMaxTwinWorkers),
		mpt:                    make(map[kademlia.PublicKey]*twin),
		mpp:                    make(map[kademlia.PublicKey]*TwinServiceProvider),
		maxOfflineTimeDuration: defaultMaxTwinOfflineTimeDuration,
	}
}

func (tp *TwinsPool) length() (int, int) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	return len(tp.mpt), len(tp.mpp)
}

// return the number of the append providers
func (tp *TwinsPool) appendProviders(providers ...*TwinServiceProvider) int {
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
		// acquire twin
		_ = tp.acquire(providers[i])
	}
	return pNum
}

// return the number of the removed providers, the number of excess-twins,
func (tp *TwinsPool) removeProviders(providers ...*TwinServiceProvider) (int, int) {
	var pNum, excessTwinNum = 0, 0
	for i := range providers {
		pubK := (*providers[i]).KadID().Pub
		_, pExist := tp.existServiceProvider(pubK)
		if pExist {
			tp.mu.Lock()
			delete(tp.mpp, pubK)
			tp.mu.Unlock()
			pNum++
		}
		tw, tExist := tp.existTwin(pubK)
		if tExist {
			// offline or release twin
			excessTwinNum++
			if tw.onlineStatus() {
				tw.turnToOffline()
			} else {
				// Releasing the twin that has been offline for too long.
				if time.Since(tw.scTime) > tp.maxOfflineTimeDuration {
					tp.release(tw)
				}
			}
		}
	}
	return pNum, excessTwinNum
}

//return the number of excess-twins, the number of lacking-twins
func (tp *TwinsPool) checkTwinsProvidersPairStatus() (int, int) {
	var pNum, excessTwinNum = 0, 0
	for k, tw := range tp.mpt {
		tp.mu.RLock()
		_, exist := tp.mpp[k]
		tp.mu.RUnlock()

		if exist {
			pNum++
			if !tw.onlineStatus() {
				tw.turnToOnline()
			}
		} else {
			excessTwinNum++
			if tw.onlineStatus() {
				tw.turnToOffline()
			} else {
				// Releasing the twin that has been offline for too long.
				if time.Since(tw.scTime) > tp.maxOfflineTimeDuration {
					tp.release(tw)
				}
			}
		}
	}

	lackingTwinNum := len(tp.mpp) - pNum
	if lackingTwinNum > 0 {
		//  means some of providers haven't the pair twins.
		for _, pd := range tp.mpp {
			_ = tp.acquire(pd)
		}
	}

	return excessTwinNum, lackingTwinNum
}

/*
func (tp *TwinsPool) pairStatus(pub kademlia.PublicKey) bool {
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
}*/

func (tp *TwinsPool) existServiceProvider(pubK kademlia.PublicKey) (*TwinServiceProvider, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	p, exist := tp.mpp[pubK]
	return p, exist
}

func (tp *TwinsPool) existTwin(pubK kademlia.PublicKey) (*twin, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	tw, exist := tp.mpt[pubK]
	return tw, exist
}

func (tp *TwinsPool) acquire(provider *TwinServiceProvider) *twin {
	if provider == nil || (*provider).KadID() == nil {
		return nil
	}

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

func (tp *TwinsPool) release(tw *twin) {
	if tw == nil || tw.prd == nil || (*tw.prd).KadID() == nil {
		// already reset or kadId pointer equal nil
		return
	}

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

func (tp *TwinsPool) close() {
	tp.ttp.close()
	for _, tw := range tp.mpt {
		tw.close()
	}
}
