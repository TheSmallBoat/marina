package marina

import (
	"sync"

	rpc "github.com/TheSmallBoat/carlo/streaming_rpc"
	"github.com/lithdew/kademlia"
)

type twinsPool struct {
	mu sync.RWMutex
	sp sync.Pool

	mpt map[kademlia.PublicKey]*twin
	mpp map[kademlia.PublicKey]*rpc.Provider
}

func newTwinsPool() *twinsPool {
	return &twinsPool{
		mu:  sync.RWMutex{},
		sp:  sync.Pool{},
		mpt: make(map[kademlia.PublicKey]*twin),
		mpp: make(map[kademlia.PublicKey]*rpc.Provider),
	}
}

func (tp *twinsPool) length() (int, int) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	return len(tp.mpt), len(tp.mpp)
}

func (tp *twinsPool) exist(peerNodeId *kademlia.ID) (*twin, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	t, exist := tp.mpt[peerNodeId.Pub]
	return t, exist
}

func (tp *twinsPool) acquire(peerNodeId *kademlia.ID) *twin {
	t, exist := tp.exist(peerNodeId)
	if exist {
		if !t.onlineStatus() {
			t.turnToOnline()
		}
		return t
	}

	tp.mu.RLock()
	v := tp.sp.Get()
	tp.mu.RUnlock()

	if v == nil {
		v = newTwin(peerNodeId)
	} else {
		v.(*twin).initWithOnline(peerNodeId)
	}

	t = v.(*twin)

	tp.mu.Lock()
	tp.mpt[t.kadId.Pub] = t
	tp.mu.Unlock()

	return t
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
