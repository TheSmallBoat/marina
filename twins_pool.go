package marina

import (
	"sync"

	"github.com/lithdew/kademlia"
)

type twinsPool struct {
	m  map[string]*twin
	sp sync.Pool

	mu sync.RWMutex
}

func newTwinsPool() *twinsPool {
	return &twinsPool{m: make(map[string]*twin), sp: sync.Pool{}}
}

func (tp *twinsPool) length() int {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	return len(tp.m)
}

func (tp *twinsPool) exist(peerNodeId *kademlia.ID) (*twin, bool) {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	t, exist := tp.m[peerNodeId.Pub.String()]
	return t, exist
}

func (tp *twinsPool) acquire(peerNodeId *kademlia.ID) *twin {
	t, exist := tp.exist(peerNodeId)
	if exist {
		t.turnToOnline()
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
	tp.m[t.kadId.Pub.String()] = t
	tp.mu.Unlock()

	return t
}

func (tp *twinsPool) release(t *twin) {
	tp.mu.RLock()
	_, exist := tp.m[t.kadId.Pub.String()]
	tp.mu.RUnlock()

	if exist {
		tp.mu.Lock()
		delete(tp.m, t.kadId.Pub.String())
		tp.mu.Unlock()
	}

	t.reset()
	tp.sp.Put(t)
}
