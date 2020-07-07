package marina

import (
	"sync"

	"github.com/lithdew/kademlia"
)

type twinsPool struct {
	m  map[string]*twin
	sp sync.Pool
}

func newTwinsPool() *twinsPool {
	return &twinsPool{m: make(map[string]*twin), sp: sync.Pool{}}
}

func (tp *twinsPool) length() int {
	return len(tp.m)
}

func (tp *twinsPool) exist(peerNodeId *kademlia.ID) (*twin, bool) {
	t, exist := tp.m[peerNodeId.Pub.String()]
	return t, exist
}

func (tp *twinsPool) acquire(peerNodeId *kademlia.ID) *twin {
	t, exist := tp.exist(peerNodeId)
	if exist {
		t.turnToOnline()
		return t
	}

	v := tp.sp.Get()
	if v == nil {
		v = newTwin(peerNodeId)
	} else {
		v.(*twin).initWithOnline(peerNodeId)
	}

	t = v.(*twin)
	tp.m[t.kadId.Pub.String()] = t
	return t
}

func (tp *twinsPool) release(t *twin) {
	_, exist := tp.m[t.kadId.Pub.String()]
	if exist {
		delete(tp.m, t.kadId.Pub.String())
	}

	t.reset()
	tp.sp.Put(t)
}
