package marina

import (
	"sync"

	"github.com/lithdew/kademlia"
)

type twinsPool struct {
	m  map[string]*twin
	sp sync.Pool
}

func newTwins() *twinsPool {
	return &twinsPool{m: make(map[string]*twin), sp: sync.Pool{}}
}

func (tp *twinsPool) length() int {
	return len(tp.m)
}

func (tp *twinsPool) acquire(peerNodeId *kademlia.ID) *twin {
	t, exist := tp.m[peerNodeId.Pub.String()]
	if exist {
		t.turnToOnline()
		return t
	}

	v := tp.sp.Get()
	if v == nil {
		v = newTwin(peerNodeId)
	}
	tt := v.(*twin)

	tt.turnToOnline()
	tp.m[t.kadId.Pub.String()] = tt
	return tt
}

func (tp *twinsPool) release(t *twin) {
	_, exist := tp.m[t.kadId.Pub.String()]
	if exist {
		delete(tp.m, t.kadId.Pub.String())
	}

	t.reset()
	tp.sp.Put(t)
}
