package marina

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/lithdew/kademlia"
)

const defaultTwinChannelSize = 32 // The default channel size for the twin

type twin struct {
	kadId *kademlia.ID // the peer node ID
	tc    chan []byte

	online  bool   // the flag about the activity of the peer-node twin, if true means that can work, otherwise cannot.
	counter uint32 // the counter for the push operation while online.
	offNum  uint32 // the counter for the push operation while offline.
	mu      sync.RWMutex
}

func newTwin(peerNodeId *kademlia.ID) *twin {
	return &twin{
		kadId:   peerNodeId,
		tc:      make(chan []byte, defaultTwinChannelSize),
		online:  true,
		counter: uint32(0),
		offNum:  uint32(0),
		mu:      sync.RWMutex{},
	}
}

func (t *twin) Push(payLoad []byte) error {
	if !t.online {
		//maybe need to cache the payload.

		atomic.AddUint32(&t.offNum, uint32(1))
		return fmt.Errorf("the '%s:%d' host's twin is not online", t.kadId.Host.String(), t.kadId.Port)
	}

	atomic.AddUint32(&t.counter, uint32(1))
	t.tc <- payLoad
	return nil
}

func (t *twin) turnToOffline() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.online = false
}

func (t *twin) turnToOnline() {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.online = true
}

func (t *twin) setOnlineStatus(status bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	t.online = status
}

func (t *twin) reset() {
	close(t.tc)
	t.kadId = nil
	t.online = false
	t.counter = 0
	t.offNum = 0
}

func (t *twin) initWithOnline(peerNodeId *kademlia.ID) {
	t.tc = make(chan []byte, defaultTwinChannelSize)
	t.kadId = peerNodeId
	t.online = true
}
