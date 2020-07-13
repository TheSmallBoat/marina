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

	mu      sync.RWMutex
	online  bool   // the flag about the activity of the peer-node twin, if true means that can work, otherwise cannot.
	counter uint32 // the counter for the push operation while online.
	offNum  uint32 // the counter for the push operation while offline.
}

func newTwin(peerNodeId *kademlia.ID) *twin {
	return &twin{
		kadId:   peerNodeId,
		tc:      make(chan []byte, defaultTwinChannelSize),
		mu:      sync.RWMutex{},
		online:  true,
		counter: uint32(0),
		offNum:  uint32(0),
	}
}

func (t *twin) CheckOnlineStatus() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Todo: test the peer-provide's status

	return t.online
}

func (t *twin) PushMessagePacket(pkt []byte) error {
	if !t.CheckOnlineStatus() {
		//maybe need to cache the pkt.

		atomic.AddUint32(&t.offNum, uint32(1))
		return fmt.Errorf("the '%s:%d' host's twin is not online", t.kadId.Host.String(), t.kadId.Port)
	}

	t.tc <- pkt
	atomic.AddUint32(&t.counter, uint32(1))

	return nil
}

func (t *twin) PullMessagePacket() ([]byte, bool) {
	pkt, ok := <-t.tc
	return pkt, ok
}

func (t *twin) turnToOffline() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.online = false
}

func (t *twin) turnToOnline() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.online = true
}

func (t *twin) setOnlineStatus(status bool) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.online = status
}

func (t *twin) reset() {
	t.mu.Lock()
	defer t.mu.Unlock()

	close(t.tc)
	t.kadId = nil
	t.online = false
	t.counter = 0
	t.offNum = 0
}

func (t *twin) initWithOnline(peerNodeId *kademlia.ID) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.tc = make(chan []byte, defaultTwinChannelSize)
	t.kadId = peerNodeId
	t.online = true
}
