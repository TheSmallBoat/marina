package marina

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTwinChannelSize = 32 // The default channel size for the twin.

type twin struct {
	prd *twinServiceProvider

	tc   chan []byte   // The channel in the twin for receiving the data.
	exit chan struct{} // The channel in the twin for the exit signal of the task.

	mu     sync.RWMutex
	online bool      // The flag about the activity of the peer-node twin, if true means that can work, otherwise cannot.
	scTime time.Time // The change time of the online/offline status.

	pushSucNum  uint32 // The counter for the pushing operation while online.
	pushErrNum  uint32 // The counter for the pushing operation while offline.
	transSucNum uint32 // the success count of the transmitting data operation
	transErrNum uint32 // the error count of the transmitting data operation
}

func newTwin(provider *twinServiceProvider) *twin {
	tw := &twin{
		prd:         provider,
		tc:          make(chan []byte, defaultTwinChannelSize),
		exit:        make(chan struct{}, 0),
		mu:          sync.RWMutex{},
		online:      false,
		pushSucNum:  uint32(0),
		pushErrNum:  uint32(0),
		transSucNum: uint32(0),
		transErrNum: uint32(0),
	}

	tw.turnToOnline()

	return tw
}

func (t *twin) onlineStatus() bool {
	t.mu.RLock()
	defer t.mu.RUnlock()

	return t.online
}

func (t *twin) pushMessagePacketToChannel(pkt []byte) error {
	if !t.onlineStatus() {
		//todo : maybe need to cache the pkt until the expire time coming.

		kadId := (*t.prd).KadID()
		atomic.AddUint32(&t.pushErrNum, uint32(1))
		return fmt.Errorf("the '%s:%d' host's twin is not online", kadId.Host.String(), kadId.Port)
	}

	t.tc <- pkt
	atomic.AddUint32(&t.pushSucNum, uint32(1))
	return nil
}

func (t *twin) pullMessagePacketFromChannel() ([]byte, bool) {
	pkt, ok := <-t.tc
	return pkt, ok
}

func (t *twin) turnToOffline() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.exit <- struct{}{}
	t.online = false
	t.scTime = time.Now()
}

func (t *twin) turnToOnline() {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.online = true
	t.scTime = time.Now()
	t.executeTask()
}

func (t *twin) reset() {
	t.turnToOffline()

	t.mu.Lock()
	defer t.mu.Unlock()

	t.prd = nil
	t.pushSucNum = 0
	t.pushErrNum = 0
	t.transSucNum = 0
	t.transErrNum = 0
}

func (t *twin) initWithOnline(provider *twinServiceProvider) {
	t.mu.Lock()
	t.tc = make(chan []byte, defaultTwinChannelSize)
	t.prd = provider
	t.mu.Unlock()

	t.turnToOnline()
}

func (t *twin) executeTask() {
	go func() {
		for {
			select {
			case data, ok := <-t.tc:
				if ok {
					err := (*t.prd).Push(data)
					if err != nil {
						atomic.AddUint32(&t.transErrNum, uint32(1))
					} else {
						atomic.AddUint32(&t.transSucNum, uint32(1))
					}
				}
			case <-t.exit:
				return
			}
		}
	}()
}
