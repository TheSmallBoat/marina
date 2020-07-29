package marina

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"
)

const defaultTwinChannelSize = 32 // The default channel size for the twin.

type twin struct {
	prd *TwinServiceProvider

	tc   chan []byte   // The channel in the twin for receiving the data.
	exit chan struct{} // The channel in the twin for the exit signal of the task.

	mu     sync.RWMutex
	online bool      // The flag about the activity of the peer-node twin, if true means that can work, otherwise cannot.
	scTime time.Time // The change time of the online/offline status.

	pushSucNum   uint32 // The counter for the pushing operation while online.
	pushErrNum   uint32 // The counter for the pushing operation while offline.
	transSucNum  uint32 // the success count of the transmitting data operation
	transErrNum  uint32 // the error count of the transmitting data operation
	transSucSize uint64 // the success count of the transmitting data operation
	transErrSize uint64 // the error count of the transmitting data operation
}

func newTwin(provider *TwinServiceProvider) *twin {
	tw := &twin{
		prd:          provider,
		tc:           make(chan []byte, defaultTwinChannelSize),
		exit:         make(chan struct{}, 0),
		mu:           sync.RWMutex{},
		online:       false,
		pushSucNum:   uint32(0),
		pushErrNum:   uint32(0),
		transSucNum:  uint32(0),
		transErrNum:  uint32(0),
		transSucSize: uint64(0),
		transErrSize: uint64(0),
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
		//todo : building a global cache for all the twins while they offline until expire.

		kadId := (*t.prd).KadID()
		atomic.AddUint32(&t.pushErrNum, uint32(1))
		return fmt.Errorf("the '%s:%d' host's twin is not online", kadId.Host.String(), kadId.Port)
	}

	t.tc <- pkt
	atomic.AddUint32(&t.pushSucNum, uint32(1))
	return nil
}

/*
func (t *twin) pullMessagePacketFromChannel() ([]byte, bool) {
	pkt, ok := <-t.tc
	return pkt, ok
}*/

func (t *twin) turnToOffline() {
	if t.onlineStatus() {
		t.exit <- struct{}{}

		t.mu.Lock()
		defer t.mu.Unlock()

		t.online = false
		t.scTime = time.Now()
	}
}

func (t *twin) turnToOnline() {
	if !t.onlineStatus() {
		t.mu.Lock()
		defer t.mu.Unlock()

		t.executeTask()
		t.online = true
		t.scTime = time.Now()
	}
}

func (t *twin) reset() {
	t.turnToOffline()

	t.mu.Lock()
	defer t.mu.Unlock()

	close(t.tc)
	t.prd = nil
	t.pushSucNum = 0
	t.pushErrNum = 0
	t.transSucNum = 0
	t.transErrNum = 0
	t.transSucSize = 0
	t.transErrSize = 0
}

func (t *twin) initWithOnline(provider *TwinServiceProvider) {
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
					size := len(data)
					err := (*t.prd).Push(data)
					if err != nil {
						atomic.AddUint32(&t.transErrNum, uint32(1))
						atomic.AddUint64(&t.transErrSize, uint64(size))
					} else {
						atomic.AddUint32(&t.transSucNum, uint32(1))
						atomic.AddUint64(&t.transSucSize, uint64(size))
					}
				}
			case <-t.exit:
				return
			}
		}
	}()
}

func (t *twin) close() {
	if t.onlineStatus() {
		t.exit <- struct{}{}
	}
	close(t.tc)
	close(t.exit)
}
