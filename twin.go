package marina

import (
	"fmt"
	"sync/atomic"

	"github.com/lithdew/kademlia"
)

const defaultTwinChannelSize = 32 // The default channel size for the twin

type twin struct {
	kadId *kademlia.ID // the peer node ID
	tc    chan []byte

	counter uint32 // the counter for the push operation.
	online  bool   // the flag about the activity of the peer-node twin, if true means that can work, otherwise cannot.
}

func NewTwin(peerNodeId *kademlia.ID) *twin {
	return &twin{
		kadId:   peerNodeId,
		tc:      make(chan []byte, defaultTwinChannelSize),
		counter: uint32(0),
		online:  true,
	}
}

func (t *twin) Push(payLoad []byte) error {
	if !t.online {
		//maybe need to cache the payload.

		return fmt.Errorf("the '%s:%d' host's twin is not online", t.kadId.Host.String(), t.kadId.Port)
	}

	atomic.AddUint32(&t.counter, uint32(1))
	t.tc <- payLoad
	return nil
}
