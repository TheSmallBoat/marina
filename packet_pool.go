package marina

import (
	"sync"

	"github.com/lithdew/kademlia"
)

var theMessagePacketPool = &messagePacketPool{sp: sync.Pool{}}

type messagePacketPool struct {
	sp sync.Pool
}

func (mpp *messagePacketPool) acquire(pubKadId *kademlia.ID, mid uint32, qos byte, topic []byte, payLoad []byte) *MessagePacket {
	v := mpp.sp.Get()
	if v == nil {
		v = &MessagePacket{mu: sync.Mutex{}}
	}
	mp := v.(*MessagePacket)

	mp.mu.Lock()
	mp.pubKadId = pubKadId
	mp.mid = mid
	mp.qos = qos
	mp.topic = topic
	mp.payLoad = payLoad
	mp.mu.Unlock()

	return mp
}

func (mpp *messagePacketPool) release(mp *MessagePacket) {
	mp.mu.Lock()
	mp.pubKadId = nil
	mp.brkKadId = nil
	mp.subKadId = nil
	mp.mid = uint32(0)
	mp.qos = byte(0)
	mp.topic = make([]byte, 0)
	mp.payLoad = make([]byte, 0)
	mp.mu.Unlock()

	mpp.sp.Put(mp)
}
