package marina

import (
	"sync"

	"github.com/lithdew/kademlia"
)

var theMessagePacketPool = &messagePacketPool{sp: sync.Pool{}}

const (
	zeroMid = uint32(0)
	zeroQos = byte(0)
)

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
	defer mp.mu.Unlock()

	mp.pubKadId = pubKadId
	mp.mid = mid
	mp.qos = qos
	mp.topic = topic
	mp.payLoad = payLoad

	return mp
}

func (mpp *messagePacketPool) release(mp *MessagePacket) {
	mp.mu.Lock()
	mp.pubKadId = nil
	mp.brkKadId = nil
	mp.subKadId = nil
	mp.mid = zeroMid
	mp.qos = zeroQos
	mp.topic = nil
	mp.payLoad = nil
	mp.mu.Unlock()

	mpp.sp.Put(mp)
}
