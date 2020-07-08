package marina

import (
	"sync"

	"github.com/lithdew/bytesutil"
	"github.com/lithdew/kademlia"
)

type MessagePacket struct {
	mu sync.Mutex

	pubKadId *kademlia.ID // the publish-peer-node KadId
	brkKadId *kademlia.ID // the broker-peer-node KadId
	subKadId *kademlia.ID // the subscribe-peer-node KadId

	mid     uint32 // the number of the message-packet by the creator
	qos     byte
	topic   []byte
	payLoad []byte
}

// Todo: add messagePacketPool

func NewMessagePacket(pubKadId *kademlia.ID, mid uint32, qos byte, topic []byte, payLoad []byte) *MessagePacket {
	return &MessagePacket{
		mu:       sync.Mutex{},
		pubKadId: pubKadId,
		brkKadId: nil,
		subKadId: nil,
		mid:      mid,
		qos:      qos,
		topic:    topic,
		payLoad:  payLoad,
	}
}

func (mp *MessagePacket) SetBrokerKadId(kadId *kademlia.ID) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.brkKadId = kadId
}

func (mp *MessagePacket) SetSubscriberKadId(kadId *kademlia.ID) {
	mp.mu.Lock()
	defer mp.mu.Unlock()

	mp.subKadId = kadId
}

func (mp *MessagePacket) AppendTo(dst []byte) []byte {
	dst = bytesutil.AppendUint32BE(dst, mp.mid)
	dst = append(dst, mp.qos)
	dst = bytesutil.AppendUint16BE(dst, uint16(len(mp.topic)))
	dst = append(dst, mp.topic...)
	dst = bytesutil.AppendUint16BE(dst, uint16(len(mp.payLoad)))
	dst = append(dst, mp.payLoad...)
	dst = mp.pubKadId.AppendTo(dst)
	dst = mp.brkKadId.AppendTo(dst)
	dst = mp.subKadId.AppendTo(dst)
	return dst
}

func (mp *MessagePacket) UnmarshalMessagePacket(buf []byte) (*MessagePacket, error) {
	return nil, nil
}
