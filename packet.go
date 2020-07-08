package marina

import (
	"io"
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

func UnmarshalMessagePacket(buf []byte) (*MessagePacket, error) {
	var err error
	var size uint16
	var mid uint32
	var qos byte
	var topic, payLoad []byte
	var pubKadId, brkKadId, subKadId kademlia.ID

	if len(buf) < 5 {
		return nil, io.ErrUnexpectedEOF
	}
	mid, buf = bytesutil.Uint32BE(buf[:4]), buf[4:]
	qos, buf = buf[0], buf[1:]

	if len(buf) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	size, buf = bytesutil.Uint16BE(buf[:2]), buf[2:]
	topic, buf = buf[:size], buf[size:]

	if len(buf) < 2 {
		return nil, io.ErrUnexpectedEOF
	}
	size, buf = bytesutil.Uint16BE(buf[:2]), buf[2:]
	payLoad, buf = buf[:size], buf[size:]

	pubKadId, buf, err = kademlia.UnmarshalID(buf)
	if err != nil {
		return nil, err
	}

	brkKadId, buf, err = kademlia.UnmarshalID(buf)
	if err != nil {
		return nil, err
	}

	subKadId, buf, err = kademlia.UnmarshalID(buf)
	if err != nil {
		return nil, err
	}

	pkt := NewMessagePacket(&pubKadId, mid, qos, topic, payLoad)
	pkt.SetBrokerKadId(&brkKadId)
	pkt.SetSubscriberKadId(&subKadId)
	return pkt, nil
}
