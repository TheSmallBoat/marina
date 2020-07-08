package marina

import (
	"sync/atomic"

	"github.com/TheSmallBoat/cabinet"
	"github.com/lithdew/kademlia"
)

const defaultPublisherMaxWorkers = 32

// The publish message-packets come from the producers.
type publisher struct {
	kadId *kademlia.ID //the broker-peer-node kadID

	wp *workerPool
	tt *cabinet.TTree

	pubSucNum uint32 // the success number of the publishing operation
	pubErrNum uint32 // the error number of the publishing operation
	fwdSucNum uint32 // the success number of the forwarding operation
	fwdErrNum uint32 // the error number of the forwarding operation
	entNilNum uint32 // the error number of the nil subscribe entity
}

func NewPublisher(bKadId *kademlia.ID, tTree *cabinet.TTree) *publisher {
	return &publisher{
		kadId:     bKadId,
		wp:        NewWorkerPool(defaultPublisherMaxWorkers),
		tt:        tTree,
		pubSucNum: 0,
		pubErrNum: 0,
		fwdSucNum: 0,
		fwdErrNum: 0,
	}
}

// skid : the source-peer-node KadID
func (p *publisher) Publish(pkt *messagePacket) {
	if pkt.qos == byte(1) {
		// Todo:process response
	}

	p.wp.SubmitTask(func() { processPublishMessagePacket(p, pkt) })
}

// To find the matched topic, and put the StreamPacket to the twin-pool
func processPublishMessagePacket(p *publisher, pkt *messagePacket) {
	pkt.setBrokerKadId(p.kadId)

	entities := make([]interface{}, 0)
	err := p.tt.LinkedEntities(pkt.topic, &entities)
	if err != nil || len(entities) < 1 {
		atomic.AddUint32(&p.pubErrNum, uint32(1))
		return
	}
	atomic.AddUint32(&p.pubSucNum, uint32(1))

	for _, v := range entities {
		tw := v.(*twin)
		if tw == nil {
			atomic.AddUint32(&p.entNilNum, uint32(1))
		} else {
			pkt.setSubscriberKadId(tw.kadId)

			dst := make([]byte, 0)
			err := tw.Push(pkt.AppendTo(dst))
			if err != nil {
				atomic.AddUint32(&p.fwdErrNum, uint32(1))
			}
			atomic.AddUint32(&p.fwdSucNum, uint32(1))
		}
	}
}

func (p *publisher) Close() {
	p.wp.Close()
}
