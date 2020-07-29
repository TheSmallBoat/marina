package marina

import (
	"sync"
	"sync/atomic"

	"github.com/TheSmallBoat/cabinet"
	"github.com/lithdew/kademlia"
)

const defaultMaxPublishWorkers = 32

// The publish message-packets come from the producers.
type PublishWorker struct {
	tp    *taskPool
	tt    *cabinet.TTree
	kadId *kademlia.ID //the broker-peer-node kadID

	pubSucNum uint32 // the success count of the publishing operation
	pubErrNum uint32 // the error count of the publishing operation
	fwdSucNum uint32 // the success count of the forwarding operation
	fwdErrNum uint32 // the error count of the forwarding operation

	wg sync.WaitGroup
}

func NewPublishWorker(bKadId *kademlia.ID, tTree *cabinet.TTree) *PublishWorker {
	return &PublishWorker{
		tp:        newTaskPool(defaultMaxPublishWorkers),
		kadId:     bKadId,
		tt:        tTree,
		pubSucNum: 0,
		pubErrNum: 0,
		fwdSucNum: 0,
		fwdErrNum: 0,
	}
}

func (p *PublishWorker) EntitiesNumFor(topic []byte) int {
	var entities = p.EntitiesFor(topic)
	if entities == nil {
		return 0
	}
	return len(entities)
}

func (p *PublishWorker) EntitiesFor(topic []byte) []interface{} {
	entities := make([]interface{}, 0)
	err := p.tt.LinkedEntities(topic, &entities)
	if err != nil || len(entities) < 1 {
		return nil
	}
	return entities
}

func (p *PublishWorker) WorkFor(pkt *MessagePacket) {
	if pkt.qos == byte(1) {
		// Todo:process response
	}

	p.wg.Add(1)
	p.tp.submitTask(func() { forwardMessagePacket(p, pkt) })
}

// To find the matched topic, and put the messagePacket to the twin
func forwardMessagePacket(pubW *PublishWorker, pkt *MessagePacket) {
	defer pubW.wg.Done()

	pkt.SetBrokerKadId(pubW.kadId)

	entities := pubW.EntitiesFor(pkt.topic)
	if entities == nil {
		atomic.AddUint32(&pubW.pubErrNum, uint32(1))
		return
	}

	dst := make([]byte, 0)
	for _, v := range entities {
		tw := v.(*twin)
		if tw != nil {
			pkt.SetSubscriberKadId((*tw.prd).KadID())

			dst = dst[0:0]
			err := tw.pushMessagePacketToChannel(pkt.AppendTo(dst))
			if err != nil {
				atomic.AddUint32(&pubW.fwdErrNum, uint32(1))
			} else {
				atomic.AddUint32(&pubW.fwdSucNum, uint32(1))
			}
		}
	}

	atomic.AddUint32(&pubW.pubSucNum, uint32(1))
}

func (p *PublishWorker) Close() {
	p.tp.close()
}

func (p *PublishWorker) Wait() {
	p.wg.Wait()
}
