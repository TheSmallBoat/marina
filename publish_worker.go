package marina

import (
	"sync"
	"sync/atomic"

	"github.com/TheSmallBoat/cabinet"
	"github.com/lithdew/kademlia"
)

const defaultMaxPublishWorkers = 32

// The publish message-packets come from the producers.
type publishWorker struct {
	tp    *taskPool
	tt    *cabinet.TTree
	kadId *kademlia.ID //the broker-peer-node kadID

	pubSucNum uint32 // the success number of the publishing operation
	pubErrNum uint32 // the error number of the publishing operation
	fwdSucNum uint32 // the success number of the forwarding operation
	fwdErrNum uint32 // the error number of the forwarding operation

	wg sync.WaitGroup
}

func NewPublishWorker(bKadId *kademlia.ID, tTree *cabinet.TTree) *publishWorker {
	return &publishWorker{
		tp:        NewTaskPool(defaultMaxPublishWorkers),
		kadId:     bKadId,
		tt:        tTree,
		pubSucNum: 0,
		pubErrNum: 0,
		fwdSucNum: 0,
		fwdErrNum: 0,
	}
}

func (p *publishWorker) EntitiesNumFor(topic []byte) int {
	var entities = p.EntitiesFor(topic)
	if entities == nil {
		return 0
	}
	return len(entities)
}

func (p *publishWorker) EntitiesFor(topic []byte) []interface{} {
	entities := make([]interface{}, 0)
	err := p.tt.LinkedEntities(topic, &entities)
	if err != nil || len(entities) < 1 {
		return nil
	}
	return entities
}

func (p *publishWorker) WorkFor(pkt *MessagePacket) {
	if pkt.qos == byte(1) {
		// Todo:process response
	}

	p.wg.Add(1)
	p.tp.SubmitTask(func() { forwardMessagePacket(p, pkt) })
}

// To find the matched topic, and put the messagePacket to the twin
func forwardMessagePacket(pubW *publishWorker, pkt *MessagePacket) {
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
			pkt.SetSubscriberKadId(tw.kadId)

			dst = dst[0:0]
			err := tw.PushMessagePacketToChannel(pkt.AppendTo(dst))
			if err != nil {
				atomic.AddUint32(&pubW.fwdErrNum, uint32(1))
			} else {
				atomic.AddUint32(&pubW.fwdSucNum, uint32(1))
			}
		}
	}

	atomic.AddUint32(&pubW.pubSucNum, uint32(1))
}

func (p *publishWorker) Close() {
	p.tp.Close()
}

func (p *publishWorker) Wait() {
	p.wg.Wait()
}
