package marina

import (
	"sync"
	"sync/atomic"

	"github.com/TheSmallBoat/cabinet"
	"github.com/lithdew/kademlia"
)

const defaultMaxSubscribeWorkers = 4

// The subscribe packets come from the peer-nodes.
type SubscribeWorker struct {
	tp  *taskPool
	twp *TwinsPool
	tt  *cabinet.TTree

	subSucNum   uint32 // the success count of the subscribing operation
	subErrNum   uint32 // the error count of the subscribing operation
	unSubSucNum uint32 // the success count of the unsubscribing operation
	unSubErrNum uint32 // the error count of the unsubscribing operation

	wg sync.WaitGroup
}

func NewSubscribeWorker(twp *TwinsPool, tTree *cabinet.TTree) *SubscribeWorker {
	return &SubscribeWorker{
		tp:          newTaskPool(defaultMaxSubscribeWorkers),
		twp:         twp,
		tt:          tTree,
		subSucNum:   0,
		subErrNum:   0,
		unSubSucNum: 0,
		unSubErrNum: 0,
	}
}

// kid : the subscribe-peer-node kadId
func (s *SubscribeWorker) PeerNodeSubscribe(prd *TwinServiceProvider, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}
	s.wg.Add(1)
	s.tp.submitTask(func() { processPeerNodeSubscribe(s, prd, topic) })
}

func (s *SubscribeWorker) PeerNodeUnSubscribe(pubK kademlia.PublicKey, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}

	s.wg.Add(1)
	s.tp.submitTask(func() { processPeerNodeUnSubscribe(s, pubK, topic) })
}

// To link the twin for the peer-node to this topic
func processPeerNodeSubscribe(subW *SubscribeWorker, prd *TwinServiceProvider, topic []byte) {
	defer subW.wg.Done()

	err := subW.tt.EntityLink(topic, subW.twp.acquire(prd))
	if err != nil {
		atomic.AddUint32(&subW.subErrNum, uint32(1))
	} else {
		atomic.AddUint32(&subW.subSucNum, uint32(1))
	}
}

// To unlink the twin for the peer-node to this topic
func processPeerNodeUnSubscribe(subW *SubscribeWorker, pubK kademlia.PublicKey, topic []byte) {
	defer subW.wg.Done()

	tw, exist := subW.twp.existTwin(pubK)
	if exist {
		err := subW.tt.EntityUnLink(topic, tw)
		if err != nil {
			atomic.AddUint32(&subW.unSubErrNum, uint32(1))
		} else {
			atomic.AddUint32(&subW.unSubSucNum, uint32(1))
		}
	} else {
		atomic.AddUint32(&subW.unSubErrNum, uint32(1))
	}
}

func (s *SubscribeWorker) Close() {
	s.tp.close()
}

func (s *SubscribeWorker) Wait() {
	s.wg.Wait()
}
