package marina

import (
	"sync"
	"sync/atomic"

	"github.com/TheSmallBoat/cabinet"
	"github.com/lithdew/kademlia"
)

const defaultMaxSubscribeWorkers = 4

// The subscribe packets come from the peer-nodes.
type subscribeWorker struct {
	tp  *taskPool
	twp *twinsPool
	tt  *cabinet.TTree

	subSucNum   uint32 // the success count of the subscribing operation
	subErrNum   uint32 // the error count of the subscribing operation
	unSubSucNum uint32 // the success count of the unsubscribing operation
	unSubErrNum uint32 // the error count of the unsubscribing operation

	wg sync.WaitGroup
}

func NewSubscribeWorker(twp *twinsPool, tTree *cabinet.TTree) *subscribeWorker {
	return &subscribeWorker{
		tp:          NewTaskPool(defaultMaxSubscribeWorkers),
		twp:         twp,
		tt:          tTree,
		subSucNum:   0,
		subErrNum:   0,
		unSubSucNum: 0,
		unSubErrNum: 0,
	}
}

// kid : the subscribe-peer-node kadId
func (s *subscribeWorker) peerNodeSubscribe(prd *twinServiceProvider, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}
	s.wg.Add(1)
	s.tp.SubmitTask(func() { processPeerNodeSubscribe(s, prd, topic) })
}

func (s *subscribeWorker) peerNodeUnSubscribe(pubK kademlia.PublicKey, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}

	s.wg.Add(1)
	s.tp.SubmitTask(func() { processPeerNodeUnSubscribe(s, pubK, topic) })
}

// To link the twin for the peer-node to this topic
func processPeerNodeSubscribe(subW *subscribeWorker, prd *twinServiceProvider, topic []byte) {
	defer subW.wg.Done()

	err := subW.tt.EntityLink(topic, subW.twp.acquire(prd))
	if err != nil {
		atomic.AddUint32(&subW.subErrNum, uint32(1))
	} else {
		atomic.AddUint32(&subW.subSucNum, uint32(1))
	}
}

// To unlink the twin for the peer-node to this topic
func processPeerNodeUnSubscribe(subW *subscribeWorker, pubK kademlia.PublicKey, topic []byte) {
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

func (s *subscribeWorker) Close() {
	s.tp.Close()
}

func (s *subscribeWorker) Wait() {
	s.wg.Wait()
}
