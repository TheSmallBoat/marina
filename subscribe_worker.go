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
	wp *workingPool
	tp *twinsPool
	tt *cabinet.TTree

	subSucNum   uint32 // the success number of the subscribing operation
	subErrNum   uint32 // the error number of the subscribing operation
	unSubSucNum uint32 // the success number of the unsubscribing operation
	unSubErrNum uint32 // the error number of the unsubscribing operation

	wg sync.WaitGroup
}

func NewSubscribeWorker(twp *twinsPool, tTree *cabinet.TTree) *subscribeWorker {
	return &subscribeWorker{
		wp:          NewWorkingPool(defaultMaxSubscribeWorkers),
		tp:          twp,
		tt:          tTree,
		subSucNum:   0,
		subErrNum:   0,
		unSubSucNum: 0,
		unSubErrNum: 0,
	}
}

// kid : the subscribe-peer-node kadId
func (s *subscribeWorker) peerNodeSubscribe(kid *kademlia.ID, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}
	s.wg.Add(1)
	s.wp.SubmitTask(func() { processPeerNodeSubscribe(s, kid, topic) })
}

func (s *subscribeWorker) peerNodeUnSubscribe(kid *kademlia.ID, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}

	s.wg.Add(1)
	s.wp.SubmitTask(func() { processPeerNodeUnSubscribe(s, kid, topic) })
}

// To link the twin for the peer-node to this topic
func processPeerNodeSubscribe(subW *subscribeWorker, kid *kademlia.ID, topic []byte) {
	defer subW.wg.Done()

	err := subW.tt.EntityLink(topic, subW.tp.acquire(kid))
	if err != nil {
		atomic.AddUint32(&subW.subErrNum, uint32(1))
	} else {
		atomic.AddUint32(&subW.subSucNum, uint32(1))
	}
}

// To unlink the twin for the peer-node to this topic
func processPeerNodeUnSubscribe(subW *subscribeWorker, kid *kademlia.ID, topic []byte) {
	defer subW.wg.Done()

	tw, exist := subW.tp.exist(kid)
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
	s.wp.Close()
}

func (s *subscribeWorker) Wait() {
	s.wg.Wait()
}
