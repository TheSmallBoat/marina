package marina

import (
	"sync/atomic"

	"github.com/TheSmallBoat/cabinet"
	"github.com/lithdew/kademlia"
)

const defaultSubscriberMaxWorkers = 4

// The subscribe packets come from the peer-nodes.
type subscriber struct {
	tp *twinsPool
	wp *workerPool
	tt *cabinet.TTree

	subSucNum   uint32 // the success number of the subscribing operation
	subErrNum   uint32 // the error number of the subscribing operation
	unSubSucNum uint32 // the success number of the unsubscribing operation
	unSubErrNum uint32 // the error number of the unsubscribing operation
}

func NewSubscriber(twp *twinsPool) *subscriber {
	return &subscriber{
		wp:          NewWorkerPool(defaultSubscriberMaxWorkers),
		tp:          twp,
		tt:          cabinet.NewTopicTree(),
		subSucNum:   0,
		subErrNum:   0,
		unSubSucNum: 0,
		unSubErrNum: 0,
	}
}

// kid : the subscribe-peer-node kadId
func (s *subscriber) peerNodeSubscribe(kid *kademlia.ID, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}

	s.wp.SubmitTask(func() { processPeerNodeSubscribe(s, kid, topic) })
}

func (s *subscriber) peerNodeUnSubscribe(kid *kademlia.ID, qos byte, topic []byte) {
	if qos == byte(1) {
		// Todo:process response
	}

	s.wp.SubmitTask(func() { processPeerNodeUnSubscribe(s, kid, topic) })
}

// To link the twin for the peer-node to this topic
func processPeerNodeSubscribe(sub *subscriber, kid *kademlia.ID, topic []byte) {
	err := sub.tt.EntityLink(topic, sub.tp.acquire(kid))
	if err != nil {
		atomic.AddUint32(&sub.subErrNum, uint32(1))
	} else {
		atomic.AddUint32(&sub.subSucNum, uint32(1))
	}
}

// To unlink the twin for the peer-node to this topic
func processPeerNodeUnSubscribe(sub *subscriber, kid *kademlia.ID, topic []byte) {
	tw, exist := sub.tp.exist(kid)
	if exist {
		err := sub.tt.EntityUnLink(topic, tw)
		if err != nil {
			atomic.AddUint32(&sub.unSubErrNum, uint32(1))
		} else {
			atomic.AddUint32(&sub.unSubSucNum, uint32(1))
		}
	} else {
		atomic.AddUint32(&sub.unSubErrNum, uint32(1))
	}
}

func (s *subscriber) Close() {
	s.wp.Close()
	_ = s.tt.Close()
}
