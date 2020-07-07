package marina

import "github.com/lithdew/kademlia"

const defaultSubscriberMaxWorkers = 8

// The subscribe packets come from the peer-nodes.
type subscriber struct {
	wp *workerPool
}

func NewSubscriber() *subscriber {
	return &subscriber{wp: NewWorkerPool(defaultSubscriberMaxWorkers)}
}

func (s *subscriber) peerNodeSubscribe(kid *kademlia.ID, qos byte, topic []byte) {
	if qos == byte(1) {
		// process response
	}
	s.wp.SubmitTask(func() { processPeerNodeSubscribe(topic) })
}

// To link the peer-node to the topic, and create the twin pool for the twin-tunnel
func processPeerNodeSubscribe(topic []byte) {

}

func (s *subscriber) Close() {
	s.wp.Close()
}
