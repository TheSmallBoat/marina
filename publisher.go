package marina

const defaultPublisherMaxWorkers = 32

// The publish stream packets come from the producers.
type publisher struct {
	wp *workerPool
}

func NewPublisher() *publisher {
	return &publisher{wp: NewWorkerPool(defaultPublisherMaxWorkers)}
}

func (p *publisher) Publish(pkt *streamPacket) {
	if pkt.qos == byte(1) {
		// process response
	}
	p.wp.SubmitTask(func() { processPublishStreamPacket(pkt.topic, pkt.body) })

}

// To find the matched topic, and put the StreamPacket to the twin-pool
func processPublishStreamPacket(topic []byte, body []byte) {

}

func (p *publisher) Close() {
	p.wp.Close()
}
