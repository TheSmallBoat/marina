package marina

const defaultPublisherChannelSize = 32

type publisher struct {
	ch   chan packet
	exit chan struct{}
}

func NewPublisher() *publisher {
	return &publisher{ch: make(chan packet, defaultPublisherChannelSize), exit: make(chan struct{}, 0)}
}

func (p *publisher) Publish(pkt *packet) {
	p.ch <- *pkt
}

func (p *publisher) Close() {
	p.exit <- struct{}{}
}

func (p *publisher) Start() {
	for {
		select {
		case <-p.exit:
			return
		case _, ok := <-p.ch:
			if ok {
				continue
			}

		}
	}
}
