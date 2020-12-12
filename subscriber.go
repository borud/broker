package broker

// Subscriber ...
type Subscriber struct {
	id            uint64
	topicName     string
	downstreamCh  chan Message
	unsubscribeCh chan unsubscribe
}

// Messages returns the message channel
func (s *Subscriber) Messages() <-chan Message {
	return s.downstreamCh
}

// Cancel cancels the subscription
func (s *Subscriber) Cancel() {
	s.unsubscribeCh <- unsubscribe{
		id:        s.id,
		topicName: s.topicName,
	}
}
