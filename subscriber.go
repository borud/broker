package broker

// Subscriber represents a subscription to a topic or topic prefix.
type Subscriber struct {
	id            uint64
	topicName     string
	downstreamCh  chan Message
	unsubscribeCh chan unsubscribe
	broker        *Broker
}

// Messages returns the message channel.
func (s *Subscriber) Messages() <-chan Message {
	return s.downstreamCh
}

// Cancel cancels the subscription
func (s *Subscriber) Cancel() error {
	if s.broker.isClosed.Load() != nil {
		return ErrBrokerClosed
	}

	s.unsubscribeCh <- unsubscribe{
		id:        s.id,
		topicName: s.topicName,
	}
	return nil
}
