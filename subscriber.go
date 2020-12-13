package broker

// Subscriber represents a subscription to a topic or topic prefix.
type Subscriber struct {
	id           uint64
	topicName    string
	downstreamCh chan Message
	broker       *Broker
	subscribed   chan interface{}
}

// Messages returns the message channel.
func (s *Subscriber) Messages() <-chan Message {
	<-s.subscribed
	return s.downstreamCh
}

// Cancel cancels the subscription
func (s *Subscriber) Cancel() error {
	if s.broker.isClosed.Load() != nil {
		return ErrBrokerClosed
	}

	s.broker.unsubscribeCh <- unsubscribe{
		id:        s.id,
		topicName: s.topicName,
	}
	return nil
}
