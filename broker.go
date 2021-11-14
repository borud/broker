package broker

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	"github.com/dghubble/trie"
)

// Printfer is a Printf'er - that is, you can give it a function that looks
// like Printf.
type Printfer func(string, ...interface{}) (int, error)

// Broker represents the message broker.
type Broker struct {
	downStreamChanLen  int
	publishChanLen     int
	subscribeChanLen   int
	unsubscribeChanLen int
	ctx                context.Context
	cancelFunc         context.CancelFunc
	topics             *trie.PathTrie
	publishCh          chan Message
	subscribeCh        chan Subscriber
	unsubscribeCh      chan unsubscribe
	lastSubscriberID   uint64
	closed             chan interface{}
	isClosed           atomic.Value
	deliveryTimeout    time.Duration
	deliveryCount      uint64
	droppedCount       uint64
	logPrintf          Printfer
}

// Config contains the broker configuration.
type Config struct {
	// DownStreamChanLen is the length of the channel used to send messages to the subscriber
	DownStreamChanLen int
	// PublishChanLen is the length of the incoming channel used by Publish()
	PublishChanLen int
	// SubscribeChanLen is the length of the channel that accepts Subscribe() requests
	SubscribeChanLen int
	// UnsubscribeChanLen is the length of the channel that accepts unsubscribe requests.
	// This is used by the Cancel() call on Subscriber
	UnsubscribeChanLen int
	// DeliveryTimeout is the timeout before giving up delivering a message to a subscriber
	DeliveryTimeout time.Duration
	// Logger provides a logger for logging errors.  Libraries shouldn't log so
	// this is a compromise.
	Logger Printfer
}

type topic struct {
	Name        string
	Subscribers map[uint64]*Subscriber
}

// Message contains the topic name the message was sent to and the
// payload.
type Message struct {
	Topic   string
	Payload interface{}
}

type unsubscribe struct {
	id        uint64
	topicName string
}

// ErrTimedTimedOut operation timed out
var ErrTimedTimedOut = errors.New("operation timed out")

// ErrBrokerClosed the broker has been closed
var ErrBrokerClosed = errors.New("Broker has been closed")

const (
	defaultDownstreamChanLen  = 5
	defaultPublishChanlen     = 5
	defaultSubscribeChanLen   = 5
	defaultUnsubscribeChanLen = 5
	defaultDeliveryTimeout    = 100 * time.Millisecond
)

// New returns a new broker.
func New(config Config) *Broker {
	ctx, cancelFunc := context.WithCancel(context.Background())

	if config.DownStreamChanLen == 0 {
		config.DownStreamChanLen = defaultDownstreamChanLen
	}

	if config.PublishChanLen == 0 {
		config.PublishChanLen = defaultPublishChanlen
	}

	if config.SubscribeChanLen == 0 {
		config.SubscribeChanLen = defaultSubscribeChanLen
	}

	if config.UnsubscribeChanLen == 0 {
		config.UnsubscribeChanLen = defaultUnsubscribeChanLen
	}

	if config.DeliveryTimeout == 0 {
		config.DeliveryTimeout = defaultDeliveryTimeout
	}

	if config.Logger == nil {
		config.Logger = func(string, ...interface{}) (int, error) { return 0, nil }
	}

	broker := &Broker{
		downStreamChanLen:  config.DownStreamChanLen,
		publishChanLen:     config.PublishChanLen,
		subscribeChanLen:   config.SubscribeChanLen,
		unsubscribeChanLen: config.UnsubscribeChanLen,
		ctx:                ctx,
		cancelFunc:         cancelFunc,
		topics:             trie.NewPathTrie(),
		publishCh:          make(chan Message, config.PublishChanLen),
		subscribeCh:        make(chan Subscriber, config.SubscribeChanLen),
		unsubscribeCh:      make(chan unsubscribe, config.UnsubscribeChanLen),
		lastSubscriberID:   0,
		closed:             make(chan interface{}),
		isClosed:           atomic.Value{},
		deliveryTimeout:    config.DeliveryTimeout,
		deliveryCount:      0,
		droppedCount:       0,
		logPrintf:          config.Logger,
	}
	go broker.mainLoop()
	return broker
}

// Subscribe creates a subscription and asks the broker to add it to
// its subscribers.
func (b *Broker) Subscribe(topicName string) (*Subscriber, error) {
	if b.isClosed.Load() != nil {
		return nil, ErrBrokerClosed
	}

	// Create and add subscriber
	subscriber := Subscriber{
		id:           atomic.AddUint64(&b.lastSubscriberID, 1),
		topicName:    topicName,
		downstreamCh: make(chan Message, b.downStreamChanLen),
		broker:       b,
		subscribed:   make(chan interface{}),
	}
	b.subscribeCh <- subscriber
	return &subscriber, nil
}

// Publish a payload to topic.
func (b *Broker) Publish(topic string, payload interface{}, timeout time.Duration) error {
	if b.isClosed.Load() != nil {
		return ErrBrokerClosed
	}

	m := Message{
		Topic:   topic,
		Payload: payload,
	}

	select {
	case b.publishCh <- m:
		return nil
	case <-time.After(timeout):
		return ErrTimedTimedOut
	}
}

// Counts returns the delivery count and dropped count respectively
func (b *Broker) Counts() (deliveryCount uint64, droppedCount uint64) {
	return atomic.LoadUint64(&b.deliveryCount), atomic.LoadUint64(&b.droppedCount)
}

// Shutdown broker
func (b *Broker) Shutdown() {
	b.isClosed.Store(new(interface{}))
	b.cancelFunc()
	<-b.closed
}

func (b *Broker) mainLoop() {
	for {
		select {
		case subscriber := <-b.subscribeCh:
			b.subscribeInternal(subscriber)

		case incoming := <-b.publishCh:
			b.publishInternal(incoming)

		case unsubMessage := <-b.unsubscribeCh:
			b.unsubscribeInternal(unsubMessage)

		case <-b.ctx.Done():
			b.shutdownInternal()
			return
		}
	}
}

func (b *Broker) subscribeInternal(sub Subscriber) error {
	t := b.topics.Get(sub.topicName)
	if t == nil {
		// If the topic doesn't exist we create it
		t = &topic{
			Name:        sub.topicName,
			Subscribers: make(map[uint64]*Subscriber),
		}
		b.topics.Put(sub.topicName, t)
	}

	topic, ok := t.(*topic)
	if !ok {
		return fmt.Errorf("inconsistency, topic was wrong type: %t", t)
	}

	topic.Subscribers[sub.id] = &sub

	// Signal that we are now subscribed
	close(sub.subscribed)

	return nil
}

func (b *Broker) unsubscribeInternal(u unsubscribe) {
	t := b.topics.Get(u.topicName)
	if t == nil {
		b.logPrintf("inconsistency: unsubscribe, topic %s did not exist", u.topicName)
		return
	}

	topic, ok := t.(*topic)
	if !ok {
		b.logPrintf("inconsistency, topic was wrong type: %t", t)
		return
	}

	delete(topic.Subscribers, u.id)

	// Remove topic if empty
	if len(topic.Subscribers) == 0 {
		b.topics.Delete(u.topicName)
	}
}

func (b *Broker) publishInternal(m Message) {
	err := b.topics.WalkPath(m.Topic, func(key string, value interface{}) error {
		if value == nil {
			return fmt.Errorf("node was nil for key=%s", key)
		}

		t, ok := value.(*topic)
		if !ok {
			return fmt.Errorf("value was wrong type: %t | %+v", value, value)
		}

		for _, sub := range t.Subscribers {
			select {
			case sub.downstreamCh <- m:
				atomic.AddUint64(&b.deliveryCount, 1)

			case <-time.After(b.deliveryTimeout):
				atomic.AddUint64(&b.droppedCount, 1)
				b.logPrintf("dropped message after deliveryTimeout: %+v", m)
			}
		}
		return nil
	})
	if err != nil {
		b.logPrintf("error walking topic tree: %v", err)
	}
}

func (b *Broker) shutdownInternal() {
	err := b.topics.Walk(func(key string, value interface{}) error {
		if value == nil {
			return fmt.Errorf("node was nil for key=%s", key)
		}

		t, ok := value.(*topic)
		if !ok {
			return fmt.Errorf("value was wrong type: %t | %+v", value, value)
		}

		for _, sub := range t.Subscribers {
			close(sub.downstreamCh)
		}
		return nil
	})

	if err != nil {
		b.logPrintf("error shutting down, Walk returned an error: %v", err)
	}
	close(b.closed)
}
