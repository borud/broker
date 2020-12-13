package broker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	"github.com/dghubble/trie"
)

// Broker represents the message broker.
type Broker struct {
	ctx              context.Context
	cancelFunc       context.CancelFunc
	topics           *trie.PathTrie
	publishCh        chan Message
	subscribeCh      chan Subscriber
	unsubscribeCh    chan unsubscribe
	lastSubscriberID uint64
	closed           chan interface{}
	// fetching an atomic value is fast, so we use this as a flag to
	// determine if writing to the channels is going to be safe.
	isClosed atomic.Value
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
var ErrTimedTimedOut = errors.New("Operation timed out")

// ErrBrokerClosed the broker has been closed
var ErrBrokerClosed = errors.New("Broker has been closed")

const (
	defaultDownstreamChanLen  = 5
	defaultPublishChanlen     = 5
	defaultSubscribeChanLen   = 5
	defaultUnsubscribeChanLen = 5
)

// New returns a new broker.
func New() *Broker {
	ctx, cancelFunc := context.WithCancel(context.Background())

	broker := &Broker{
		ctx:           ctx,
		cancelFunc:    cancelFunc,
		topics:        trie.NewPathTrie(),
		publishCh:     make(chan Message, defaultPublishChanlen),
		subscribeCh:   make(chan Subscriber, defaultSubscribeChanLen),
		unsubscribeCh: make(chan unsubscribe, defaultUnsubscribeChanLen),
		closed:        make(chan interface{}),
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
		downstreamCh: make(chan Message, defaultDownstreamChanLen),
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

		case <-time.After(100 * time.Millisecond):

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
		return fmt.Errorf("Inconsistency, topic was wrong type: %t", t)
	}

	topic.Subscribers[sub.id] = &sub

	// Signal that we are now subscribed
	close(sub.subscribed)

	return nil
}

func (b *Broker) unsubscribeInternal(u unsubscribe) {
	t := b.topics.Get(u.topicName)
	if t == nil {
		log.Printf("inconsistency: unsubscribe, topic %s did not exist", u.topicName)
		return
	}

	topic, ok := t.(*topic)
	if !ok {
		log.Printf("inconsistency, topic was wrong type: %t", t)
		return
	}

	delete(topic.Subscribers, u.id)

	// Remove topic if empty
	if len(topic.Subscribers) == 0 {
		b.topics.Delete(u.topicName)
	}
}

func (b *Broker) publishInternal(m Message) {
	deliveryCount := 0
	droppedCount := 0
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
				deliveryCount++

			case <-time.After(20 * time.Millisecond):
				droppedCount++
			}
		}
		return nil
	})
	if err != nil {
		log.Printf("Error walking topic tree: %v", err)
	}

	// log.Printf("Delivered %d, dropped %d", deliveryCount, droppedCount)
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
		log.Printf("Error shutting down, Walk returned an error: %v", err)
	}
	close(b.closed)
}
