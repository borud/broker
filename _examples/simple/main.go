package main

import (
	"log"
	"time"

	"github.com/borud/broker"
)

func main() {
	b := broker.New(broker.Config{
		DownStreamChanLen:  100,
		PublishChanLen:     100,
		SubscribeChanLen:   10,
		UnsubscribeChanLen: 10,
		DeliveryTimeout:    100 * time.Millisecond,
	})

	// Create a subscription for "foo".  Note that later we publish to
	// "foo/bar".  Subscriptions are by path prefix, so anything that has
	// the same path prefix will be received by this subscriber.
	sub, err := b.Subscribe("foo")
	if err != nil {
		log.Fatalf("unable to subscribe: %v", err)
	}

	// Fire up goroutine which consumes messages
	go func() {
		for msg := range sub.Messages() {
			log.Printf("sub> %+v", msg)
		}
	}()

	// Create and publish messages forever (until you interrupt the program)
	for {
		err := b.Publish("foo/bar", "this is a message", time.Second)
		if err != nil {
			log.Fatalf("unable to publish message: %v", err)
		}

		time.Sleep(750 * time.Millisecond)
	}
}
