# Broker - a minimal pubsub

[![GoDoc Reference](https://godoc.org/github.com/borud/broker?status.svg)](http://godoc.org/github.com/borud/broker)

pubsub is a very small library for implementing the simplest possible
publish-subscribe mechanism for Go using channels. 

# Usage

    import "github.com/borud/broker"
	
**Creating a new broker**

	broker := broker.New()
		
**Subscribe to topic or topic prefix**
		
    sub, err := broker.Subscribe("/foo/bar")
	
**Fetch messages from subscription**

    for msg := range sub.Messages() {
        log.Printf("topic = '%s', message = '%+v'", msg.Topic, msg.Payload)
    }

**Publish message to broker**

    err := broker.Publish("/foo", "some payload")
	
**Cancel a subscription**
	
	err := sub.Cancel()

**Shut down broker**

	broker.Shutdown()

# Topics

Topics are entirely dynamic, meaning that a topic exists if there are
subscribers listening to it.  If a message is published to a topic
that has no subscribers, nothing will happen and the message is
silently discarded.

Topics are hierarchical and look like filesyste paths and matching is
by path prefix.

    /house/bedroom/light
	/house/bedroom/temp
    /house/kitchen/light
    /house/kitchen/temp
    /house/kitchen/humidity

Your subscription can be for any prefix of the path, including the
full path.  You will receive all messages that match your prefix.  So
for instance if you subscribe to `/house/kitchen` you will get all
messages sent to 

    /house/kitchen
	/house/kitchen/light
	/house/kitchen/temp
	/house/kitchen/humidity
	
If you subscribe to `/house/kitchen/temp` you will only get messages
sent to this single topic since it has no children.

At this time **no** wildcard matching is supported.
