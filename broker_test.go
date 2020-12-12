package broker

import (
	"log"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

// This is really more of a smoke test
func TestBroker(t *testing.T) {
	b := New()
	defer b.Shutdown()

	var wg1 sync.WaitGroup
	wg1.Add(2)

	// Top level subscriber.  This one should get all messages that
	// match the "/a" prefix.
	go func() {
		sub := b.Subscribe("/a")
		assert.NotNil(t, sub)

		wg1.Done()
		for msg := range sub.Messages() {
			log.Printf("sub /a   : %+v", msg)
		}
	}()

	// Leaf node subscriber.  This one should not get anything
	// published to "/a" top level node.
	go func() {
		sub := b.Subscribe("/a/b")
		assert.NotNil(t, sub)

		wg1.Done()
		for msg := range sub.Messages() {
			log.Printf("sub /a/b : %+v", msg)
		}
	}()

	var err error

	wg1.Wait()

	err = b.Publish("/a", "should be received by A", time.Millisecond)
	assert.Nil(t, err)

	err = b.Publish("/a/b", "should be received by both", time.Millisecond)
	assert.Nil(t, err)

	err = b.Publish("/something/else", "foo", time.Millisecond)
	assert.Nil(t, err)

	time.Sleep(100 * time.Millisecond)
}
