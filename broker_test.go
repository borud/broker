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
	b := New(Config{Logger: log.Printf})

	var wgSubscribed sync.WaitGroup
	wgSubscribed.Add(2)

	var wgCount sync.WaitGroup
	wgCount.Add(3)

	// Top level subscriber.  This one should get all messages that
	// match the "/a" prefix.
	{
		sub1, err := b.Subscribe("/a")
		assert.Nil(t, err)
		assert.NotNil(t, sub1)
		mChan1 := sub1.Messages()
		go func() {
			wgSubscribed.Done()
			for range mChan1 {
				wgCount.Done()
			}
		}()
	}

	// Leaf node subscriber.  This one should not get anything
	// published to "/a" top level node.
	{
		sub2, err := b.Subscribe("/a/b")
		assert.Nil(t, err)
		assert.NotNil(t, sub2)
		mChan2 := sub2.Messages()
		go func() {
			wgSubscribed.Done()
			for range mChan2 {
				wgCount.Done()
			}
		}()
	}

	wgSubscribed.Wait()

	err := b.Publish("/a", "should be received by A", 100*time.Millisecond)
	assert.Nil(t, err)

	err = b.Publish("/a/b", "should be received by both", 100*time.Millisecond)
	assert.Nil(t, err)

	wgCount.Wait()

	// Test double shutdowns
	b.Shutdown()
	b.Shutdown()
}

func TestDoubleMessages(t *testing.T) {
	b := New(Config{Logger: log.Printf})
	defer b.Shutdown()
	sub, err := b.Subscribe("/foo")
	assert.Nil(t, err)
	assert.NotNil(t, sub)

	sub.Messages()
	sub.Messages()
}

func TestShutdown(t *testing.T) {
	// Test subscribe
	{
		b := New(Config{Logger: log.Printf})
		assert.Nil(t, b.isClosed.Load())
		b.Shutdown()

		sub, err := b.Subscribe("/mytopic")
		assert.Nil(t, sub)
		assert.Equal(t, ErrBrokerClosed, err)
	}

	// Test publish
	{
		b := New(Config{})
		sub, err := b.Subscribe("/mytopic")
		assert.NotNil(t, sub)
		assert.Nil(t, err)
		b.Shutdown()

		err = b.Publish("/foo", "payload", 0)
		assert.Equal(t, ErrBrokerClosed, err)
	}

	// Test cancel
	{
		b := New(Config{Logger: log.Printf})
		sub, err := b.Subscribe("/mytopic")
		assert.NotNil(t, sub)
		assert.Nil(t, err)
		b.Shutdown()

		err = sub.Cancel()
		assert.Equal(t, ErrBrokerClosed, err)
	}

}

func BenchmarkSimple(b *testing.B) {
	broker := New(Config{
		DownStreamChanLen:  100,
		PublishChanLen:     100,
		SubscribeChanLen:   1,
		UnsubscribeChanLen: 1,
		DeliveryTimeout:    5 * time.Millisecond,
		Logger:             log.Printf,
	})

	wg := sync.WaitGroup{}

	for i := 0; i < 20; i++ {
		sub, err := broker.Subscribe("/foo")
		assert.NoError(b, err)
		assert.NotNil(b, sub)
		wg.Add(b.N)
		go func() {
			for range sub.Messages() {
				wg.Done()
			}
		}()
	}

	time.Sleep(time.Second)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		assert.NoError(b, broker.Publish("/foo/bar/gazonk", struct{}{}, time.Second))
	}
	wg.Wait()
	broker.Shutdown()
}
