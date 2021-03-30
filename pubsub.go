package pubsub

import (
	"errors"
	"fmt"
	"strings"
	"sync"
)

// Subscriber registers interest/uninterest to a topic
//
// At the moment it doesn't support wildcars ('*' or '>')
type Subscriber interface {
	Subscribe(topic string) (<-chan interface{}, error)
	Unsubscribe(topic string, ch <-chan interface{}) error
}

// Publisher sends "something" to a grouped by topic bunch of channels
type Publisher interface {
	Publish(topic string, payload interface{}) error
}

// Stream is the interface to a publish / subscribe dispatcher
//
// it implements 1:N fanout channel mechanism, grouped by topic
type Stream interface {
	Subscriber
	Publisher
}

// New creates a Stream interface to publish / subscribe dispatcher
//
// it permits virtually unlimited subscriptions to unlimited subjects
func New() Stream {
	return &streamImpl{
		subscriptions: map[string][]chan interface{}{},
	}
}

type streamImpl struct {
	Stream

	subscriptions map[string][]chan interface{}

	mutex sync.RWMutex
}

// Subscribe registers the interest to a topic and returns a channel
func (sub *streamImpl) Subscribe(topic string) (<-chan interface{}, error) {
	// topic validation
	if err := validateTopic(topic); err != nil {
		return nil, err
	}
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	// check if already subscribed
	if _, ok := sub.subscriptions[topic]; !ok {
		sub.subscriptions[topic] = make([]chan interface{}, 0, 1)
	}
	// create new subscription
	ch := make(chan interface{}, 1)
	sub.subscriptions[topic] = append(sub.subscriptions[topic], ch)

	return ch, nil
}

func find(slice []chan interface{}, element <-chan interface{}) int {
	for i, e := range slice {
		if e == element {
			return i
		}
	}
	return -1
}

// Unsubscribe registers the uninterest to a topic, it closed the channel
func (sub *streamImpl) Unsubscribe(topic string, ch <-chan interface{}) error {
	sub.mutex.Lock()
	defer sub.mutex.Unlock()

	// check if subscribed
	chList, ok := sub.subscriptions[topic]
	if !ok {
		return errors.New(fmt.Sprint("topic not subscribed"))
	}
	// find in list
	pos := 0
	if pos = find(chList, ch); pos == -1 {
		return errors.New(fmt.Sprint("subscription not found"))
	}
	// close & remove
	close(chList[pos])

	if len(chList) <= 1 {
		delete(sub.subscriptions, topic)
	} else {
		// remove from slice
		sub.subscriptions[topic] = append(sub.subscriptions[topic][:pos], sub.subscriptions[topic][pos+1:]...)
	}
	return nil
}

// Publish sends "something" to a grouped by topic bunch of channels
func (pub *streamImpl) Publish(topic string, payload interface{}) error {
	pub.mutex.RLock()
	defer pub.mutex.RUnlock()

	// topic validation
	if err := validateTopic(topic); err != nil {
		return err
	}
	// check if subscribed
	if _, ok := pub.subscriptions[topic]; !ok {
		return errors.New(fmt.Sprint("topic not subscribed"))
	}
	// fanout payload to subscriptions
	for _, ch := range pub.subscriptions[topic] {
		ch <- payload
	}
	return nil
}

const (
	// wildcards for multiple subscriptions, not yet supported
	wildcards = "*>"
	// chars to be used
	validchrs = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz01234567890._"
)

// check the topic : must be composed by valid chars
func validateTopic(topic string) error {
	// check if empty
	if len(topic) == 0 {
		return errors.New(fmt.Sprint("topic empty"))
	}
	// check valid chars
	for _, c := range topic {
		if chr := string(c); !strings.Contains(validchrs, chr) {
			return errors.New(fmt.Sprintf("topic contains illegal char [%s]", chr))
		}
	}
	return nil
}
