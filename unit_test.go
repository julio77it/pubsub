package pubsub

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestValidateTopic(t *testing.T) {
	table := map[string]bool{
		"A.B.C":       true,
		"A.B.C.D.E.F": true,
		"C.B.A":       true,
		"1.2.3":       true,
		"A.123.C":     true,
		"A,B,C":       false,
		"a.b.c.d":     true,
		"£$%&/()":     false,
		"ABCD":        true,
		"":            false,
	}

	for test, result := range table {
		if result {
			assert.NoError(t, validateTopic(test))
		} else {
			assert.Error(t, validateTopic(test))
		}
	}
}
func TestSubscribe(t *testing.T) {
	stream := New()

	_, err := stream.Subscribe("ABC###")
	assert.Error(t, err)

	ch1, err := stream.Subscribe("ABC.DEF")
	assert.NoError(t, err)

	ch2, err := stream.Subscribe("ABC.DEF")
	assert.NoError(t, err)

	err = stream.Unsubscribe("ABC.DEF.BOH", ch1)
	assert.Error(t, err)

	err = stream.Unsubscribe("ABC.DEF", nil)
	assert.Error(t, err)

	err = stream.Unsubscribe("ABC.DEF", ch1)
	assert.NoError(t, err)

	err = stream.Unsubscribe("ABC.DEF", ch2)
	assert.NoError(t, err)
}

func TestPublish(t *testing.T) {
	stream := New()

	err := stream.Publish("ABC###", "abcdef")
	assert.Error(t, err)

	err = stream.Publish("ABC.DEF", "abcdef")
	assert.Error(t, err)

	ch, err := stream.Subscribe("ABC.DEF")
	assert.NoError(t, err)

	err = stream.Publish("ABC.DEF", "abcdef")
	assert.NoError(t, err)

	err = stream.Unsubscribe("ABC.DEF", ch)
	assert.NoError(t, err)

	err = stream.Publish("ABC.DEF", "abcdef")
	assert.Error(t, err)
}

func collect(stats map[string]int, wg *sync.WaitGroup) chan string {
	ch := make(chan string, 10000000)

	go func() {
		wg.Add(1)
		for {
			select {
			case occ, ok := <-ch:
				if !ok {
					wg.Done()
					return
				}
				stats[occ] = stats[occ] + 1
			}
		}
	}()

	return ch
}
func consume(topic string, ch <-chan interface{}, stats chan<- string, occur int, wg *sync.WaitGroup) {
	for i := 0; i < occur; i++ {
		<-ch
		stats <- topic
	}
	wg.Done()
}

type singleTest struct {
	nmsg int
	ngo  int
}

func test(t *testing.T, table map[string]singleTest) {
	wgc := sync.WaitGroup{}

	stats := map[string]int{}
	coll := collect(stats, &wgc)

	stream := New()

	wgp := sync.WaitGroup{}

	start := time.Now()
	for payload, test := range table {
		for i := 0; i < test.ngo; i++ {
			ch, err := stream.Subscribe(payload)
			assert.NoError(t, err)
			wgp.Add(1)
			go consume(payload, ch, coll, test.nmsg, &wgp)
		}
		time.Sleep(time.Millisecond * 2)
		for i := 0; i < test.nmsg; i++ {
			err := stream.Publish(payload, payload)
			assert.NoError(t, err)
		}
	}
	wgp.Wait()

	elapsed := time.Since(start)

	close(coll)

	wgc.Wait()

	for subscription, occurences := range stats {
		test, ok := table[subscription]

		assert.True(t, ok)
		assert.Equal(t, test.ngo*test.nmsg, occurences)
		fmt.Printf("%s Goro=%d Msgs=%d Processed=%d\n", subscription, test.ngo, test.nmsg, occurences)
	}
	fmt.Printf("Took %s ---------------------\n\n", elapsed)
}

func TestDispatch(t *testing.T) {

	test(t, map[string]singleTest{
		"A.B.C0": {nmsg: 10, ngo: 10},
		"A.B.C1": {nmsg: 10, ngo: 10},
		"A.B.C2": {nmsg: 10, ngo: 10},
		"A.B.C3": {nmsg: 10, ngo: 10},
		"A.B.C4": {nmsg: 10, ngo: 10},
		"A.B.C5": {nmsg: 10, ngo: 10},
		"A.B.C6": {nmsg: 10, ngo: 10},
		"A.B.C7": {nmsg: 10, ngo: 10},
		"A.B.C8": {nmsg: 10, ngo: 10},
		"A.B.C9": {nmsg: 10, ngo: 10},
	})

}
