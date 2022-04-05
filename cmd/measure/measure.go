package main

import (
	"crypto/rand"
	"errors"
	"flag"
	"fmt"
	"sync"
	"time"

	"github.com/julio77it/pubsub"
)

var (
	stream      pubsub.Stream
	statCh      chan Stat
	statistics  map[string][]time.Duration
	wgInit      sync.WaitGroup
	wgProd      sync.WaitGroup
	wgCons      sync.WaitGroup
	totalToSend int
	totalToRecv int
)

// Stat : per topic publish to subscribe elapsed time
type Stat struct {
	topic   string
	elapsed time.Duration
}

// Avg : elapsed time average
type Avg struct {
	avg  time.Duration
	msgs int
}

func init() {
	stream = pubsub.New()

	statCh = make(chan Stat, 1000000)
	statistics = make(map[string][]time.Duration)
}

func publishOnTopic(topic string) {
	_ = stream.Publish(topic, time.Now())
	wgProd.Done()
}

func subscribeTopic(topic string, nmsg int) {
	in, err := stream.Subscribe(topic)
	wgInit.Done()

	if err != nil {
		panic(err)
	}

	for i := 0; i < nmsg; i++ {
		msg, ok := <-in

		if !ok {
			panic(errors.New("subscriber : reading on topic " + topic))
		}
		start := msg.(time.Time)
		elapsed := time.Since(start)

		statCh <- Stat{
			topic:   topic,
			elapsed: elapsed,
		}
	}
	if err = stream.Unsubscribe(topic, in); err != nil {
		panic(err)
	}
}

func randomString(n uint) string {
	letters := []byte("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	slice := make([]byte, n)
	_, _ = rand.Read(slice)

	for i, b := range slice {
		slice[i] = letters[int(b)%len(letters)]
	}
	return string(slice)
}

func average(in []time.Duration) time.Duration {
	out := time.Duration(0)

	for _, val := range in {
		out += val
	}
	return out / time.Duration(len(in))
}

func main() {
	var topics, goro, msgs int
	flag.IntVar(&topics, "topics", 1, "num topic to subscribe")
	flag.IntVar(&goro, "goro", 1, "num goroutines/subscriptions for every topic")
	flag.IntVar(&msgs, "msgs", 1, "num messages for every topic")
	flag.Parse()

	totalToSend = topics * msgs
	totalToRecv = totalToSend * goro
	topicList := make([]string, 0, topics)

	wgInit.Add(topics * goro)
	// init & start subscribers
	fmt.Printf("Starting %d goroutines\n", topics*goro)
	for t := 0; t < topics; t++ {
		topicList = append(topicList, randomString(16))

		for g := 0; g < goro; g++ {
			go subscribeTopic(topicList[t], msgs)
		}
	}
	wgInit.Wait()

	wgProd.Add(totalToSend)
	// publish
	fmt.Printf("Publishing %d messages\n", totalToSend)
	for _, topic := range topicList {
		for m := 0; m < msgs; m++ {
			go publishOnTopic(topic)
		}
	}
	wgProd.Wait()

	fmt.Printf("Collecting %d messages\n", totalToRecv)

	// collect dispatch times
	for i := 0; i < totalToRecv; i++ {
		stat, ok := <-statCh
		if !ok {
			break
		}
		_, ok = statistics[stat.topic]
		if !ok {
			statistics[stat.topic] = make([]time.Duration, 0, 1000000)
		}
		statistics[stat.topic] = append(statistics[stat.topic], stat.elapsed)
	}
	avg := make(map[string]Avg)

	for topic, reads := range statistics {
		avg[topic] = Avg{avg: average(reads), msgs: len(reads)}
	}
	// print statics
	for k, v := range avg {
		fmt.Printf("%s [%d] : %v\n", k, v.msgs, v.avg)
	}
}
