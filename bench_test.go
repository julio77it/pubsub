package pubsub

import (
	"math/rand"
	"sync"
	"testing"
)

var result bool

type Test struct {
	topic      string
	payload    string
	goroutines uint
	messages   uint
}

func randomString(n uint) string {
	letters := []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ")

	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}

// for this subscription, # listening goroutines, # messages dispatched for every goroutine, message size
func createTest(goroutines uint, messages uint, payloadSize uint) Test {
	return Test{
		topic:      randomString(16), // subject size : 16 bytes
		payload:    randomString(payloadSize),
		goroutines: goroutines,
		messages:   messages,
	}
}

// # subscriptions to test (every subscription same test size)
func createTableTest(subscriptions uint, goroutines uint, messages uint, payloadSize uint) []Test {
	s := make([]Test, subscriptions, subscriptions)

	for i := uint(0); i < subscriptions; i++ {
		s[i] = createTest(goroutines, messages, payloadSize)
	}
	return s
}

func runTest(plan []Test) bool {
	consumer := func(stream Stream, topic string, nmsg uint, wgs *sync.WaitGroup, wgd *sync.WaitGroup) {
		ch, err := stream.Subscribe(topic)
		wgs.Done()

		if err != nil {
			panic(err)
		}
		for i := uint(0); i < nmsg; i++ {
			<-ch
			wgd.Done()
		}
	}
	producer := func(stream Stream, topic string, payload string, nmsg uint, wg *sync.WaitGroup) {
		for i := uint(0); i < nmsg; i++ {

			if err := stream.Publish(topic, payload); err != nil {

			}
		}
		wg.Done()
	}
	wgc := &sync.WaitGroup{}
	wgp := &sync.WaitGroup{}
	wgd := &sync.WaitGroup{}

	stream := New()

	wgc.Add(int(plan[0].goroutines) * len(plan))
	wgd.Add(int(plan[0].goroutines*plan[0].messages) * len(plan))
	for _, test := range plan {
		for i := uint(0); i < test.goroutines; i++ {
			go consumer(stream, test.topic, test.messages, wgc, wgd)
		}
	}
	wgc.Wait()

	wgp.Add(len(plan))
	for _, test := range plan {
		go producer(stream, test.topic, test.payload, test.messages, wgp)
	}
	wgp.Wait()
	wgd.Wait()

	return true
}

func BenchmarkTopic1Go10Msg10Bytes64(b *testing.B) {
	tbl := createTableTest(1, 10, 10, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic1Go10Msg100Bytes64(b *testing.B) {
	tbl := createTableTest(1, 10, 100, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic1Go10Msg1000Bytes64(b *testing.B) {
	tbl := createTableTest(1, 10, 1000, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic10Go10Msg10Bytes64(b *testing.B) {
	tbl := createTableTest(10, 10, 10, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic10Go10Msg100Bytes64(b *testing.B) {
	tbl := createTableTest(10, 10, 100, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic10Go10Msg1000Bytes64(b *testing.B) {
	tbl := createTableTest(10, 10, 1000, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic10Go100Msg10Bytes64(b *testing.B) {
	tbl := createTableTest(10, 100, 10, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic10Go100Msg100Bytes64(b *testing.B) {
	tbl := createTableTest(10, 100, 100, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic10Go100Msg1000Bytes64(b *testing.B) {
	tbl := createTableTest(10, 100, 1000, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}
func BenchmarkTopic100Go100Msg10Bytes64(b *testing.B) {
	tbl := createTableTest(100, 100, 10, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic100Go100Msg100Bytes64(b *testing.B) {
	tbl := createTableTest(100, 100, 100, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}

func BenchmarkTopic100Go100Msg1000Bytes64(b *testing.B) {
	tbl := createTableTest(100, 100, 1000, 64)
	var r bool

	for n := 0; n < b.N; n++ {
		r = runTest(tbl)
	}
	result = r
}
