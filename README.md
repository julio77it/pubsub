# pubsub
Simple topic based publish/subscribe package over Go's channels

[![Go Report Card](https://goreportcard.com/badge/github.com/julio77it/pubsub)](https://goreportcard.com/report/github.com/julio77it/pubsub)

```go
    stream := pubsub.New()

    ch, err := stream.Subscribe("A.B.C.D")

    // ...

    go func() {
        for payload := range ch {
            fmt.Println(payload)
        }
    }()

    // ...

    err := stream.Publish("A.B.C.D", "This is a text payload")
```