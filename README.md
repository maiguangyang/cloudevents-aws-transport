# cloudevents-aws-transport

Publish/Receive CloudEvents messages using AWS SNS/SQS

# SNS Example

```
package main

import (
	"context"
	"fmt"
	"log"

	cloudevents "github.com/cloudevents/sdk-go"
)

func Receive(event cloudevents.Event) {
	log.Printf("? %v", event)
	// do something with event.Context and event.Data (via event.DataAs(foo)
}

func main() {
	t, err := NewSNSTransport("arn:aws:sns:eu-central-1:123456789:test")
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}
	c, err := cloudevents.NewClient(t)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

    event := cloudevents.NewEvent()
    event.SetID(fmt.Sprintf("test123 %d", i))
    event.SetType("com.cloudevents.readme.sent")
    event.SetSource("http://localhost:8080/")
    event.SetData(fmt.Sprintf("hello world %d", i))
    log.Println(c.Send(context.Background(), event))

	log.Fatal(c.StartReceiver(context.Background(), Receive))
}
```

# SQS Example

```
package main

import (
	"context"
	"fmt"
	"log"

	cloudevents "github.com/cloudevents/sdk-go"
)

func Receive(event cloudevents.Event) {
	log.Printf("? %v", event)
	// do something with event.Context and event.Data (via event.DataAs(foo)
}

func main() {
	t, err := NewSQSTransport("https://sqs.eu-central-1.amazonaws.com/123456789/sqs-queue-test")
	if err != nil {
		log.Fatalf("failed to create transport, %v", err)
	}
	c, err := cloudevents.NewClient(t)
	if err != nil {
		log.Fatalf("failed to create client, %v", err)
	}

    // send message
    event := cloudevents.NewEvent()
    event.SetID(fmt.Sprintf("test123 %d", i))
    event.SetType("com.cloudevents.readme.sent")
    event.SetSource("http://localhost:8080/")
    event.SetData(fmt.Sprintf("hello world %d", i))
    c.Send(context.Background(), event)

    // start receiver
	log.Fatal(c.StartReceiver(context.Background(), Receive))
}
```
