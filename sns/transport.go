package sns

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/transport"
	"github.com/maiguangyang/cloudevents-aws-transport/encoding"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	_sns "github.com/aws/aws-sdk-go/service/sns"
	context2 "github.com/cloudevents/sdk-go/pkg/cloudevents/context"
	"github.com/deadmanssnitch/snshttp"
	"go.uber.org/zap"
)

const (
	// DefaultShutdownTimeout defines the default timeout given to the http.Server when calling Shutdown.
	DefaultShutdownTimeout = time.Minute * 1

	// TransportName is the name of this transport.
	TransportName = "AWS SNS"
)

// Transport adheres to transport.Transport.
var _ transport.Transport = (*Transport)(nil)

// Transport acts as both a http client and a http handler.
type Transport struct {
	snshttp.DefaultHandler

	Encoding encoding.Encoding
	Client   *_sns.SNS
	TopicARN string

	// Port is the port to bind the receiver to. Defaults to 8080.
	Port *int
	// Path is the path to bind the receiver to. Defaults to "/".
	Path string
	// Handler is the handler the http Server will use. Use this to reuse the
	// http server. If nil, the Transport will create a one.
	Handler *http.ServeMux
	// ShutdownTimeout defines the timeout given to the http.Server when calling Shutdown.
	// If nil, DefaultShutdownTimeout is used.
	ShutdownTimeout *time.Duration

	Receiver transport.Receiver
	// Converter is invoked if the incoming transport receives an undecodable
	// message.
	Converter transport.Converter

	codec transport.Codec
}

// New creates a new NATS transport.
func New(topicARN string, opts ...Option) (*Transport, error) {
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		return nil, err
	}
	t := &Transport{
		Client:   _sns.New(sess),
		TopicARN: topicARN,
	}
	if err := t.applyOptions(opts...); err != nil {
		return nil, err
	}

	return t, nil
}

func (t *Transport) applyOptions(opts ...Option) error {
	for _, fn := range opts {
		if err := fn(t); err != nil {
			return err
		}
	}
	return nil
}

func (t *Transport) loadCodec() bool {
	if t.codec == nil {
		switch t.Encoding {
		case encoding.Default:
			t.codec = &encoding.Codec{TransportName: TransportName}
		case encoding.StructuredV02:
			t.codec = &encoding.CodecV02{Encoding: t.Encoding, TransportName: TransportName}
		case encoding.StructuredV03:
			t.codec = &encoding.CodecV03{Encoding: t.Encoding, TransportName: TransportName}
		case encoding.StructuredV1:
			t.codec = &encoding.CodecV1{Encoding: t.Encoding, TransportName: TransportName}
		default:
			return false
		}
	}
	return true
}

// Send implements Transport.Send
func (t *Transport) Send(ctx context.Context, event cloudevents.Event) (context.Context, *cloudevents.Event, error) {
	// TODO populate response context properly.
	if ok := t.loadCodec(); !ok {
		return ctx, nil, fmt.Errorf("unknown encoding set on transport: %d", t.Encoding)
	}

	msg, err := t.codec.Encode(ctx, event)
	if err != nil {
		return ctx, nil, err
	}

	if m, ok := msg.(*encoding.Message); ok {
		input := _sns.PublishInput{
			Message:  aws.String(string(m.Body)),
			TopicArn: aws.String(t.TopicARN),
		}
		_, err := t.Client.Publish(&input)
		return ctx, nil, err
	}

	return ctx, nil, fmt.Errorf("failed to encode Event into a Message")
}

// SetReceiver implements Transport.SetReceiver
func (t *Transport) SetReceiver(r transport.Receiver) {
	t.Receiver = r
}

// SetConverter implements Transport.SetConverter
func (t *Transport) SetConverter(c transport.Converter) {
	t.Converter = c
}

// HasConverter implements Transport.HasConverter
func (t *Transport) HasConverter() bool {
	return t.Converter != nil
}

// HasTracePropagation implements Transport.HasTracePropagation
func (t *Transport) HasTracePropagation() bool {
	return false
}

// StartReceiver implements Transport.StartReceiver
func (t *Transport) StartReceiver(ctx context.Context) (err error) {

	if ok := t.loadCodec(); !ok {
		return fmt.Errorf("failed to load coded")
	}

	if t.Handler == nil {
		t.Handler = http.NewServeMux()

		snsHandler := snshttp.New(t)
		t.Handler.Handle(t.GetPath(), snsHandler)
	}

	addr := fmt.Sprintf(":%d", t.GetPort())
	server := &http.Server{Addr: addr, Handler: t.Handler}

	go func() {
		log.Printf("starting receiver %s%s", addr, t.GetPath())
		log.Fatal(server.ListenAndServe())
	}()

	// wait for the server to return or ctx.Done().
	select {
	case <-ctx.Done():
		// Try a gracefully shutdown.
		timeout := DefaultShutdownTimeout
		if t.ShutdownTimeout != nil {
			timeout = *t.ShutdownTimeout
		}
		ctx, cancel := context.WithTimeout(context.Background(), timeout)
		defer cancel()
		return server.Shutdown(ctx)
	}
}

// Notification is called for messages published to the SNS Topic. When using
// DefaultHandler as above this is the only event you need to implement.
func (t *Transport) Notification(ctx context.Context, notif *snshttp.Notification) error {
	logger := context2.LoggerFrom(ctx)

	msg := &encoding.Message{
		Body: []byte(notif.Message),
	}
	event, err := t.codec.Decode(ctx, msg)
	if err != nil {
		logger.Errorw("failed to decode message", zap.Error(err))
		return err
	}

	if err := t.Receiver.Receive(context.TODO(), *event, nil); err != nil {
		logger.Warnw("SNS receiver return err", zap.Error(err))
		return err
	}

	return nil
}

func (t *Transport) GetPath() string {
	path := strings.TrimSpace(t.Path)
	if len(path) > 0 {
		return path
	}
	return "/" // default
}

func (t *Transport) GetPort() int {
	if t.Port == nil {
		return 8080
	}
	return *t.Port
}
