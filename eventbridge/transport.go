package eventbridge

import (
	"context"
	"fmt"
	"strings"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/transport"
	"github.com/maiguangyang/cloudevents-aws-transport/encoding"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	_eventbridge "github.com/aws/aws-sdk-go/service/eventbridge"
)

const (
	// TransportName is the name of this transport.
	TransportName = "AWS EventBridge"
)

// Transport adheres to transport.Transport.
var _ transport.Transport = (*Transport)(nil)

// Transport acts as both a http client and a http handler.
type Transport struct {
	Encoding encoding.Encoding
	Client   *_eventbridge.EventBridge
	BusName  string

	Receiver transport.Receiver
	// Converter is invoked if the incoming transport receives an undecodable
	// message.
	Converter transport.Converter

	codec transport.Codec
}

// New creates a new NATS transport.
func New(busARN string, opts ...Option) (*Transport, error) {
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		return nil, err
	}

	arnParts := strings.Split(busARN, "/")

	t := &Transport{
		Client:  _eventbridge.New(sess),
		BusName: arnParts[len(arnParts)-1],
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
		input := _eventbridge.PutEventsInput{
			Entries: []*_eventbridge.PutEventsRequestEntry{
				&_eventbridge.PutEventsRequestEntry{
					EventBusName: aws.String(t.BusName),
					Detail:       aws.String(string(m.Body)),
					DetailType:   aws.String(event.Type()),
					Source:       aws.String(event.Source()),
					Time:         aws.Time(event.Time()),
				},
			},
		}
		_, err := t.Client.PutEvents(&input)
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
	return fmt.Errorf("%s doesn't implement receiver", TransportName)
}
