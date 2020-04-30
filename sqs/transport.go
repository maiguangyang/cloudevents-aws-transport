package sqs

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cloudevents/sdk-go/pkg/cloudevents"
	"github.com/cloudevents/sdk-go/pkg/cloudevents/transport"
	"github.com/maiguangyang/cloudevents-aws-transport/encoding"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/session"
	_sqs "github.com/aws/aws-sdk-go/service/sqs"
	context2 "github.com/cloudevents/sdk-go/pkg/cloudevents/context"
	"go.uber.org/zap"
)

const (
	// TransportName is the name of this transport.
	TransportName = "AWS SNS"
)

// Transport adheres to transport.Transport.
var _ transport.Transport = (*Transport)(nil)

// Transport acts as both a http client and a http handler.
type Transport struct {
	Encoding encoding.Encoding
	Client   *_sqs.SQS
	QueueURL string

	Receiver transport.Receiver
	// Converter is invoked if the incoming transport receives an undecodable
	// message.
	Converter transport.Converter

	codec transport.Codec
}

// New creates a new NATS transport.
func New(queueURL string, opts ...Option) (*Transport, error) {
	sess, err := session.NewSession(&aws.Config{})
	if err != nil {
		return nil, err
	}
	t := &Transport{
		Client:   _sqs.New(sess),
		QueueURL: queueURL,
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
		input := _sqs.SendMessageInput{
			MessageBody: aws.String(string(m.Body)),
			QueueUrl:    aws.String(t.QueueURL),
		}
		_, err := t.Client.SendMessage(&input)
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
// NOTE: This is a blocking call.
func (t *Transport) StartReceiver(ctx context.Context) (err error) {

	if ok := t.loadCodec(); !ok {
		return fmt.Errorf("failed to load coded")
	}

	fmt.Println("start receiver", t.QueueURL)
	for {
		select {
		case <-ctx.Done():
			break
		default:
			err := t.receiverLoop(ctx)
			if err != nil {
				return err
			}
		}
	}

}
func (t *Transport) receiverLoop(ctx context.Context) (err error) {
	logger := context2.LoggerFrom(ctx)

	m, err := t.Client.ReceiveMessage(&_sqs.ReceiveMessageInput{
		QueueUrl:            aws.String(t.QueueURL),
		MaxNumberOfMessages: aws.Int64(1),
		VisibilityTimeout:   aws.Int64(20), // 20 seconds
		WaitTimeSeconds:     aws.Int64(20),
	})
	if err != nil {
		return err
	}

	for _, message := range m.Messages {
		if message.Body == nil {
			continue
		}
		body := []byte(*message.Body)

		// handle case when event is published using SNS topic
		var snsMessage encoding.SNSMessage
		err := json.Unmarshal(body, &snsMessage)
		if err == nil && snsMessage.IsNotification() {
			body = []byte(snsMessage.Message)
		}

		msg := &encoding.Message{
			Body: body,
		}
		event, err := t.codec.Decode(ctx, msg)
		if err != nil {
			logger.Errorw("failed to decode message", zap.Error(err))
			continue
		}

		if err := t.Receiver.Receive(context.TODO(), *event, nil); err != nil {
			logger.Warnw("sqs receiver return err", zap.Error(err))
			continue
		}

		_, err = t.Client.DeleteMessage(&_sqs.DeleteMessageInput{
			QueueUrl:      &t.QueueURL,
			ReceiptHandle: message.ReceiptHandle,
		})
	}
	return nil
}
