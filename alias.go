package cloudeventsaws

import (
	"github.com/maiguangyang/cloudevents-aws-transport/eventbridge"
	"github.com/maiguangyang/cloudevents-aws-transport/sns"
	"github.com/maiguangyang/cloudevents-aws-transport/sqs"
)

var (
	// SQS
	NewSQSTransport = sqs.New

	// SNS
	NewSNSTransport = sns.New
	WithPort        = sns.WithPort
	WithPath        = sns.WithPath

	// EventBridge
	NewEventBridgeTransport = eventbridge.New
)
