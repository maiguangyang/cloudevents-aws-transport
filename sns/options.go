package sns

import (
	"fmt"
	"strings"
	"time"

	"github.com/maiguangyang/cloudevents-aws-transport/encoding"
)

// Option is the function signature required to be considered an nats.Option.
type Option func(*Transport) error

// WithEncoding sets the encoding for NATS transport.
func WithEncoding(encoding encoding.Encoding) Option {
	return func(t *Transport) error {
		t.Encoding = encoding
		return nil
	}
}

// WithShutdownTimeout sets the shutdown timeout when the http server is being shutdown.
func WithShutdownTimeout(timeout time.Duration) Option {
	return func(t *Transport) error {
		if t == nil {
			return fmt.Errorf("http shutdown timeout option can not set nil transport")
		}
		t.ShutdownTimeout = &timeout
		return nil
	}
}

// WithPort sets the listening port for StartReceiver.
func WithPort(port int) Option {
	return func(t *Transport) error {
		if t == nil {
			return fmt.Errorf("http port option can not set nil transport")
		}
		if port < 0 || port > 65535 {
			return fmt.Errorf("http port option was given an invalid port: %d", port)
		}
		t.Port = &port
		return nil
	}
}

// WithPath sets the path to receive cloudevents on for SNS transports.
func WithPath(path string) Option {
	return func(t *Transport) error {
		if t == nil {
			return fmt.Errorf("http path option can not set nil transport")
		}
		path = strings.TrimSpace(path)
		if len(path) == 0 {
			return fmt.Errorf("http path option was given an invalid path: %q", path)
		}
		t.Path = path
		return nil
	}
}
