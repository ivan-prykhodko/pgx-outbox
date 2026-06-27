package outbox

import "context"

// Envelope wraps a message with its routing information.
type Envelope struct {
	Topic          string
	Key            string
	Message        Message
	IdempotencyKey string
}

// Publisher defines the interface for sending messages to external systems.
type Publisher interface {
	// Publish sends the envelope to the destination topic.
	Publish(ctx context.Context, env Envelope) error
}
