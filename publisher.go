package outbox

import "context"

// Envelope wraps a message with its routing information.
type Envelope struct {
	Route
	Message
}

// Publisher defines the interface for sending messages to external systems.
type Publisher interface {
	// Publish sends the envelope to the destination.
	Publish(ctx context.Context, env Envelope) error
}
