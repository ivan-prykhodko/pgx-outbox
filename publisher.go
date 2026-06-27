package outbox

import "context"

type Envelope struct {
	Topic          string
	Key            string
	Message        Message
	IdempotencyKey string
}

type Publisher interface {
	Publish(ctx context.Context, env Envelope) error
}
