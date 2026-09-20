package outbox

import "context"

//go:generate mockery
type Acknowledger interface {
	// Ack marks a message as successfully published.
	Ack(ctx context.Context, id int64) error
	// Nack marks a message as failed with an error.
	Nack(ctx context.Context, id int64, errMsg error) error
}
