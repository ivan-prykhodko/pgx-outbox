package outbox

import (
	"context"
	"fmt"
)

// Processor processes messages from the outbox.
//
//go:generate mockery
type Processor interface {
	Process(ctx context.Context, msg *Message) error
}

type defaultProcessor struct {
	dispatcher   Dispatcher
	acknowledger Acknowledger
}

func NewDefaultProcessor(dispatcher Dispatcher, acknowledger Acknowledger) Processor {
	return &defaultProcessor{
		dispatcher:   dispatcher,
		acknowledger: acknowledger,
	}
}

func (p *defaultProcessor) Process(ctx context.Context, msg *Message) error {
	var err error

	if err = p.dispatcher.Dispatch(ctx, msg); err != nil {
		if isRetryable(err) {
			return err
		}

		// TODO: retry strategy on serialization error?

		return p.acknowledger.Nack(ctx, msg.ID, err)
	}

	if err = p.acknowledger.Ack(ctx, msg.ID); err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	return nil
}
