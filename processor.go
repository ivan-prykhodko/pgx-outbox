package outbox

import (
	"context"
	"fmt"
)

type Processor interface {
	Process(ctx context.Context, msg *Message) error
}

type defaultProcessor struct {
	repo       Repository
	dispatcher Dispatcher
}

func NewDefaultProcessor(repo Repository, dispatcher Dispatcher) Processor {
	return &defaultProcessor{
		repo:       repo,
		dispatcher: dispatcher,
	}
}

func (p *defaultProcessor) Process(ctx context.Context, msg *Message) error {
	var err error

	if err = p.dispatcher.Dispatch(ctx, msg); err != nil {
		if isRetryable(err) {
			return err
		}

		// TODO: retry strategy on serialization error?

		return p.repo.MarkFailed(ctx, msg.ID, err)
	}

	if err = p.repo.MarkPublished(ctx, msg.ID); err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	return nil
}
