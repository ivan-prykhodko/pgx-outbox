package outbox

import (
	"context"
	"fmt"
)

// Worker retrieves messages and dispatches them to their destinations.
type Worker interface {
	Run(ctx context.Context) error
}

type worker struct {
	provider  Provider
	processor Processor
}

func NewWorker(provider Provider, processor Processor) Worker {
	return &worker{
		provider:  provider,
		processor: processor,
	}
}

// Run starts the worker loop until whether the context is canceled or an error occurs.
func (w *worker) Run(ctx context.Context) error {
	deliveryCh, errCh := w.provider.Provide(ctx)

	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveryCh:
			if !ok {
				return nil
			}
			if err := w.process(ctx, &delivery); err != nil {
				return err
			}
		case err := <-errCh:
			return err
		}
	}
}

func (w *worker) process(ctx context.Context, delivery *Delivery) error {
	if err := w.processor.Process(ctx, delivery); err != nil {
		return fmt.Errorf("process message %d of type %s: %w", delivery.Message.ID, delivery.Message.EventType, err)
	}

	return nil
}
