package outbox

import (
	"context"
	"fmt"
)

type Dispatcher interface {
	Dispatch(ctx context.Context, msg *Message) error
}

type dispatcher struct {
	publisher Publisher
	router    Router
}

func NewDispatcher(publisher Publisher, router Router) Dispatcher {
	return &dispatcher{
		publisher: publisher,
		router:    router,
	}
}

func (d *dispatcher) Dispatch(ctx context.Context, msg *Message) error {
	env, err := d.buildEnvelope(msg)
	if err != nil {
		return fmt.Errorf("build envelope: %w", err)
	}

	if err := d.publisher.Publish(ctx, env); err != nil {
		return fmt.Errorf(
			"dispatch message id=%d event=%s topic=%s: %w",
			msg.ID,
			msg.EventType,
			env.Topic,
			err,
		)
	}

	return nil
}

func (d *dispatcher) buildEnvelope(msg *Message) (Envelope, error) {
	route, err := d.router.Resolve(msg)
	if err != nil {
		return Envelope{}, fmt.Errorf("resolve route for message: %w", err)
	}

	return Envelope{
		Topic:          route.Topic,
		Key:            route.Key,
		Message:        *msg,
		IdempotencyKey: route.IdempotencyKey,
	}, nil
}
