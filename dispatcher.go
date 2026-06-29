package outbox

import (
	"context"
	"fmt"
)

// Dispatcher handles message routing and publishing.
type Dispatcher interface {
	// Dispatch routes the message and publishes it using the configured publisher.
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
			"dispatch message id=%d event=%s route=%v: %w",
			msg.ID,
			msg.EventType,
			env.Route,
			err,
		)
	}

	return nil
}

// buildEnvelope resolves the route and wraps the message into an envelope.
func (d *dispatcher) buildEnvelope(msg *Message) (Envelope, error) {
	route, err := d.router.Resolve(msg)
	if err != nil {
		return Envelope{}, fmt.Errorf("resolve route for message: %w", err)
	}

	return Envelope{
		Route:   route,
		Message: *msg,
	}, nil
}
