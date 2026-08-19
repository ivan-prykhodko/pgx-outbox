package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDispatcher_Dispatch(t *testing.T) {
	ctx := t.Context()
	msg := Message{ID: 1, AggregateType: "order", EventType: "created"}
	route := mockRoute{data: map[string]any{"target": "queue1"}}
	env := Envelope{Route: route, Message: msg}

	t.Run("successfully dispatches message", func(t *testing.T) {
		publisher := newMockPublisher(t)
		router := newMockRouter(t)
		d := NewDispatcher(publisher, router)

		router.On("Resolve", msg).Return(route, nil)
		publisher.On("Publish", ctx, env).Return(nil)

		err := d.Dispatch(ctx, msg)

		assert.NoError(t, err)
		router.AssertExpectations(t)
		publisher.AssertExpectations(t)
	})

	t.Run("returns error if router fails", func(t *testing.T) {
		publisher := newMockPublisher(t)
		router := newMockRouter(t)
		d := NewDispatcher(publisher, router)

		router.On("Resolve", msg).Return(nil, assert.AnError)

		err := d.Dispatch(ctx, msg)

		assert.ErrorIs(t, err, assert.AnError)
		assert.Contains(t, err.Error(), "resolve route")
		router.AssertExpectations(t)
	})

	t.Run("returns error if publisher fails", func(t *testing.T) {
		publisher := newMockPublisher(t)
		router := newMockRouter(t)
		d := NewDispatcher(publisher, router)

		router.On("Resolve", msg).Return(route, nil)
		publisher.On("Publish", ctx, env).Return(assert.AnError)

		err := d.Dispatch(ctx, msg)

		assert.ErrorIs(t, err, assert.AnError)
		assert.Contains(t, err.Error(), "dispatch message")
		router.AssertExpectations(t)
		publisher.AssertExpectations(t)
	})
}
