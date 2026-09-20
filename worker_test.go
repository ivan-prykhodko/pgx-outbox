package outbox

import (
	"context"
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWorker_Run(t *testing.T) {
	ctx := t.Context()

	t.Run("successfully processes messages until channel closed", func(t *testing.T) {
		provider := newMockProvider(t)
		processor := newMockProcessor(t)
		w := NewWorker(provider, processor)

		deliveryCh := make(chan Delivery, 2)
		errCh := make(chan error, 1)

		provider.On("Provide", mock.Anything).Return((<-chan Delivery)(deliveryCh), (<-chan error)(errCh))

		msg1 := Message{ID: 1}
		msg2 := Message{ID: 2}

		deliveryCh <- Delivery{Message: msg1, Ack: func() {}}
		deliveryCh <- Delivery{Message: msg2, Ack: func() {}}
		close(deliveryCh)

		processor.On("Process", mock.Anything, mock.MatchedBy(func(d *Delivery) bool {
			return d.Message.ID == 1
		})).Return(nil)
		processor.On("Process", mock.Anything, mock.MatchedBy(func(d *Delivery) bool {
			return d.Message.ID == 2
		})).Return(nil)

		err := w.Run(ctx)
		assert.NoError(t, err)

		provider.AssertExpectations(t)
		processor.AssertExpectations(t)
	})

	t.Run("returns error when provider error channel signals", func(t *testing.T) {
		provider := newMockProvider(t)
		processor := newMockProcessor(t)
		w := NewWorker(provider, processor)

		deliveryCh := make(chan Delivery)
		errCh := make(chan error, 1)
		providerErr := errors.New("provider error")

		provider.On("Provide", mock.Anything).Return((<-chan Delivery)(deliveryCh), (<-chan error)(errCh))
		errCh <- providerErr

		err := w.Run(ctx)
		assert.ErrorIs(t, err, providerErr)
	})

	t.Run("returns error when processor fails", func(t *testing.T) {
		provider := newMockProvider(t)
		processor := newMockProcessor(t)
		w := NewWorker(provider, processor)

		deliveryCh := make(chan Delivery, 1)
		errCh := make(chan error, 1)
		processErr := errors.New("process error")

		provider.On("Provide", mock.Anything).Return((<-chan Delivery)(deliveryCh), (<-chan error)(errCh))

		msg := Message{ID: 1, EventType: "test"}
		deliveryCh <- Delivery{Message: msg, Ack: func() {}}

		processor.On("Process", mock.Anything, mock.Anything).Return(processErr)

		err := w.Run(ctx)
		assert.ErrorContains(t, err, "process message 1 of type test")
		assert.ErrorIs(t, err, processErr)
	})

	t.Run("stops when context is canceled", func(t *testing.T) {
		provider := newMockProvider(t)
		processor := newMockProcessor(t)
		w := NewWorker(provider, processor)

		cancelCtx, cancel := context.WithCancel(ctx)

		deliveryCh := make(chan Delivery)
		errCh := make(chan error)
		provider.On("Provide", cancelCtx).Return((<-chan Delivery)(deliveryCh), (<-chan error)(errCh))

		cancel()
		err := w.Run(cancelCtx)
		assert.NoError(t, err)
	})
}
