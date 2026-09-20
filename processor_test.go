package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestDefaultProcessor_Process(t *testing.T) {
	ctx := t.Context()
	msg := Message{ID: 1}

	t.Run("successfully processes and marks as published", func(t *testing.T) {
		dispatcher := newMockDispatcher(t)
		acknowledger := newMockAcknowledger(t)
		p := NewDefaultProcessor(dispatcher, acknowledger)

		dispatcher.On("Dispatch", ctx, &msg).Return(nil)
		acknowledger.On("Ack", ctx, msg.ID).Return(nil)

		err := p.Process(ctx, &msg)

		assert.NoError(t, err)
		dispatcher.AssertExpectations(t)
		acknowledger.AssertExpectations(t)
	})

	t.Run("marks as failed on non-retryable error", func(t *testing.T) {
		dispatcher := newMockDispatcher(t)
		acknowledger := newMockAcknowledger(t)
		p := NewDefaultProcessor(dispatcher, acknowledger)

		dispatchErr := assert.AnError
		dispatcher.On("Dispatch", ctx, &msg).Return(dispatchErr)
		acknowledger.On("Nack", ctx, msg.ID, dispatchErr).Return(nil)

		err := p.Process(ctx, &msg)

		assert.NoError(t, err) // Nack consumes the error and returns nil
		dispatcher.AssertExpectations(t)
		acknowledger.AssertExpectations(t)
	})

	t.Run("returns error on retryable error without marking failed", func(t *testing.T) {
		dispatcher := newMockDispatcher(t)
		acknowledger := newMockAcknowledger(t)
		p := NewDefaultProcessor(dispatcher, acknowledger)

		retryableErr := ErrNetwork
		dispatcher.On("Dispatch", ctx, &msg).Return(retryableErr)

		err := p.Process(ctx, &msg)

		assert.ErrorIs(t, err, retryableErr)
		dispatcher.AssertExpectations(t)
		acknowledger.AssertNotCalled(t, "Nack", mock.Anything, mock.Anything, mock.Anything)
		acknowledger.AssertNotCalled(t, "Ack", mock.Anything, mock.Anything)
	})

	t.Run("returns error if mark published fails", func(t *testing.T) {
		dispatcher := newMockDispatcher(t)
		acknowledger := newMockAcknowledger(t)
		p := NewDefaultProcessor(dispatcher, acknowledger)

		dispatcher.On("Dispatch", ctx, &msg).Return(nil)
		acknowledger.On("Ack", ctx, msg.ID).Return(assert.AnError)

		err := p.Process(ctx, &msg)

		assert.ErrorIs(t, err, assert.AnError)
		assert.Contains(t, err.Error(), "mark published")
		dispatcher.AssertExpectations(t)
		acknowledger.AssertExpectations(t)
	})
}
