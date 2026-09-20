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

		acked := false
		delivery := Delivery{
			Message: msg,
			Ack:     func() { acked = true },
		}
		err := p.Process(ctx, &delivery)

		assert.NoError(t, err)
		assert.True(t, acked)
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

		acked := false
		delivery := Delivery{
			Message: msg,
			Ack:     func() { acked = true },
		}
		err := p.Process(ctx, &delivery)

		assert.NoError(t, err) // Nack consumes the error and returns nil
		assert.True(t, acked)
		dispatcher.AssertExpectations(t)
		acknowledger.AssertExpectations(t)
	})

	t.Run("returns error on retryable error without marking failed", func(t *testing.T) {
		dispatcher := newMockDispatcher(t)
		acknowledger := newMockAcknowledger(t)
		p := NewDefaultProcessor(dispatcher, acknowledger)

		retryableErr := ErrNetwork
		dispatcher.On("Dispatch", ctx, &msg).Return(retryableErr)

		acked := false
		delivery := Delivery{
			Message: msg,
			Ack:     func() { acked = true },
		}
		err := p.Process(ctx, &delivery)

		assert.ErrorIs(t, err, retryableErr)
		assert.True(t, acked)
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

		acked := false
		delivery := Delivery{
			Message: msg,
			Ack:     func() { acked = true },
		}
		err := p.Process(ctx, &delivery)

		assert.ErrorIs(t, err, assert.AnError)
		assert.True(t, acked)
		assert.Contains(t, err.Error(), "mark published")
		dispatcher.AssertExpectations(t)
		acknowledger.AssertExpectations(t)
	})
}
