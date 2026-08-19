package outbox

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestWorker_doProcess(t *testing.T) {
	ctx := t.Context()

	t.Run("successfully processes messages from channel", func(t *testing.T) {
		reader := newMockReader(t)
		processor := newMockProcessor(t)
		w := &worker{
			reader:    reader,
			processor: processor,
			logger:    slog.Default(),
		}

		msgCh := make(chan Message, 2)
		msg1 := Message{ID: 1}
		msg2 := Message{ID: 2}
		msgCh <- msg1
		msgCh <- msg2
		close(msgCh)

		reader.On("Read", ctx).Return((<-chan Message)(msgCh), nil)
		processor.On("Process", ctx, msg1).Return(nil)
		processor.On("Process", ctx, msg2).Return(nil)

		err := w.doProcess(ctx)

		assert.NoError(t, err)
		reader.AssertExpectations(t)
		processor.AssertExpectations(t)
	})

	t.Run("returns error if reader fails", func(t *testing.T) {
		reader := newMockReader(t)
		w := &worker{reader: reader}

		reader.On("Read", ctx).Return(nil, assert.AnError)

		err := w.doProcess(ctx)

		assert.ErrorIs(t, err, assert.AnError)
		assert.Contains(t, err.Error(), "read messages")
	})

	t.Run("returns error if processor fails", func(t *testing.T) {
		reader := newMockReader(t)
		processor := newMockProcessor(t)
		w := &worker{
			reader:    reader,
			processor: processor,
			logger:    slog.Default(),
		}

		msgCh := make(chan Message, 1)
		msg1 := Message{ID: 1}
		msgCh <- msg1
		close(msgCh)

		reader.On("Read", ctx).Return((<-chan Message)(msgCh), nil)
		processor.On("Process", ctx, msg1).Return(assert.AnError)

		err := w.doProcess(ctx)

		assert.ErrorIs(t, err, assert.AnError)
		assert.Contains(t, err.Error(), "process message")
	})
}

func TestWorker_Run(t *testing.T) {
	t.Run("stops when context is cancelled", func(t *testing.T) {
		reader := newMockReader(t)
		processor := newMockProcessor(t)
		// Use very long interval to avoid multiple ticks
		w := NewWorker(reader, processor, time.Hour, time.Millisecond, nil)

		ctx, cancel := context.WithCancel(t.Context())

		// Initial process call in Run
		msgCh := make(chan Message)
		close(msgCh)
		reader.On("Read", mock.Anything).Return((<-chan Message)(msgCh), nil)

		go func() {
			time.Sleep(50 * time.Millisecond)
			cancel()
		}()

		w.Run(ctx)
		// Should return after cancel
		reader.AssertExpectations(t)
	})
}
