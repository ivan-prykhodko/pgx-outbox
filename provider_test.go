package outbox

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestProvider_Provide(t *testing.T) {
	t.Run("context cancellation successfully cought", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())

		repo := newMockRepository(t)
		nl := newMockNotificationListener(t)
		p := NewProvider(repo, nl, time.Hour, 10)

		nl.On("WaitForNotification", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			nCtx := args.Get(0).(context.Context)
			<-nCtx.Done()
		}).Return().Maybe()

		repo.On("ClaimPending", mock.Anything, 10).Return(nil, nil).Once()

		deliveryCh, errCh := p.Provide(ctx)

		// Cancel context to stop provider
		cancel()

		select {
		case err := <-errCh:
			assert.ErrorIs(t, err, context.Canceled)
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for provider to exit")
		}

		// Channel should be closed
		_, ok := <-deliveryCh
		assert.False(t, ok)
	})

	t.Run("successfully drain and poll", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		repo := newMockRepository(t)
		nl := newMockNotificationListener(t)
		// Small polling interval for testing
		pollingInterval := time.Millisecond * 10
		p := NewProvider(repo, nl, pollingInterval, 2)

		msg1 := Message{ID: 1}
		msg2 := Message{ID: 2}
		msg3 := Message{ID: 3}

		nl.On("WaitForNotification", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

		// First drain call returns 2 messages
		repo.On("ClaimPending", mock.Anything, 2).Return([]Message{msg1, msg2}, nil).Once()
		// Second drain call returns 0, finishing drain
		repo.On("ClaimPending", mock.Anything, 2).Return(nil, nil).Once()
		// Then first poll (from timer) returns 1 message
		repo.On("ClaimPending", mock.Anything, 2).Return([]Message{msg3}, nil).Once()
		// Then return empty to wait again
		repo.On("ClaimPending", mock.Anything, 2).Return(nil, nil)

		deliveryCh, errCh := p.Provide(ctx)

		// Collect first 2 messages (from drain)
		d1 := <-deliveryCh
		assert.Equal(t, msg1.ID, d1.Message.ID)
		d1.Ack()

		d2 := <-deliveryCh
		assert.Equal(t, msg2.ID, d2.Message.ID)
		d2.Ack()

		// Collect 3rd message (from poll)
		d3 := <-deliveryCh
		assert.Equal(t, msg3.ID, d3.Message.ID)
		d3.Ack()

		// Verify no errors
		select {
		case err := <-errCh:
			t.Fatalf("unexpected error: %v", err)
		default:
		}

		cancel()
		<-errCh // wait for exit
	})

	t.Run("successfully notification trigger", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		repo := newMockRepository(t)
		nl := newMockNotificationListener(t)
		p := NewProvider(repo, nl, time.Hour, 10)

		notifyChSet := make(chan chan<- struct{}, 1)
		var nlDone sync.WaitGroup
		nlDone.Add(1)

		nl.On("WaitForNotification", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			defer nlDone.Done()
			notifyChSet <- args.Get(1).(chan<- struct{})
			nCtx := args.Get(0).(context.Context)
			<-nCtx.Done()
		}).Return()

		// Drain empty
		repo.On("ClaimPending", mock.Anything, 10).Return(nil, nil).Once()

		deliveryCh, _ := p.Provide(ctx)

		var notifyCh chan<- struct{}
		select {
		case notifyCh = <-notifyChSet:
		case <-ctx.Done():
			t.Fatal("context canceled while waiting for notifyCh to be set")
		}

		msg := Message{ID: 100}
		repo.On("ClaimPending", mock.Anything, 10).Return([]Message{msg}, nil).Once()
		repo.On("ClaimPending", mock.Anything, 10).Return(nil, nil)

		// Trigger notification
		notifyCh <- struct{}{}

		select {
		case d := <-deliveryCh:
			assert.Equal(t, msg.ID, d.Message.ID)
			d.Ack()
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for message triggered by notification")
		}

		cancel()
		nlDone.Wait()
	})

	t.Run("successfully ack synchronization", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		repo := newMockRepository(t)
		nl := newMockNotificationListener(t)
		p := NewProvider(repo, nl, time.Hour, 2)

		nl.On("WaitForNotification", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

		msg1 := Message{ID: 1}
		msg2 := Message{ID: 2}

		repo.On("ClaimPending", mock.Anything, 2).Return([]Message{msg1, msg2}, nil).Once()
		repo.On("ClaimPending", mock.Anything, 2).Return(nil, nil)

		deliveryCh, _ := p.Provide(ctx)

		d1 := <-deliveryCh
		d2 := <-deliveryCh

		// At this point, pollMessages is waiting for acks.
		// We can verify this by checking if a new ClaimPending call is made after we ack.

		ackDone := make(chan struct{})
		go func() {
			d1.Ack()
			d2.Ack()
			close(ackDone)
		}()

		select {
		case <-ackDone:
			// Success
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for acks")
		}
	})

	t.Run("repository error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		repo := newMockRepository(t)
		nl := newMockNotificationListener(t)
		p := NewProvider(repo, nl, time.Hour, 10)

		nl.On("WaitForNotification", mock.Anything, mock.Anything, mock.Anything).Return().Maybe()

		expectedErr := errors.New("db error")
		repo.On("ClaimPending", mock.Anything, 10).Return(nil, expectedErr).Once()

		_, errCh := p.Provide(ctx)

		select {
		case err := <-errCh:
			assert.ErrorContains(t, err, "db error")
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for error")
		}
	})

	t.Run("notification listener error", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		repo := newMockRepository(t)
		nl := newMockNotificationListener(t)
		p := NewProvider(repo, nl, time.Hour, 10)

		repo.On("ClaimPending", mock.Anything, 10).Return(nil, nil).Once()

		nl.On("WaitForNotification", mock.Anything, mock.Anything, mock.Anything).Run(func(args mock.Arguments) {
			errCh := args.Get(2).(chan<- error)
			errCh <- errors.New("notification error")
		}).Return()

		_, errCh := p.Provide(ctx)

		select {
		case err := <-errCh:
			assert.ErrorContains(t, err, "notification error")
		case <-time.After(time.Second):
			t.Fatal("timeout waiting for error")
		}
	})
}
