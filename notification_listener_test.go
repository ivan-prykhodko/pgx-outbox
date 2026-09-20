package outbox

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNotificationListener_WaitForNotification(t *testing.T) {
	t.Run("successfully receive notification", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		connector := newMocknlConnector(t)
		conn := newMocknlConnection(t)
		retryPolicy := newMockRetryPolicy(t)

		config := &pgx.ConnConfig{}
		l := &notificationListener{
			connConfig:     config,
			listenChannel:  "test_channel",
			waitingTimeout: time.Second,
			closingTimeout: time.Second,
			retryPolicy:    retryPolicy,
			connector:      connector,
		}

		notifyCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		retryPolicy.On("ShouldRetry", 0).Return(true)
		connector.On("ConnectConfig", mock.Anything, config).Return(conn, nil)
		conn.On("Exec", mock.Anything, "LISTEN \"test_channel\"").Return(pgconn.CommandTag{}, nil)
		conn.On("Close", mock.Anything).Return(nil)

		// First call returns notification
		conn.On("WaitForNotification", mock.Anything).Return(&pgconn.Notification{}, nil).Once()
		// Second call blocks until context is canceled
		conn.On("WaitForNotification", mock.Anything).Run(func(args mock.Arguments) {
			<-ctx.Done()
		}).Return(nil, context.Canceled)

		done := make(chan struct{})
		go func() {
			defer close(done)
			l.WaitForNotification(ctx, notifyCh, errCh)
		}()

		select {
		case <-notifyCh:
			// Success
		case err := <-errCh:
			t.Fatalf("unexpected error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for notification: %v", ctx.Err())
		}

		cancel()
		<-done

		connector.AssertExpectations(t)
		conn.AssertExpectations(t)
		retryPolicy.AssertExpectations(t)
	})

	t.Run("successfully reconnection", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		connector := newMocknlConnector(t)
		conn := newMocknlConnection(t)
		retryPolicy := newMockRetryPolicy(t)

		config := &pgx.ConnConfig{}
		l := &notificationListener{
			connConfig:     config,
			listenChannel:  "test_channel",
			waitingTimeout: time.Millisecond * 10,
			closingTimeout: time.Second,
			retryPolicy:    retryPolicy,
			connector:      connector,
		}

		notifyCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		// First connection fails
		retryPolicy.On("ShouldRetry", 0).Return(true).Once()
		connector.On("ConnectConfig", mock.Anything, config).Return(nil, errors.New("connection failed")).Once()

		// Retry
		retryPolicy.On("ShouldRetry", 1).Return(true).Once()
		retryPolicy.On("NextRetry", 0).Return(time.Now().Add(-time.Second)).Once()

		// Second connection succeeds
		connector.On("ConnectConfig", mock.Anything, config).Return(conn, nil).Once()
		conn.On("Exec", mock.Anything, "LISTEN \"test_channel\"").Return(pgconn.CommandTag{}, nil)
		conn.On("Close", mock.Anything).Return(nil)

		conn.On("WaitForNotification", mock.Anything).Return(&pgconn.Notification{}, nil).Once()
		conn.On("WaitForNotification", mock.Anything).Run(func(args mock.Arguments) {
			<-ctx.Done()
		}).Return(nil, context.Canceled)

		done := make(chan struct{})
		go func() {
			defer close(done)
			l.WaitForNotification(ctx, notifyCh, errCh)
		}()

		select {
		case <-notifyCh:
			// Success
		case err := <-errCh:
			t.Fatalf("unexpected error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for notification: %v", ctx.Err())
		}

		cancel()
		<-done

		connector.AssertExpectations(t)
		conn.AssertExpectations(t)
		retryPolicy.AssertExpectations(t)
	})

	t.Run("successfully handle disconnection during waiting", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		connector := newMocknlConnector(t)
		conn1 := newMocknlConnection(t)
		conn2 := newMocknlConnection(t)
		retryPolicy := newMockRetryPolicy(t)

		config := &pgx.ConnConfig{}
		l := &notificationListener{
			connConfig:     config,
			listenChannel:  "test_channel",
			waitingTimeout: time.Millisecond * 10,
			closingTimeout: time.Second,
			retryPolicy:    retryPolicy,
			connector:      connector,
		}

		notifyCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		// First connection
		retryPolicy.On("ShouldRetry", 0).Return(true)
		connector.On("ConnectConfig", mock.Anything, config).Return(conn1, nil).Once()
		conn1.On("Exec", mock.Anything, "LISTEN \"test_channel\"").Return(pgconn.CommandTag{}, nil)
		conn1.On("WaitForNotification", mock.Anything).Return(nil, errors.New("connection lost")).Once()
		conn1.On("Close", mock.Anything).Return(nil).Once()

		// Reconnect
		retryPolicy.On("ShouldRetry", 0).Return(true)
		connector.On("ConnectConfig", mock.Anything, config).Return(conn2, nil).Once()
		conn2.On("Exec", mock.Anything, "LISTEN \"test_channel\"").Return(pgconn.CommandTag{}, nil)
		conn2.On("WaitForNotification", mock.Anything).Return(&pgconn.Notification{}, nil).Once()
		conn2.On("WaitForNotification", mock.Anything).Run(func(args mock.Arguments) {
			<-ctx.Done()
		}).Return(nil, context.Canceled)
		conn2.On("Close", mock.Anything).Return(nil).Once()

		done := make(chan struct{})
		go func() {
			defer close(done)
			l.WaitForNotification(ctx, notifyCh, errCh)
		}()

		select {
		case <-notifyCh:
			// Success
		case err := <-errCh:
			t.Fatalf("unexpected error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for notification: %v", ctx.Err())
		}

		cancel()
		<-done

		connector.AssertExpectations(t)
		conn1.AssertExpectations(t)
		conn2.AssertExpectations(t)
		retryPolicy.AssertExpectations(t)
	})

	t.Run("reconnect attempts exhausted", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		connector := newMocknlConnector(t)
		retryPolicy := newMockRetryPolicy(t)

		config := &pgx.ConnConfig{}
		l := &notificationListener{
			connConfig:     config,
			listenChannel:  "test_channel",
			waitingTimeout: time.Millisecond * 10,
			closingTimeout: time.Second,
			retryPolicy:    retryPolicy,
			connector:      connector,
		}

		notifyCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		// Exhaust retries
		retryPolicy.On("ShouldRetry", 0).Return(true).Once()
		connector.On("ConnectConfig", mock.Anything, config).Return(nil, errors.New("conn error")).Once()

		retryPolicy.On("ShouldRetry", 1).Return(false).Once()

		l.WaitForNotification(ctx, notifyCh, errCh)

		select {
		case err := <-errCh:
			assert.Contains(t, err.Error(), "reconnect attempts exhausted")
		default:
			t.Fatal("expected error but got none")
		}

		connector.AssertExpectations(t)
		retryPolicy.AssertExpectations(t)
	})

	t.Run("successfully handle timeout during wait", func(t *testing.T) {
		ctx, cancel := context.WithCancel(t.Context())
		defer cancel()

		connector := newMocknlConnector(t)
		conn := newMocknlConnection(t)
		retryPolicy := newMockRetryPolicy(t)

		config := &pgx.ConnConfig{}
		l := &notificationListener{
			connConfig:     config,
			listenChannel:  "test_channel",
			waitingTimeout: time.Millisecond * 10,
			closingTimeout: time.Second,
			retryPolicy:    retryPolicy,
			connector:      connector,
		}

		notifyCh := make(chan struct{}, 1)
		errCh := make(chan error, 1)

		retryPolicy.On("ShouldRetry", 0).Return(true)
		connector.On("ConnectConfig", mock.Anything, config).Return(conn, nil)
		conn.On("Exec", mock.Anything, "LISTEN \"test_channel\"").Return(pgconn.CommandTag{}, nil)
		conn.On("Close", mock.Anything).Return(nil)

		// First call returns timeout
		conn.On("WaitForNotification", mock.Anything).Return(nil, context.DeadlineExceeded).Once()
		// Second call returns notification
		conn.On("WaitForNotification", mock.Anything).Return(&pgconn.Notification{}, nil).Once()
		// Third call blocks
		conn.On("WaitForNotification", mock.Anything).Run(func(args mock.Arguments) {
			<-ctx.Done()
		}).Return(nil, context.Canceled)

		done := make(chan struct{})
		go func() {
			defer close(done)
			l.WaitForNotification(ctx, notifyCh, errCh)
		}()

		select {
		case <-notifyCh:
			// Success
		case err := <-errCh:
			t.Fatalf("unexpected error: %v", err)
		case <-ctx.Done():
			t.Fatalf("timeout waiting for notification: %v", ctx.Err())
		}

		cancel()
		<-done

		connector.AssertExpectations(t)
		conn.AssertExpectations(t)
		retryPolicy.AssertExpectations(t)
	})
}
