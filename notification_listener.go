package outbox

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

type NotificationListener interface {
	WaitForNotification(ctx context.Context, notifyCh chan<- struct{}, errCh chan<- error)
}

type nlConnector interface {
	ConnectConfig(ctx context.Context, config *pgx.ConnConfig) (nlConnection, error)
}

type nlConnection interface {
	WaitForNotification(ctx context.Context) (*pgconn.Notification, error)
	Exec(ctx context.Context, sql string, arguments ...any) (pgconn.CommandTag, error)
	Close(ctx context.Context) error
}

type pgxNLConnector struct{}

func (c *pgxNLConnector) ConnectConfig(ctx context.Context, config *pgx.ConnConfig) (nlConnection, error) {
	conn, err := pgx.ConnectConfig(ctx, config)
	if err != nil {
		return nil, err
	}
	return conn, nil
}

type notificationListener struct {
	connConfig     *pgx.ConnConfig
	listenChannel  string
	waitingTimeout time.Duration
	closingTimeout time.Duration
	retryPolicy    RetryPolicy
	connector      nlConnector
}

func NewNotificationListener(
	connConfig *pgx.ConnConfig,
	listenChannel string,
	waitingTimeout time.Duration,
	closingTimeout time.Duration,
	retryPolicy RetryPolicy,
) NotificationListener {
	return &notificationListener{
		connConfig:     connConfig,
		listenChannel:  listenChannel,
		waitingTimeout: waitingTimeout,
		closingTimeout: closingTimeout,
		retryPolicy:    retryPolicy,
		connector:      &pgxNLConnector{},
	}
}

func (l *notificationListener) WaitForNotification(ctx context.Context, notifyCh chan<- struct{}, errCh chan<- error) {
	var conn nlConnection

	defer func() {
		if conn != nil {
			_ = l.disconnect(ctx, conn)
		}
	}()

	for {
		if conn == nil {
			var err error

			conn, err = l.reconnect(ctx)
			if err != nil {
				select {
				case errCh <- fmt.Errorf("failed to reconnect: %w", err):
				case <-ctx.Done():
				}
				return
			}
		}

		notifyCtx, cancel := context.WithTimeout(ctx, l.waitingTimeout)
		_, err := conn.WaitForNotification(notifyCtx)
		cancel()

		if err != nil {
			if ctx.Err() != nil {
				return
			}
			if errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			_ = l.disconnect(ctx, conn)
			conn = nil

			continue
		}

		select {
		case notifyCh <- struct{}{}:
		default:
		}
	}
}

func (l *notificationListener) reconnect(ctx context.Context) (nlConnection, error) {
	var lastErr error

	for attempts := 0; l.retryPolicy.ShouldRetry(attempts); attempts++ {
		if attempts > 0 {
			retryAt := l.retryPolicy.NextRetry(attempts - 1)
			if err := l.waitUntil(ctx, retryAt); err != nil {
				return nil, err
			}
		}

		conn, err := l.connector.ConnectConfig(ctx, l.connConfig)
		if err != nil {
			lastErr = fmt.Errorf("failed to connect: %w", err)
			continue
		}

		if err := l.listen(ctx, conn); err != nil {
			_ = l.disconnect(ctx, conn)
			lastErr = fmt.Errorf("failed to listen: %w", err)
			continue
		}

		return conn, nil
	}

	return nil, fmt.Errorf("reconnect attempts exhausted: %w", lastErr)
}

func (l *notificationListener) listen(ctx context.Context, conn nlConnection) error {
	channel := pgx.Identifier{l.listenChannel}.Sanitize()
	_, err := conn.Exec(ctx, "LISTEN "+channel)

	return err
}

func (l *notificationListener) disconnect(ctx context.Context, conn nlConnection) error {
	closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), l.closingTimeout)
	defer cancel()

	return conn.Close(closeCtx)
}

func (l *notificationListener) waitUntil(ctx context.Context, at time.Time) error {
	delay := time.Until(at)

	if delay <= 0 {
		return nil
	}

	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
