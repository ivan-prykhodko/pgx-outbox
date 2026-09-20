package outbox

import (
	"context"
	"fmt"
	"sync/atomic"
	"time"
)

type Delivery struct {
	Message Message
	Ack     func()
}

type Provider interface {
	Provide(ctx context.Context) (<-chan Delivery, <-chan error)
}

type provider struct {
	repo                 Repository
	notificationListener NotificationListener
	pollingInterval      time.Duration
	msgLimit             int
}

func NewProvider(
	repo Repository,
	notificationListener NotificationListener,
	pollingInterval time.Duration,
	msgLimit int,
) Provider {
	return &provider{
		repo:                 repo,
		notificationListener: notificationListener,
		pollingInterval:      pollingInterval,
		msgLimit:             msgLimit,
	}
}

func (p *provider) Provide(ctx context.Context) (<-chan Delivery, <-chan error) {
	deliveryCh := make(chan Delivery, p.msgLimit)
	errCh := make(chan error, 1)

	go func() {
		defer close(errCh)
		defer close(deliveryCh)

		if err := p.run(ctx, deliveryCh); err != nil {
			errCh <- err
		}
	}()

	return deliveryCh, errCh
}

func (p *provider) run(ctx context.Context, deliveryCh chan<- Delivery) error {
	notifyCh := make(chan struct{}, 1)
	notifyErrCh := make(chan error, 1)

	notifyCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	go p.notificationListener.WaitForNotification(notifyCtx, notifyCh, notifyErrCh)

	if err := p.drainMessages(ctx, deliveryCh); err != nil {
		return fmt.Errorf("failed to drain messages: %w", err)
	}

	var (
		nMsg int
		err  error
	)

	timer := time.NewTimer(p.pollingInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case err = <-notifyErrCh:
			return fmt.Errorf("failed to listen to notification: %w", err)
		case <-notifyCh:
			// TODO: add debounce?
		case <-timer.C:
		}

		if nMsg, err = p.pollMessages(ctx, deliveryCh); err != nil {
			return fmt.Errorf("failed to poll messages: %w", err)
		}

		if nMsg > 0 {
			// if any message was read, poll messages immediately
			timer.Reset(time.Nanosecond)
		} else {
			timer.Reset(p.pollingInterval)
		}
	}
}

func (p *provider) drainMessages(ctx context.Context, deliveryCh chan<- Delivery) error {
	for {
		n, err := p.pollMessages(ctx, deliveryCh)
		if err != nil {
			return err
		}

		if n == 0 {
			return nil
		}
	}
}

func (p *provider) pollMessages(ctx context.Context, deliveryCh chan<- Delivery) (int, error) {
	messages, err := p.repo.ClaimPending(ctx, p.msgLimit)
	if err != nil {
		return 0, err
	}

	if len(messages) == 0 {
		return 0, nil
	}

	done := make(chan struct{})
	var remaining atomic.Int32
	remaining.Store(int32(len(messages)))

	for i, msg := range messages {
		delivery := Delivery{
			Message: msg,
			Ack: func() {
				if remaining.Add(-1) == 0 {
					done <- struct{}{}
				}
			},
		}

		select {
		case deliveryCh <- delivery:
		case <-ctx.Done():
			return i, ctx.Err()
		}
	}

	select {
	case <-done:
	case <-ctx.Done():
		return 0, ctx.Err()
	}

	return len(messages), nil
}
