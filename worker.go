package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Worker polls the repository and dispatches messages to their destinations.
type Worker struct {
	repo       Repository
	dispatcher Dispatcher
	interval   time.Duration
	limit      int
	errorSleep time.Duration
	logger     *slog.Logger
}

func NewWorker(
	repo Repository,
	dispatcher Dispatcher,
	interval time.Duration,
	limit int,
	errorSleep time.Duration,
	logger *slog.Logger,
) Worker {
	if logger == nil {
		logger = slog.Default()
	}

	return Worker{
		repo:       repo,
		dispatcher: dispatcher,
		interval:   interval,
		limit:      limit,
		errorSleep: errorSleep,
		logger:     logger,
	}
}

// Run starts the worker loop until the context is cancelled.
func (w *Worker) Run(ctx context.Context) {
	w.logger.Info("outbox worker started")
	defer w.logger.Info("outbox worker stopped")

	w.process(ctx)

	ticker := time.NewTicker(w.interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.process(ctx)
		}
	}
}

// process executes a single processing cycle with error handling and retry sleep.
func (w *Worker) process(ctx context.Context) {
	if err := w.doProcess(ctx); err != nil {
		w.logger.Error("outbox processing failed",
			slog.Any("error", err),
		)
		if isRetryable(err) {
			w.logger.Info("retrying after sleep due to transient error",
				slog.Duration("duration", w.errorSleep),
			)
			time.Sleep(w.errorSleep)
		}
	}
}

// doProcess claims pending messages and handles each one.
func (w *Worker) doProcess(ctx context.Context) error {
	msgs, err := w.repo.ClaimPending(ctx, w.limit)
	if err != nil {
		return fmt.Errorf("claim pending messages: %w", err)
	}

	for _, msg := range msgs {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := w.handle(ctx, msg); err != nil {
			return fmt.Errorf("process message %d (%s): %w", msg.ID, msg.EventType, err)
		}
	}

	return nil
}

// handle dispatches a single message and marks it as published or failed.
func (w *Worker) handle(ctx context.Context, msg *Message) error {
	var err error

	if err = w.dispatcher.Dispatch(ctx, msg); err != nil {
		if isRetryable(err) {
			return err
		}

		// TODO: retry strategy on serialization error?

		return w.repo.MarkFailed(ctx, msg.ID, err)
	}

	if err = w.repo.MarkPublished(ctx, msg.ID); err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	return nil
}
