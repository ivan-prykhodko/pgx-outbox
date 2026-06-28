package outbox

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

// Worker polls the repository and dispatches messages to their destinations.
type Worker interface {
	Run(ctx context.Context)
}

type worker struct {
	reader     Reader
	processor  Processor
	interval   time.Duration
	retrySleep time.Duration // simple timeout for transient errors
	logger     *slog.Logger
}

func NewWorker(
	reader Reader,
	processor Processor,
	interval time.Duration,
	retrySleep time.Duration,
	logger *slog.Logger,
) Worker {
	if logger == nil {
		logger = slog.Default()
	}

	return &worker{
		reader:     reader,
		processor:  processor,
		interval:   interval,
		retrySleep: retrySleep,
		logger:     logger,
	}
}

// Run starts the worker loop until the context is cancelled.
func (w *worker) Run(ctx context.Context) {
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
func (w *worker) process(ctx context.Context) {
	if err := w.doProcess(ctx); err != nil {
		w.logger.Error("outbox processing failed",
			slog.Any("error", err),
		)
		if isRetryable(err) {
			w.logger.Info("retrying after sleep due to transient error",
				slog.Duration("duration", w.retrySleep),
			)
			time.Sleep(w.retrySleep)
		}
	}
}

// doProcess retrieves messages and handles each one.
func (w *worker) doProcess(ctx context.Context) error {
	msgsCh, err := w.reader.Read(ctx)
	if err != nil {
		return fmt.Errorf("read messages: %w", err)
	}

	for msg := range msgsCh {
		if err := ctx.Err(); err != nil {
			return err
		}

		if err := w.processor.Process(ctx, &msg); err != nil {
			return fmt.Errorf("process message %d (%s): %w", msg.ID, msg.EventType, err)
		}
	}

	return nil
}
