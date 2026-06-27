package outbox

import (
	"context"
)

type Repository interface {
	// ClaimPending locks and returns a batch of messages to be processed.
	ClaimPending(ctx context.Context, limit int) ([]*Message, error)
	// MarkPublished marks a message as successfully published.
	MarkPublished(ctx context.Context, id int64) error
	// MarkFailed marks a message as failed with an error.
	MarkFailed(ctx context.Context, id int64, errMsg error) error
}
