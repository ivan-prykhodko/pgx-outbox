package outbox

import (
	"context"
)

type Repository interface {
	ClaimPending(ctx context.Context, limit int) ([]*Message, error)
	MarkPublished(ctx context.Context, id int64) error
	MarkFailed(ctx context.Context, id int64, errMsg error) error
}
