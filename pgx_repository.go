package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type pgxRepository struct {
	pool      *pgxpool.Pool
	tableName string // TODO
}

func NewPgxRepository(pool *pgxpool.Pool) Repository {
	return &pgxRepository{
		pool: pool,
	}
}

func (r *pgxRepository) ClaimPending(ctx context.Context, limit int) ([]*Message, error) {
	const q = `
WITH cte AS (
	SELECT id
	FROM outbox_messages
	WHERE (status = 'PENDING' or status = 'PROCESSING')
	ORDER BY occurred_at ASC
	FOR UPDATE SKIP LOCKED
	LIMIT $1
)
UPDATE outbox_messages m
SET status = 'PROCESSING'
FROM cte
WHERE m.id = cte.id
RETURNING 
	m.id,
	m.aggregate_type,
	m.aggregate_id,
	m.event_type,
	m.payload,
	m.metadata,
	m.status,
	m.error,
	m.occurred_at,
	m.created_at,
	m.published_at;
`

	rows, err := r.pool.Query(ctx, q, limit)
	if err != nil {
		return nil, fmt.Errorf("claim pending messages: %w", err)
	}
	// Note: CollectRows handles rows.Close()

	//msgs, err := pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[Message])
	msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (*Message, error) {
		msg := &Message{}
		err := row.Scan(
			&msg.ID,
			&msg.AggregateType,
			&msg.AggregateID,
			&msg.EventType,
			&msg.Payload,
			&msg.Metadata,
			&msg.Status,
			&msg.Error,
			&msg.OccurredAt,
			&msg.CreatedAt,
			&msg.PublishedAt,
		)
		return msg, err
	})
	if err != nil {
		return nil, fmt.Errorf("collect claimed messages: %w", err)
	}

	return msgs, nil
}

func (r *pgxRepository) MarkPublished(ctx context.Context, id int64) error {
	const q = `
UPDATE outbox_messages
SET status = 'PUBLISHED',
    published_at = NOW()
WHERE id = $1
`

	tag, err := r.pool.Exec(ctx, q, id)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

func (r *pgxRepository) MarkFailed(ctx context.Context, id int64, errMsg error) error {
	const q = `
UPDATE outbox_messages
SET status = 'FAILED',
    error = $2
WHERE id = $1
`

	tag, err := r.pool.Exec(ctx, q, id, errMsg.Error())
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}
