package outbox

import (
	"context"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type Repository interface {
	// ClaimPending locks and returns a batch of messages to be processed.
	ClaimPending(ctx context.Context, limit int) ([]Message, error)
}

type repository struct {
	pool               *pgxpool.Pool
	claimPendingQuery  string
	markPublishedQuery string
	markFailedQuery    string
}

func NewRepository(pool *pgxpool.Pool, tableName string) Repository {
	return &repository{
		pool:               pool,
		claimPendingQuery:  getClaimPendingQuery(tableName),
		markPublishedQuery: getMarkPublishedQuery(tableName),
		markFailedQuery:    getMarkFailedQuery(tableName),
	}
}

func (r *repository) ClaimPending(ctx context.Context, limit int) ([]Message, error) {
	rows, err := r.pool.Query(ctx, r.claimPendingQuery, limit)
	if err != nil {
		return nil, fmt.Errorf("claim pending messages: %w", err)
	}
	// Note: CollectRows handles rows.Close()

	// TODO: compare performance pgx.CollectRows(rows, pgx.RowToAddrOfStructByName[Message])
	msgs, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (Message, error) {
		msg := Message{}
		scanErr := row.Scan(
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
		)
		return msg, scanErr
	})
	if err != nil {
		return nil, fmt.Errorf("collect claimed messages: %w", err)
	}

	return msgs, nil
}

func (r *repository) Ack(ctx context.Context, id int64) error {
	tag, err := r.pool.Exec(ctx, r.markPublishedQuery, id)
	if err != nil {
		return fmt.Errorf("mark published: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

func (r *repository) Nack(ctx context.Context, id int64, errMsg error) error {
	tag, err := r.pool.Exec(ctx, r.markFailedQuery, id, errMsg.Error())
	if err != nil {
		return fmt.Errorf("mark failed: %w", err)
	}

	if tag.RowsAffected() == 0 {
		return ErrNotFound
	}

	return nil
}

func getClaimPendingQuery(tableName string) string {
	return fmt.Sprintf(`
WITH cte AS (
	SELECT id
	FROM %s
	WHERE (status = 'PENDING' or status = 'PROCESSING')
	ORDER BY occurred_at ASC
	FOR UPDATE SKIP LOCKED
	LIMIT $1
), updated AS (
	UPDATE %s m
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
		m.created_at
)
SELECT
	u.id,
	u.aggregate_type,
	u.aggregate_id,
	u.event_type,
	u.payload,
	u.metadata,
	u.status,
	u.error,
	u.occurred_at,
	u.created_at 
FROM
	updated u 
ORDER BY
    occurred_at ASC
;
`, tableName, tableName)
}

func getMarkPublishedQuery(tableName string) string {
	return fmt.Sprintf(`
UPDATE %s
SET status = '%s',
    published_at = NOW()
WHERE id = $1
`, tableName, StatusPublished)
}

func getMarkFailedQuery(tableName string) string {
	return fmt.Sprintf(`
UPDATE %s
SET status = '%s',
    error = $2
WHERE id = $1
`, tableName, StatusFailed)
}
