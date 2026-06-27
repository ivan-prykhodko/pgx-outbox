package outbox

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/jackc/pgx/v5"
)

type Writer interface {
	Write(ctx context.Context, tx RowQuerier, msg *Message) (int64, error)
}

type writer struct {
	query string
}

func NewWriter(tableName string) Writer {
	query := fmt.Sprintf(`
INSERT INTO %s (
	aggregate_type,
	aggregate_id,
	event_type,
	payload,
	metadata,
	status,
	error,
	occurred_at,
	created_at,
	published_at
) VALUES (
	$1,$2,$3,$4,$5,$6,$7,$8,$9,$10
) RETURNING id;
`, tableName)

	return &writer{
		query: query,
	}
}

func (w writer) Write(ctx context.Context, tx RowQuerier, msg *Message) (int64, error) {
	if tx == nil {
		return 0, ErrTxNil
	}

	row := tx.QueryRow(ctx, w.query,
		msg.AggregateType,
		msg.AggregateID,
		msg.EventType,
		msg.Payload,
		msg.Metadata,
		msg.Status,
		msg.Error,
		msg.OccurredAt,
		msg.CreatedAt,
		msg.PublishedAt,
	)

	var id int64
	if err := row.Scan(&id); err != nil {
		return 0, fmt.Errorf("row.Scan: %w", err)
	}

	return id, nil
}

type RowQuerier interface {
	QueryRow(ctx context.Context, query string, args ...any) pgx.Row
}

type SqlRowQuerier struct {
	*sql.Tx
}

func (w SqlRowQuerier) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	return w.QueryRowContext(ctx, query, args...)
}
