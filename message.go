package outbox

import "time"

type Status string

const (
	StatusPending    Status = "PENDING"
	StatusProcessing Status = "PROCESSING"
	StatusPublished  Status = "PUBLISHED"
	StatusFailed     Status = "FAILED"
)

// Message represents an outbox entry to be processed.
type Message struct {
	ID            int64             `db:"id"`
	AggregateType string            `db:"aggregate_type"`
	AggregateID   string            `db:"aggregate_id"`
	EventType     string            `db:"event_type"`
	Payload       []byte            `db:"payload"`
	Metadata      map[string]string `db:"metadata"`
	Status        Status            `db:"status"`
	Error         *string           `db:"error"`
	OccurredAt    time.Time         `db:"occurred_at"`
	CreatedAt     time.Time         `db:"created_at"`
	PublishedAt   *time.Time        `db:"published_at"`
}

func NewMessage(
	aggregateType string,
	aggregateID string,
	eventType string,
	payload []byte,
	metadata map[string]string,
	occurredAt time.Time,
) Message {
	now := time.Now().UTC()

	return Message{
		AggregateType: aggregateType,
		AggregateID:   aggregateID,
		EventType:     eventType,
		Payload:       payload,
		Metadata:      metadata,
		Status:        StatusPending,
		OccurredAt:    occurredAt,
		CreatedAt:     now,
	}
}
