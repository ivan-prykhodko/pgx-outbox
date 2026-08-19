package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestNewMessage(t *testing.T) {
	aggregateType := "order"
	aggregateID := "123"
	eventType := "order.created"
	payload := []byte(`{"id": 123}`)
	metadata := map[string]string{"version": "1"}
	occurredAt := time.Now().Add(-time.Hour).UTC()

	t.Run("creates a new message with correct fields", func(t *testing.T) {
		msg := NewMessage(aggregateType, aggregateID, eventType, payload, metadata, occurredAt)

		assert.Equal(t, aggregateType, msg.AggregateType)
		assert.Equal(t, aggregateID, msg.AggregateID)
		assert.Equal(t, eventType, msg.EventType)
		assert.Equal(t, payload, msg.Payload)
		assert.Equal(t, metadata, msg.Metadata)
		assert.Equal(t, StatusPending, msg.Status)
		assert.Equal(t, occurredAt, msg.OccurredAt)
		assert.Nil(t, msg.Error)
		assert.Nil(t, msg.PublishedAt)
		assert.WithinDuration(t, time.Now().UTC(), msg.CreatedAt, 2*time.Second) // TODO: use a better assertion
	})
}
