package outbox

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPollReader_Read(t *testing.T) {
	limit := 10
	ctx := t.Context()

	t.Run("successfully reads messages", func(t *testing.T) {
		repo := newMockRepository(t)
		reader := NewPollReader(repo, limit)

		messages := []Message{
			{ID: 1, AggregateID: "1"},
			{ID: 2, AggregateID: "2"},
		}

		repo.On("ClaimPending", ctx, limit).Return(messages, nil)

		ch, err := reader.Read(ctx)
		require.NoError(t, err)

		var received []Message
		for msg := range ch {
			received = append(received, msg)
		}

		assert.Equal(t, messages, received)
		repo.AssertExpectations(t)
	})

	t.Run("returns error if repository fails", func(t *testing.T) {
		repo := newMockRepository(t)
		reader := NewPollReader(repo, limit)

		repo.On("ClaimPending", ctx, limit).Return(nil, assert.AnError)

		ch, err := reader.Read(ctx)
		assert.ErrorIs(t, err, assert.AnError)
		assert.Nil(t, ch)
		repo.AssertExpectations(t)
	})
}
