package outbox

import (
	"errors"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRetryableError(t *testing.T) {
	innerErr := errors.New("inner")
	err := &RetryableError{Err: innerErr}

	assert.Equal(t, fmt.Sprintf("retryable error: %v", innerErr), err.Error())
	assert.Equal(t, innerErr, err.Unwrap())
	assert.True(t, errors.Is(err, innerErr))
}

func TestIsRetryable(t *testing.T) {
	t.Run("returns true for RetryableError", func(t *testing.T) {
		err := &RetryableError{Err: errors.New("transient")}
		assert.True(t, isRetryable(err))
	})

	t.Run("returns true for ErrNetwork", func(t *testing.T) {
		assert.True(t, isRetryable(ErrNetwork))
	})

	t.Run("returns true for wrapped ErrNetwork", func(t *testing.T) {
		err := fmt.Errorf("wrapped: %w", ErrNetwork)
		assert.True(t, isRetryable(err))
	})

	t.Run("returns false for regular error", func(t *testing.T) {
		assert.False(t, isRetryable(errors.New("permanent")))
	})

	t.Run("returns false for ErrNotFound", func(t *testing.T) {
		assert.False(t, isRetryable(ErrNotFound))
	})
}
