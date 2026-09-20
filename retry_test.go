package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestRetryPolicy_ShouldRetry(t *testing.T) {
	tests := []struct {
		name        string
		maxAttempts int
		attempts    int
		expected    bool
	}{
		{
			name:        "should retry when attempts less than max",
			maxAttempts: 3,
			attempts:    0,
			expected:    true,
		},
		{
			name:        "should retry when attempts less than max (2)",
			maxAttempts: 3,
			attempts:    2,
			expected:    true,
		},
		{
			name:        "should not retry when attempts equal max",
			maxAttempts: 3,
			attempts:    3,
			expected:    false,
		},
		{
			name:        "should not retry when attempts exceed max",
			maxAttempts: 3,
			attempts:    4,
			expected:    false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			rp := NewRetryPolicy(NewBackoff(100*time.Millisecond, 1*time.Second, 2.0), tt.maxAttempts)
			actual := rp.ShouldRetry(tt.attempts)
			assert.Equal(t, tt.expected, actual)
		})
	}
}

func TestRetryPolicy_NextRetry(t *testing.T) {
	t.Run("NextRetry returns time in the future based on backoff", func(t *testing.T) {
		baseDelay := 100 * time.Millisecond
		backoff := NewBackoff(baseDelay, 1*time.Second, 2.0)
		rp := NewRetryPolicy(backoff, 3)

		before := time.Now().UTC()
		next := rp.NextRetry(0)
		after := time.Now().UTC()

		// NextRetry(0) uses baseDelay (100ms)
		expectedMin := before.Add(baseDelay)
		expectedMax := after.Add(baseDelay)

		assert.True(t, next.After(before), "NextRetry should be after 'before'")
		assert.WithinDuration(t, expectedMin, next, 50*time.Millisecond, "NextRetry should be close to expected time")
		assert.True(t, next.Equal(expectedMin) || next.After(expectedMin), "NextRetry should be >= before + baseDelay")
		assert.True(t, next.Equal(expectedMax) || next.Before(expectedMax), "NextRetry should be <= after + baseDelay")
	})

	t.Run("NextRetry respects backoff for further attempts", func(t *testing.T) {
		baseDelay := 100 * time.Millisecond
		backoff := NewBackoff(baseDelay, 1*time.Second, 2.0)
		rp := NewRetryPolicy(backoff, 3)

		// NextRetry(1) should be 200ms
		before := time.Now().UTC()
		next := rp.NextRetry(1)
		after := time.Now().UTC()

		expectedDelay := 200 * time.Millisecond
		expectedMin := before.Add(expectedDelay)
		expectedMax := after.Add(expectedDelay)

		assert.WithinDuration(t, expectedMin, next, 50*time.Millisecond)
		assert.True(t, next.Equal(expectedMin) || next.After(expectedMin))
		assert.True(t, next.Equal(expectedMax) || next.Before(expectedMax))
	})
}
