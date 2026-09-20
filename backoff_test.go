package outbox

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestBackoff_Next(t *testing.T) {
	tests := []struct {
		name      string
		baseDelay time.Duration
		maxDelay  time.Duration
		factor    float64
		attempt   int
		expected  time.Duration
	}{
		{
			name:      "attempt 0 returns base delay",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    2.0,
			attempt:   0,
			expected:  100 * time.Millisecond,
		},
		{
			name:      "negative attempt returns base delay",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    2.0,
			attempt:   -1,
			expected:  100 * time.Millisecond,
		},
		{
			name:      "attempt 1 returns base delay * factor",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    2.0,
			attempt:   1,
			expected:  200 * time.Millisecond,
		},
		{
			name:      "attempt 2 returns base delay * factor^2",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    2.0,
			attempt:   2,
			expected:  400 * time.Millisecond,
		},
		{
			name:      "attempt 3 returns base delay * factor^3",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    2.0,
			attempt:   3,
			expected:  800 * time.Millisecond,
		},
		{
			name:      "cap at max delay",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    2.0,
			attempt:   4, // 100 * 2^4 = 1600ms
			expected:  1 * time.Second,
		},
		{
			name:      "factor of 1.0 keeps delay constant",
			baseDelay: 100 * time.Millisecond,
			maxDelay:  1 * time.Second,
			factor:    1.0,
			attempt:   5,
			expected:  100 * time.Millisecond,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := NewBackoff(tt.baseDelay, tt.maxDelay, tt.factor)
			actual := b.Next(tt.attempt)
			assert.Equal(t, tt.expected, actual)
		})
	}
}
