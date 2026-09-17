package outbox

import (
	"math"
	"time"
)

type Backoff interface {
	Next(attempt int) time.Duration
}

type backoff struct {
	baseDelay time.Duration
	maxDelay  time.Duration
	factor    float64
}

func NewBackoff(baseDelay time.Duration, maxDelay time.Duration, factor float64) Backoff {
	return &backoff{
		baseDelay: baseDelay,
		maxDelay:  maxDelay,
		factor:    factor,
	}
}

func (b backoff) Next(attempt int) time.Duration {
	if attempt <= 0 {
		return b.baseDelay
	}

	d := float64(b.baseDelay) * math.Pow(b.factor, float64(attempt))
	if d > float64(b.maxDelay) {
		return b.maxDelay
	}

	return time.Duration(d)
}
