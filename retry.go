package outbox

import "time"

type RetryPolicy interface {
	ShouldRetry(attempts int) bool
	NextRetry(attempts int) time.Time
}

type retryPolicy struct {
	backoff     Backoff
	maxAttempts int
}

func NewRetryPolicy(backoff Backoff, maxAttempts int) RetryPolicy {
	return &retryPolicy{
		backoff:     backoff,
		maxAttempts: maxAttempts,
	}
}

func (r retryPolicy) ShouldRetry(attempts int) bool {
	return attempts < r.maxAttempts
}

func (r retryPolicy) NextRetry(attempts int) time.Time {
	return time.Now().UTC().Add(r.backoff.Next(attempts))
}
