package outbox

import (
	"errors"
	"fmt"
)

var (
	ErrNotFound = errors.New("outbox message not found")
	// Deprecated
	ErrTxUnsupportedType    = errors.New("unsupported transaction type")
	ErrTxNil                = errors.New("transaction is nil")
	ErrNetwork              = errors.New("network error")
	ErrMessageSerialization = errors.New("message serialization error")
)

// RetryableError wraps an error to indicate it can be retried
type RetryableError struct {
	Err error
}

func (e *RetryableError) Error() string {
	return fmt.Sprintf("retryable error: %v", e.Err)
}

func (e *RetryableError) Unwrap() error {
	return e.Err
}

// IsRetryable checks if an error is transient
func isRetryable(err error) bool {
	var target *RetryableError
	return errors.As(err, &target) || errors.Is(err, ErrNetwork)
}
