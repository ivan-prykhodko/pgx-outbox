package outbox

import "context"

type Reader interface {
	Read(ctx context.Context) (<-chan Message, error)
}

type pollReader struct {
	repo  Repository
	limit int
}

func NewPollReader(repo Repository, limit int) Reader {
	return &pollReader{
		repo:  repo,
		limit: limit,
	}
}

func (r *pollReader) Read(ctx context.Context) (<-chan Message, error) {
	msgs, err := r.repo.ClaimPending(ctx, r.limit)
	if err != nil {
		return nil, err
	}

	ch := make(chan Message)
	go func() {
		defer close(ch)
		for _, msg := range msgs {
			ch <- *msg
		}
	}()

	return ch, nil
}
