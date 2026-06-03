package outbox

import "context"

type Repository interface {
	GetPending(ctx context.Context, limit int) ([]Event, error)
	MarkPublished(ctx context.Context, id string) error
	MarkFailed(ctx context.Context, id string, reason string) error
	MarkDead(ctx context.Context, id string) error
}
