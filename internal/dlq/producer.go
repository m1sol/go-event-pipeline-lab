package dlq

import "context"

type Producer interface {
	Publish(ctx context.Context, msg Message) error
}
