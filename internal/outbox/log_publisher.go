package outbox

import (
	"context"
	"log"
)

type LogPublisher struct{}

func NewLogPublisher() *LogPublisher {
	return &LogPublisher{}
}

func (p *LogPublisher) Publish(
	ctx context.Context,
	event Event,
) error {
	log.Printf(
		"publish event: id=%s type=%s aggregate_id=%s",
		event.ID,
		event.EventType,
		event.AggregateID,
	)

	return nil
}
