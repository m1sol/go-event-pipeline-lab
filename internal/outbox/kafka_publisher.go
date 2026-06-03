package outbox

import (
	"context"
	"github.com/m1sol/go-event-pipeline-lab/internal/kafka"
)

type KafkaPublisher struct {
	producer *kafka.Producer
}

func NewKafkaPublisher(producer *kafka.Producer) *KafkaPublisher {
	return &KafkaPublisher{producer: producer}
}
func (p *KafkaPublisher) Publish(
	ctx context.Context,
	event Event,
) error {
	return p.producer.Publish(
		ctx,
		event.AggregateID,
		event.Payload,
	)
}
