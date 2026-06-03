package kafka

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"

	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
)

type Producer struct {
	writer *kafka.Writer
}

func NewProducer(brokers []string, topic string) *Producer {
	return &Producer{
		writer: &kafka.Writer{
			Addr:     kafka.TCP(brokers...),
			Topic:    topic,
			Balancer: &kafka.Hash{},
		},
	}
}

func (p *Producer) PublishOrderCreated(ctx context.Context, event orders.OrderCreated) error {
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}

	msg := kafka.Message{
		Key:   []byte(event.OrderID),
		Value: payload,
	}

	return p.writer.WriteMessages(ctx, msg)
}

func (p *Producer) Close() error {
	return p.writer.Close()
}

func (p *Producer) Publish(
	ctx context.Context,
	key string,
	payload []byte,
) error {
	msg := kafka.Message{
		Key:   []byte(key),
		Value: payload,
	}

	return p.writer.WriteMessages(ctx, msg)
}
