package dlq

import (
	"context"
	"encoding/json"

	"github.com/segmentio/kafka-go"
)

type KafkaProducer struct {
	writer *kafka.Writer
}

func NewKafkaProducer(brokers []string, topic string) *KafkaProducer {
	return &KafkaProducer{
		writer: &kafka.Writer{
			Addr:  kafka.TCP(brokers...),
			Topic: topic,
		},
	}
}

func (p *KafkaProducer) Publish(ctx context.Context, msg Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}

	return p.writer.WriteMessages(ctx, kafka.Message{
		Key:   []byte(msg.Key),
		Value: data,
	})
}

func (p *KafkaProducer) Close() error {
	return p.writer.Close()
}

var _ Producer = (*KafkaProducer)(nil)
