package main

import (
	"context"
	"time"

	"github.com/m1sol/go-event-pipeline-lab/internal/dlq"
	"github.com/segmentio/kafka-go"
)

func publishToDLQ(
	ctx context.Context,
	producer dlq.Producer,
	msg kafka.Message,
	reason error,
) error {

	dlqMsg := dlq.Message{
		OriginalTopic:     msg.Topic,
		OriginalPartition: msg.Partition,
		OriginalOffset:    msg.Offset,

		Key:     string(msg.Key),
		Payload: string(msg.Value),

		Error: reason.Error(),

		FailedAt: time.Now().UTC(),
	}

	return producer.Publish(ctx, dlqMsg)
}
