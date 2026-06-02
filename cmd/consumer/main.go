package main

import (
	"context"
	"encoding/json"
	"github.com/joho/godotenv"
	"log"
	"os"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/segmentio/kafka-go"

	appconsumer "github.com/m1sol/go-event-pipeline-lab/internal/consumer"
	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
	"github.com/m1sol/go-event-pipeline-lab/internal/postgres"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Warning: .env file not found")
	}

	ctx := context.Background()

	brokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	topic := getEnv("KAFKA_TOPIC", "orders.created")
	groupID := getEnv("KAFKA_GROUP_ID", "orders-consumer-group")
	databaseURL := getEnv(
		"DATABASE_URL",
		"postgres://kafka_lab:kafka_lab@localhost:5446/kafka_lab?sslmode=disable",
	)

	pool, err := pgxpool.New(ctx, databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	repo := postgres.NewRepository(pool)
	service := appconsumer.NewService(repo)

	reader := kafka.NewReader(kafka.ReaderConfig{
		Brokers: brokers,
		Topic:   topic,
		GroupID: groupID,

		StartOffset: kafka.FirstOffset,
	})
	defer reader.Close()

	log.Printf("consumer started: topic=%s group_id=%s", topic, groupID)

	for {
		msg, err := reader.FetchMessage(ctx)
		if err != nil {
			log.Printf("fetch message error: %v", err)
			continue
		}

		var event orders.OrderCreated
		if err := json.Unmarshal(msg.Value, &event); err != nil {
			log.Printf("invalid message: topic=%s partition=%d offset=%d error=%v",
				msg.Topic, msg.Partition, msg.Offset, err,
			)
			continue
		}

		if err := service.HandleOrderCreated(ctx, event); err != nil {
			log.Printf("handle message failed: topic=%s partition=%d offset=%d event_id=%s order_id=%s error=%v",
				msg.Topic, msg.Partition, msg.Offset, event.EventID, event.OrderID, err,
			)
			continue
		}

		if err := reader.CommitMessages(ctx, msg); err != nil {
			log.Printf("commit failed: topic=%s partition=%d offset=%d error=%v",
				msg.Topic, msg.Partition, msg.Offset, err,
			)
			continue
		}

		log.Printf("message processed: topic=%s partition=%d offset=%d event_id=%s order_id=%s",
			msg.Topic, msg.Partition, msg.Offset, event.EventID, event.OrderID,
		)
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
