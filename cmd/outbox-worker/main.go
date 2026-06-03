package main

import (
	"context"
	"github.com/joho/godotenv"
	"github.com/m1sol/go-event-pipeline-lab/internal/kafka"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m1sol/go-event-pipeline-lab/internal/outbox"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Warning: .env file not found")
	}
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable")
	db, err := pgxpool.New(
		context.Background(),
		databaseURL,
	)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	producer := kafka.NewProducer(
		[]string{"localhost:9092"},
		"orders.created",
	)
	defer producer.Close()

	repo := outbox.NewPostgresRepository(db)
	publisher := outbox.NewKafkaPublisher(producer)

	worker := outbox.NewWorker(
		repo,
		publisher,
	)

	ctx, stop := signal.NotifyContext(
		context.Background(),
		os.Interrupt,
		syscall.SIGTERM,
	)
	defer stop()

	for {
		select {
		case <-ctx.Done():
			log.Println("worker stopped")
			return
		default:
		}
		if err := worker.Process(ctx); err != nil {
			log.Printf("worker error: %v", err)
		}
		ticker := time.NewTicker(outbox.PollInterval)
		defer ticker.Stop()
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
