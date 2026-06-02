package main

import (
	"github.com/joho/godotenv"
	"log"
	"net/http"
	"os"
	"strings"

	"github.com/m1sol/go-event-pipeline-lab/internal/api"
	appkafka "github.com/m1sol/go-event-pipeline-lab/internal/kafka"
	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Warning: .env file not found")
	}
	httpAddr := getEnv("HTTP_ADDR", ":8080")
	kafkaBrokers := strings.Split(getEnv("KAFKA_BROKERS", "localhost:9092"), ",")
	kafkaTopic := getEnv("KAFKA_TOPIC", "orders.created")

	producer := appkafka.NewProducer(kafkaBrokers, kafkaTopic)
	defer producer.Close()

	orderService := orders.NewService(producer)
	orderHandler := api.NewOrdersHandler(orderService)

	mux := http.NewServeMux()
	mux.HandleFunc("/orders", orderHandler.CreateOrder)

	log.Printf("producer-api listening on %s", httpAddr)

	if err := http.ListenAndServe(httpAddr, mux); err != nil {
		log.Fatal(err)
	}
}

func getEnv(key, fallback string) string {
	value := os.Getenv(key)
	if value == "" {
		return fallback
	}
	return value
}
