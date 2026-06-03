package main

import (
	"context"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/joho/godotenv"
	"github.com/m1sol/go-event-pipeline-lab/internal/api"
	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
	"log"
	"net/http"
	"os"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		log.Println("Warning: .env file not found")
	}
	httpAddr := getEnv("HTTP_ADDR", ":8080")
	databaseURL := getEnv("DATABASE_URL", "postgres://postgres:postgres@localhost:5432/orders?sslmode=disable")

	db, err := pgxpool.New(context.Background(), databaseURL)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	repo := orders.NewPostgresRepository(db)
	orderService := orders.NewService(repo)
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
