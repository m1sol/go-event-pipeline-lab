package postgres

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
)

var ErrDuplicateMessage = errors.New("duplicate message")

type Repository struct {
	pool *pgxpool.Pool
}

func NewRepository(pool *pgxpool.Pool) *Repository {
	return &Repository{pool: pool}
}

func (r *Repository) SaveOrder(ctx context.Context, tx pgx.Tx, event orders.OrderCreated) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO orders (id, user_id, amount, created_at)
		VALUES ($1, $2, $3, $4)
	`, event.OrderID, event.UserID, event.Amount, event.CreatedAt)

	return err
}

func (r *Repository) SaveProcessedMessage(ctx context.Context, tx pgx.Tx, messageID uuid.UUID) error {
	_, err := tx.Exec(ctx, `
		INSERT INTO processed_messages (message_id, processed_at)
		VALUES ($1, $2)
	`, messageID, time.Now().UTC())
	var pgErr *pgconn.PgError

	if errors.As(err, &pgErr) {
		if pgErr.Code == "23505" {
			return ErrDuplicateMessage
		}
	}

	return err
}

func (r *Repository) BeginTx(ctx context.Context) (pgx.Tx, error) {
	return r.pool.Begin(ctx)
}
