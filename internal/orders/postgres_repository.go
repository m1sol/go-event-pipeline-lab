package orders

import (
	"context"
	"encoding/json"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PostgresRepository struct {
	db *pgxpool.Pool
}

func NewPostgresRepository(db *pgxpool.Pool) *PostgresRepository {
	return &PostgresRepository{
		db: db,
	}
}

func (r *PostgresRepository) CreateOrder(ctx context.Context, event OrderCreated) error {
	tx, err := r.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(ctx, `
		INSERT INTO orders (
			id,
			user_id,
			amount,
			created_at
		)
		VALUES ($1, $2, $3, $4)
		ON CONFLICT (id) DO NOTHING
	`,
		event.OrderID,
		event.UserID,
		event.Amount,
		event.CreatedAt,
	)
	if err != nil {
		return err
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return err
	}
	_, err = tx.Exec(ctx, `
	INSERT INTO outbox_events (
		id,
		aggregate_type,
		aggregate_id,
		event_type,
		payload
	)
	VALUES ($1, $2, $3, $4, $5)
`,
		event.EventID,
		"order",
		event.OrderID,
		"order.created",
		payload,
	)
	if err != nil {
		return err
	}
	return tx.Commit(ctx)
}
