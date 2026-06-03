package outbox

import (
	"context"
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

func (r *PostgresRepository) GetPending(
	ctx context.Context,
	limit int,
) ([]Event, error) {
	rows, err := r.db.Query(ctx, `
		SELECT
			id,
			aggregate_type,
			aggregate_id,
			event_type,
			payload,
			status,
			attempts,
			created_at,
			published_at
		FROM outbox_events
		WHERE status = 'pending'
		AND attempts < $1
		ORDER BY created_at
		LIMIT $2
	`,
		MaxAttempts,
		limit,
	)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []Event

	for rows.Next() {
		event := Event{}

		if err := rows.Scan(
			&event.ID,
			&event.AggregateType,
			&event.AggregateID,
			&event.EventType,
			&event.Payload,
			&event.Status,
			&event.Attempts,
			&event.CreatedAt,
			&event.PublishedAt,
		); err != nil {
			return nil, err
		}

		events = append(events, event)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}

	return events, nil
}

func (r *PostgresRepository) MarkPublished(
	ctx context.Context,
	id string,
) error {
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_events
		SET
			status = 'published',
			published_at = NOW()
		WHERE id = $1
	`,
		id,
	)

	return err
}

func (r *PostgresRepository) MarkFailed(
	ctx context.Context,
	id string,
	reason string,
) error {
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_events
		SET
			attempts = attempts + 1,
			last_error = $2
		WHERE id = $1
	`,
		id,
		reason,
	)

	return err
}

func (r *PostgresRepository) MarkDead(
	ctx context.Context,
	id string,
) error {
	_, err := r.db.Exec(ctx, `
		UPDATE outbox_events
		SET status = 'failed'
		WHERE id = $1
	`,
		id,
	)

	return err
}
