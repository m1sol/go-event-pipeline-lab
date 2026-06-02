package consumer

import (
	"context"
	"errors"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/m1sol/go-event-pipeline-lab/internal/postgres"

	"github.com/m1sol/go-event-pipeline-lab/internal/orders"
)

type Repository interface {
	BeginTx(ctx context.Context) (pgx.Tx, error)

	SaveProcessedMessage(
		ctx context.Context,
		tx pgx.Tx,
		messageID uuid.UUID,
	) error

	SaveOrder(
		ctx context.Context,
		tx pgx.Tx,
		event orders.OrderCreated,
	) error
}

type Service struct {
	repo Repository
}

func NewService(repo Repository) *Service {
	return &Service{repo: repo}
}

func (s *Service) HandleOrderCreated(ctx context.Context, event orders.OrderCreated) error {
	tx, err := s.repo.BeginTx(ctx)
	if err != nil {
		return err
	}
	defer tx.Rollback(ctx)

	mesUUID, err := uuid.Parse(event.MessageID)
	if err != nil {
		return err
	}

	err = s.repo.SaveProcessedMessage(
		ctx,
		tx,
		mesUUID,
	)
	if errors.Is(err, postgres.ErrDuplicateMessage) {
		_ = tx.Rollback(ctx)
		return nil
	}

	if err != nil {
		return err
	}

	err = s.repo.SaveOrder(
		ctx,
		tx,
		event,
	)
	if err != nil {
		return err
	}

	err = tx.Commit(ctx)
	return err
}
