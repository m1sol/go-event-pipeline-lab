package orders

import (
	"context"
	"time"

	"github.com/google/uuid"
)

type Producer interface {
	PublishOrderCreated(ctx context.Context, event OrderCreated) error
}

type Service struct {
	producer Producer
}

func NewService(producer Producer) *Service {
	return &Service{
		producer: producer,
	}
}

type CreateOrderInput struct {
	UserID string
	Amount int64
}

func (s *Service) CreateOrder(ctx context.Context, input CreateOrderInput) (OrderCreated, error) {
	event := OrderCreated{
		EventID:   uuid.NewString(),
		Version:   1,
		OrderID:   uuid.NewString(),
		UserID:    input.UserID,
		Amount:    input.Amount,
		CreatedAt: time.Now().UTC(),
	}

	if err := s.producer.PublishOrderCreated(ctx, event); err != nil {
		return OrderCreated{}, err
	}

	return event, nil
}
