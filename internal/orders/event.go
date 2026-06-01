package orders

import "time"

type OrderCreated struct {
	EventID   string    `json:"event_id"`
	Version   int       `json:"version"`
	OrderID   string    `json:"order_id"`
	UserID    string    `json:"user_id"`
	Amount    int64     `json:"amount"`
	CreatedAt time.Time `json:"created_at"`
}
