package outbox

import "time"

type Event struct {
	ID string

	AggregateType string
	AggregateID   string

	EventType string

	Payload []byte

	Status string

	Attempts  int
	LastError *string

	CreatedAt   time.Time
	PublishedAt *time.Time
}
