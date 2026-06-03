package outbox

import "time"

const (
	MaxAttempts  = 3
	PollInterval = 5 * time.Second
)
