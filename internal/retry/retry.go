package retry

import (
	"context"
	"log"
	"time"
)

func Do(
	ctx context.Context,
	attempts int,
	baseDelay time.Duration,
	fn func() error,
) error {

	var err error

	for i := 0; i < attempts; i++ {

		err = fn()
		if err == nil {
			return nil
		}

		log.Printf(
			"retry attempt=%d/%d err=%v",
			i+1,
			attempts,
			err,
		)

		delay := baseDelay * time.Duration(1<<i)

		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-time.After(delay):
		}
	}

	return err
}
