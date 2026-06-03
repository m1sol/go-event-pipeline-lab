package outbox

import "context"

type Worker struct {
	repo      Repository
	publisher Publisher
}

func NewWorker(
	repo Repository,
	publisher Publisher,
) *Worker {
	return &Worker{
		repo:      repo,
		publisher: publisher,
	}
}

func (w *Worker) Process(
	ctx context.Context,
) error {
	events, err := w.repo.GetPending(ctx, 100)
	if err != nil {
		return err
	}

	for _, event := range events {
		if err := w.publisher.Publish(ctx, event); err != nil {
			if event.Attempts >= MaxAttempts-1 {
				if markErr := w.repo.MarkDead(
					ctx,
					event.ID,
				); markErr != nil {
					return markErr
				}

				continue
			}

			if markErr := w.repo.MarkFailed(
				ctx,
				event.ID,
				err.Error(),
			); markErr != nil {
				return markErr
			}

			continue
		}

		if err := w.repo.MarkPublished(ctx, event.ID); err != nil {
			return err
		}
	}

	return nil
}
