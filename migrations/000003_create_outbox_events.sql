CREATE TABLE IF NOT EXISTS outbox_events (
                                             id UUID PRIMARY KEY,

                                             aggregate_type TEXT NOT NULL,
                                             aggregate_id TEXT NOT NULL,

                                             event_type TEXT NOT NULL,

                                             payload JSONB NOT NULL,

                                             status TEXT NOT NULL DEFAULT 'pending',

                                             attempts INTEGER NOT NULL DEFAULT 0,
                                             last_error TEXT,

                                             created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    published_at TIMESTAMPTZ
    );