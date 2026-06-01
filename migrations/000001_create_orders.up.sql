CREATE TABLE IF NOT EXISTS orders (
                                      id TEXT PRIMARY KEY,
                                      user_id TEXT NOT NULL,
                                      amount BIGINT NOT NULL,
                                      created_at TIMESTAMPTZ NOT NULL DEFAULT now()
    );