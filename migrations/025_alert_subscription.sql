-- Per-indexer alert subscriptions (lodestar#256). The data is public, so a subscription proves no
-- ownership; the webhook answering a test post is the only check. The manage token is stored hashed.
CREATE TABLE IF NOT EXISTS alert_subscription (
    id                   UUID PRIMARY KEY,
    indexer_address      TEXT NOT NULL,
    webhook_url          TEXT NOT NULL,
    kinds                TEXT[] NOT NULL,
    signal_move_pct      DOUBLE PRECISION NOT NULL,
    manage_token_sha256  TEXT NOT NULL,
    created_at           TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    last_evaluated_at    TIMESTAMPTZ,
    last_delivered_at    TIMESTAMPTZ,
    consecutive_failures INT NOT NULL DEFAULT 0,
    last_error           TEXT,
    disabled_at          TIMESTAMPTZ
);
CREATE INDEX IF NOT EXISTS alert_subscription_active ON alert_subscription (indexer_address) WHERE disabled_at IS NULL;

-- What the evaluator saw last time, per subscription: POI tone per allocation, denial and signal
-- baseline per deployment, cuts, REO status.
CREATE TABLE IF NOT EXISTS alert_subscription_state (
    subscription_id UUID PRIMARY KEY REFERENCES alert_subscription(id) ON DELETE CASCADE,
    state           JSONB NOT NULL,
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
