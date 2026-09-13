-- Buckets to roll up again because what they were computed from has changed.
--
-- The rollup loop only recomputes its trailing window. Two things change a bucket after it has left
-- that window: the resolver attributing an allocation signing key it could not resolve earlier
-- (retried after 24 hours), and a deployment joining or leaving `nondeterministic_deployment`. Both
-- write a request here and the rollup loop drains it, so no stored bucket keeps a count the current
-- data contradicts.
CREATE TABLE IF NOT EXISTS qos_reroll (
    id             BIGSERIAL PRIMARY KEY,
    deployment_id  TEXT,                  -- NULL: every deployment in the range
    from_ts        TIMESTAMPTZ NOT NULL,  -- probe dispatch times, widened to whole buckets on roll
    to_ts          TIMESTAMPTZ NOT NULL,
    reason         TEXT NOT NULL,
    requested_at   TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Migration 011's prose describes divergence against the stake-weighted majority. The rollup now
-- judges it exactly as the scorer grades correctness, and the schema should say so.
COMMENT ON COLUMN foghorn_qos.divergent_count IS
    'Responses differing from the largest cluster by count, counted only when that cluster holds more than half of the probe''s answers and the deployment is not flagged non-deterministic. The scorer''s definition of a correctness fault.';
COMMENT ON COLUMN foghorn_qos.comparable_count IS
    'Responses to probes with such a clear count majority, on deployments not flagged non-deterministic.';
