//! QoS Foghorn measured itself.
//!
//! [`ingest`](crate::ingest) pulls QoS from Edge & Node's oracle, which means every QoS
//! number Foghorn serves is downstream of a ten-link private pipeline. On 2026-07-29 one
//! link died mid-bucket and the feed went dark for 35+ hours while Foghorn kept quoting the
//! stale figures. This loop derives the same information from observations Foghorn has
//! already collected and stored, so the surface stays live when theirs does not.
//!
//! It is a *rollup*, not a probe: it adds no network traffic and no new dependency. Every
//! run is a full recompute of the trailing window, upserted by primary key, so it is
//! idempotent — a restart mid-window, a double-run, or a late-arriving observation all
//! converge to the same rows. That matters more than efficiency here: the current bucket is
//! partial by definition and gets rewritten on each pass until the window moves past it.
//!
//! ## What this is not
//!
//! Not a census of real query traffic. The oracle counts what the gateway actually routed;
//! this counts what Foghorn chose to probe. `probe_count` is therefore a statement about
//! Foghorn's cadence, never about an indexer's popularity, and anything served from this
//! table has to say so — see the `source` field on the API responses.

use anyhow::Result;
use foghorn_core::config::QosRollupConfig;
use sqlx::PgPool;
use std::time::Duration;
use tracing::{info, warn};

pub async fn run_qos_rollup_loop(cfg: QosRollupConfig, pool: PgPool) {
    if !cfg.enabled {
        info!("QoS rollup disabled by config");
        return;
    }
    info!(
        bucket_secs = cfg.bucket_secs,
        lookback_secs = cfg.lookback_secs,
        interval = cfg.interval_secs,
        "QoS rollup loop starting"
    );
    loop {
        match rollup_once(&cfg, &pool).await {
            Ok(n) => info!(buckets = n, "QoS rollup complete"),
            Err(e) => warn!(error = %e, "QoS rollup failed"),
        }
        tokio::time::sleep(Duration::from_secs(cfg.interval_secs)).await;
    }
}

/// Recompute every (indexer, deployment, bucket) touched by the trailing window.
///
/// Latency percentiles come from *successful* probes only. Including errors would let a
/// fast 500 flatter an indexer, and the failure is already counted in `success_rate` —
/// counting it twice, once as a failure and once as excellent latency, would be perverse.
///
/// `correctness_rate` is NULL rather than 1.0 when nothing in the bucket was comparable, so
/// "we did not check" can never be read as "verified correct". That distinction is the whole
/// reason Foghorn's correctness signal is worth anything.
pub async fn rollup_once(cfg: &QosRollupConfig, pool: &PgPool) -> Result<u64> {
    let from = chrono::Utc::now().timestamp() as f64 - cfg.lookback_secs as f64;
    let mut rows = roll_range(cfg, pool, from, None, None).await?;

    let requests: Vec<(i64, Option<String>, f64, f64)> = sqlx::query_as(
        "SELECT id, deployment_id, extract(epoch FROM from_ts)::float8, extract(epoch FROM to_ts)::float8
         FROM qos_reroll ORDER BY id",
    )
    .fetch_all(pool)
    .await?;
    for (id, deployment, from, to) in requests {
        rows += roll_range(cfg, pool, from, Some(to), deployment.as_deref()).await?;
        sqlx::query("DELETE FROM qos_reroll WHERE id = $1")
            .bind(id)
            .execute(pool)
            .await?;
    }
    Ok(rows)
}

/// Recompute every stored bucket from the observations, a day at a time. Idempotent, and safe
/// beside the running loop: it upserts the rows a rollup would write from the same data.
pub async fn reroll_all(cfg: &QosRollupConfig, pool: &PgPool) -> Result<u64> {
    const DAY: f64 = 86_400.0;
    let first: Option<f64> =
        sqlx::query_scalar("SELECT extract(epoch FROM min(dispatched_at))::float8 FROM probe")
            .fetch_one(pool)
            .await?;
    let Some(first) = first else {
        return Ok(0);
    };
    let now = chrono::Utc::now().timestamp() as f64;
    let mut from = (first / DAY).floor() * DAY;
    let mut rows = 0;
    while from <= now {
        rows += roll_range(cfg, pool, from, Some(from + DAY - 1.0), None).await?;
        from += DAY;
    }
    Ok(rows)
}

/// Ask the rollup loop to recompute every bucket this deployment has, however old.
pub(crate) async fn request_deployment_reroll(
    pool: &PgPool,
    deployment: &str,
    reason: &str,
) -> Result<()> {
    sqlx::query(
        "INSERT INTO qos_reroll (deployment_id, from_ts, to_ts, reason)
         SELECT $1, min(dispatched_at), max(dispatched_at), $2 FROM probe
         WHERE deployment_id = $1
         HAVING count(*) > 0",
    )
    .bind(deployment)
    .bind(reason)
    .execute(pool)
    .await?;
    Ok(())
}

/// Recompute the buckets holding probes dispatched from `from` to `to` (open-ended when `None`),
/// widened to whole buckets, for one deployment or all.
async fn roll_range(
    cfg: &QosRollupConfig,
    pool: &PgPool,
    from: f64,
    to: Option<f64>,
    deployment: Option<&str>,
) -> Result<u64> {
    let bucket = cfg.bucket_secs as f64;

    let result = sqlx::query(
        r#"
        WITH window_range AS (
            -- On bucket boundaries. A bare `NOW() - lookback` cut the oldest bucket in two, and the
            -- upsert replaced its complete row with the half still inside the window.
            SELECT to_timestamp(floor($2 / $1) * $1) AS at,
                   COALESCE(to_timestamp((floor($7::float8 / $1) + 1) * $1), 'infinity') AS until
        ),
        responders AS (
            SELECT o.probe_id, count(*) FILTER (WHERE o.response_hash IS NOT NULL) AS n
            FROM observation o
            JOIN probe p ON p.id = o.probe_id
            WHERE p.dispatched_at >= (SELECT at FROM window_range)
              AND p.dispatched_at < (SELECT until FROM window_range)
              AND ($8::text IS NULL OR p.deployment_id = $8)
            GROUP BY o.probe_id
        ),
        obs AS (
            SELECT
                -- `observation.indexer_address` is the ALLOCATION SIGNING KEY recovered from the
                -- gateway's EIP-712 attestation, not the indexer. Publishing QoS keyed on it
                -- produced a feed where 46 of 46 rows matched `allocation_map.allocation_key` and
                -- none matched a real indexer: every address on the page was wrong, every
                -- `indexer_url` was null, and the oracle comparison found 13 shared deployments
                -- with zero shared indexers. Resolve it here.
                --
                -- INNER JOIN deliberately: an observation we cannot attribute to a real indexer is
                -- dropped rather than published under a signing key. Attributing quality to the
                -- wrong identity is a worse failure than missing a row, because a reader cannot
                -- tell it happened.
                -- Identity depends on HOW the probe was dispatched, and getting this wrong silently
                -- discards data rather than corrupting it, which is harder to notice.
                --
                -- A gateway observation's `indexer_address` is the allocation SIGNING KEY recovered
                -- from the attestation, so it must be resolved through `allocation_map`. A paid
                -- observation's is already the indexer: we chose who to pay, so there is nothing to
                -- recover. The join below used to be an INNER JOIN on the signing key for both,
                -- which meant every paid observation matched nothing and was dropped — the whole
                -- point of paid probing produced rows that never reached the feed or the grades,
                -- while the dispatch-mix counter (which does not join) happily reported them as
                -- coverage. Unbiased data, counted and then thrown away.
                COALESCE(m.indexer_address, o.indexer_address) AS indexer_address,
                p.deployment_id,
                to_timestamp(floor(extract(epoch FROM p.dispatched_at) / $1) * $1) AS bucket_start,
                o.latency_ms,
                (o.error_class IS NULL AND o.http_status = 200) AS ok,
                o.response_hash,
                -- The majority `scorer::load_probe_agg` grades by, so the feed and the grade cannot
                -- name different indexers as serving wrong data.
                d.largest_by_count_hash AS majority_hash,
                COALESCE(d.largest_by_count_size * 2 > r.n, false) AS clear_majority,
                -- Chainhead lag, derived from data we already collect. `freshness_sample` exists in
                -- the schema but NOTHING in the codebase ever inserted into it, so this column was
                -- null on every row while the page advertised it as one of two trustworthy fields.
                --
                -- `probe.block_number` is chainhead − $6 at probe creation, so chainhead at that
                -- moment is `block_number + $6`, and `observation.meta_block_number` is the head the
                -- indexer reported. Clamped at zero because the reference is a few seconds stale
                -- (Arbitrum blocks are sub-second), which routinely makes a current indexer look
                -- microscopically "ahead". The consequence is a conservative metric: it cannot
                -- resolve lag smaller than that staleness, but the case that matters — an indexer
                -- hundreds or thousands of blocks behind — measures cleanly.
                CASE WHEN o.meta_block_number IS NOT NULL
                     THEN GREATEST(0, (p.block_number + $6) - o.meta_block_number)::double precision
                     ELSE NULL
                END AS blocks_behind,
                -- Deployments that diverge every round are the subgraph's fault, not an indexer's.
                (nd.deployment_id IS NOT NULL) AS nondeterministic
            FROM observation o
            JOIN probe p ON p.id = o.probe_id
            -- LEFT, so a paid observation survives having no signing-key entry. The WHERE below
            -- still drops a GATEWAY observation we cannot attribute: publishing quality under a
            -- signing key would put the wrong name on it, which is worse than missing a row.
            LEFT JOIN allocation_map m
              ON m.allocation_key = o.indexer_address
             AND m.indexer_address IS NOT NULL
            LEFT JOIN divergence d ON d.probe_id = o.probe_id
            LEFT JOIN responders r ON r.probe_id = o.probe_id
            LEFT JOIN nondeterministic_deployment nd ON nd.deployment_id = p.deployment_id
            WHERE p.dispatched_at >= (SELECT at FROM window_range)
              AND p.dispatched_at < (SELECT until FROM window_range)
              AND ($8::text IS NULL OR p.deployment_id = $8)
              AND (m.indexer_address IS NOT NULL OR o.dispatch_mode = 'paid')
              -- A refused payment is a fact about OUR escrow, never about the indexer.
              --
              -- `payment_denylisted` means their tap-agent has not yet observed our deposit;
              -- `payment_refused` means we cannot pay at all. In both cases the indexer's service
              -- never ran, so there is nothing to measure — the request died at their payment check.
              -- Counting these as probes would put them in the denominator and out of the numerator,
              -- publishing a collapsed success rate for operators whose only offence is that our
              -- money has not reached their agent yet. On a public page that names them, that is a
              -- straightforward libel generated by our own funding state.
              --
              -- Excluded entirely rather than counted as failures: see `payment_outcomes` for where
              -- they ARE reported, which is as a fact about us.
              AND (o.error_class IS NULL OR o.error_class NOT LIKE 'payment\_%')
        ),
        agg AS (
            SELECT
                indexer_address,
                deployment_id,
                bucket_start,
                count(*)                                     AS query_count,
                count(*) FILTER (WHERE ok)                   AS num_200,
                avg(latency_ms::double precision)
                    FILTER (WHERE ok)                        AS avg_latency,
                max(latency_ms::double precision)
                    FILTER (WHERE ok)                        AS max_latency,
                stddev_samp(latency_ms::double precision)
                    FILTER (WHERE ok)                        AS stdev_latency,
                percentile_cont(0.50) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE ok AND latency_ms IS NOT NULL) AS p50,
                percentile_cont(0.95) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE ok AND latency_ms IS NOT NULL) AS p95,
                percentile_cont(0.99) WITHIN GROUP (ORDER BY latency_ms)
                    FILTER (WHERE ok AND latency_ms IS NOT NULL) AS p99,
                -- A response counts as COMPARABLE only when at least two indexers agreed on a
                -- majority answer for the same probe, and the deployment is not known to be
                -- non-deterministic. Without those conditions a probe answered by a single indexer
                -- yielded `comparable=1, divergent=1` — a minority of one, with no majority to
                -- differ from — and published it as "serving wrong data" against a named operator.
                avg(blocks_behind)                           AS avg_blocks_behind,
                max(blocks_behind)                           AS max_blocks_behind,
                count(*) FILTER (
                    WHERE response_hash IS NOT NULL
                      AND clear_majority
                      AND NOT nondeterministic
                )                                            AS comparable_count,
                count(*) FILTER (
                    WHERE response_hash IS NOT NULL
                      AND clear_majority
                      AND NOT nondeterministic
                      AND response_hash <> majority_hash
                )                                            AS divergent_count
            FROM obs
            GROUP BY 1, 2, 3
        ),
        -- allocation_map is keyed per allocation, so an indexer appears once per allocation.
        -- Any of its URLs identifies the same operator; collapse to one to keep the join 1:1.
        urls AS (
            SELECT indexer_address, max(indexer_url) AS indexer_url
            FROM allocation_map
            WHERE indexer_address IS NOT NULL
              AND indexer_url IS NOT NULL
              AND indexer_url <> ''
            GROUP BY 1
        )
        INSERT INTO foghorn_qos (
            indexer_address, deployment_id, bucket_start, bucket_secs,
            indexer_url, chain_id, gateway_id,
            query_count, num_indexer_200_responses, proportion_indexer_200_responses,
            avg_indexer_latency_ms, max_indexer_latency_ms, stdev_indexer_latency_ms,
            latency_p50_ms, latency_p95_ms, latency_p99_ms,
            avg_indexer_blocks_behind, max_indexer_blocks_behind,
            comparable_count, divergent_count, correctness_rate,
            computed_at
        )
        SELECT
            a.indexer_address,
            a.deployment_id,
            a.bucket_start,
            $3,
            u.indexer_url,
            $4,
            $5,
            a.query_count,
            a.num_200,
            a.num_200::double precision / a.query_count::double precision,
            a.avg_latency,
            a.max_latency,
            a.stdev_latency,
            a.p50::int,
            a.p95::int,
            a.p99::int,
            a.avg_blocks_behind,
            a.max_blocks_behind,
            a.comparable_count,
            a.divergent_count,
            CASE WHEN a.comparable_count > 0
                 THEN 1.0 - (a.divergent_count::double precision
                             / a.comparable_count::double precision)
                 ELSE NULL
            END,
            NOW()
        FROM agg a
        LEFT JOIN urls u ON u.indexer_address = a.indexer_address
        ON CONFLICT (indexer_address, deployment_id, bucket_start, bucket_secs) DO UPDATE SET
            indexer_url                      = EXCLUDED.indexer_url,
            chain_id                         = EXCLUDED.chain_id,
            gateway_id                       = EXCLUDED.gateway_id,
            query_count                      = EXCLUDED.query_count,
            num_indexer_200_responses        = EXCLUDED.num_indexer_200_responses,
            proportion_indexer_200_responses = EXCLUDED.proportion_indexer_200_responses,
            avg_indexer_latency_ms           = EXCLUDED.avg_indexer_latency_ms,
            max_indexer_latency_ms           = EXCLUDED.max_indexer_latency_ms,
            stdev_indexer_latency_ms         = EXCLUDED.stdev_indexer_latency_ms,
            latency_p50_ms                   = EXCLUDED.latency_p50_ms,
            latency_p95_ms                   = EXCLUDED.latency_p95_ms,
            latency_p99_ms                   = EXCLUDED.latency_p99_ms,
            avg_indexer_blocks_behind        = EXCLUDED.avg_indexer_blocks_behind,
            max_indexer_blocks_behind        = EXCLUDED.max_indexer_blocks_behind,
            comparable_count                 = EXCLUDED.comparable_count,
            divergent_count                  = EXCLUDED.divergent_count,
            correctness_rate                 = EXCLUDED.correctness_rate,
            computed_at                      = EXCLUDED.computed_at
        "#,
    )
    .bind(bucket)
    .bind(from)
    .bind(cfg.bucket_secs as i32)
    .bind(&cfg.chain_id)
    .bind(&cfg.gateway_id)
    .bind(cfg.chainhead_offset as i64)
    .bind(to)
    .bind(deployment)
    .execute(pool)
    .await?;

    Ok(result.rows_affected())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{DateTime, Duration as ChronoDuration, Utc};
    use foghorn_core::testdb::{self, expect, TestDb};
    use sqlx::Row;
    use std::collections::HashMap;

    async fn bucket_totals(pool: &PgPool) -> HashMap<String, (i64, i64, i64)> {
        sqlx::query(
            "SELECT indexer_address, sum(query_count)::bigint AS q,
                    sum(divergent_count)::bigint AS d, sum(comparable_count)::bigint AS c
             FROM foghorn_qos GROUP BY 1",
        )
        .fetch_all(pool)
        .await
        .unwrap()
        .iter()
        .map(|r| {
            (
                r.get::<String, _>("indexer_address"),
                (
                    r.get::<i64, _>("q"),
                    r.get::<i64, _>("d"),
                    r.get::<i64, _>("c"),
                ),
            )
        })
        .collect()
    }

    #[tokio::test]
    async fn the_feed_and_the_grades_agree_on_who_served_wrong_data() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        testdb::seed_disagreement(&db.pool, Utc::now() - ChronoDuration::minutes(10)).await;

        rollup_once(&QosRollupConfig::default(), &db.pool)
            .await
            .unwrap();
        let feed = bucket_totals(&db.pool).await;
        let graded = crate::scorer::load_probe_agg(&db.pool, "1 hour")
            .await
            .unwrap();

        assert_eq!(
            feed[testdb::INDEXER],
            (expect::OBSERVATIONS, expect::FAULTS, expect::COMPARABLE)
        );
        for (indexer, agg) in &graded {
            let (observations, faults, _) = feed.get(indexer).copied().unwrap_or_default();
            assert_eq!((observations, faults), (agg.total, agg.faults), "{indexer}");
        }
        db.drop_database().await;
    }

    #[tokio::test]
    async fn a_bucket_cut_by_the_lookback_edge_is_still_counted_whole() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        let bucket = QosRollupConfig::default().bucket_secs as i64;
        let now = Utc::now().timestamp();
        let edge = (now - 3600).div_euclid(bucket) * bucket + bucket / 2;
        for offset in [-60, 60] {
            let at = DateTime::from_timestamp(edge + offset, 0).unwrap();
            testdb::seed_answer(&db.pool, at).await;
        }
        let cfg = QosRollupConfig {
            lookback_secs: (now - edge) as u64,
            ..Default::default()
        };

        rollup_once(&cfg, &db.pool).await.unwrap();

        assert_eq!(bucket_totals(&db.pool).await[testdb::INDEXER].0, 2);
        db.drop_database().await;
    }

    #[tokio::test]
    async fn reroll_all_recomputes_every_stored_bucket_and_running_it_again_changes_nothing() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        testdb::seed_disagreement(&db.pool, Utc::now() - ChronoDuration::days(3)).await;
        sqlx::query(
            "INSERT INTO foghorn_qos (indexer_address, deployment_id, bucket_start, bucket_secs, gateway_id,
                                      query_count, num_indexer_200_responses, proportion_indexer_200_responses,
                                      comparable_count, divergent_count)
             SELECT $1, $2, to_timestamp(floor(extract(epoch FROM min(dispatched_at)) / 300) * 300), 300,
                    'lodestar', 1, 1, 1.0, 1, 1
             FROM probe",
        )
        .bind(testdb::INDEXER)
        .bind(testdb::DEPLOYMENT)
        .execute(&db.pool)
        .await
        .unwrap();

        let cfg = QosRollupConfig::default();
        reroll_all(&cfg, &db.pool).await.unwrap();
        let once = bucket_totals(&db.pool).await;
        reroll_all(&cfg, &db.pool).await.unwrap();

        assert_eq!(
            once[testdb::INDEXER],
            (expect::OBSERVATIONS, expect::FAULTS, expect::COMPARABLE)
        );
        assert_eq!(bucket_totals(&db.pool).await, once);
        db.drop_database().await;
    }
}
