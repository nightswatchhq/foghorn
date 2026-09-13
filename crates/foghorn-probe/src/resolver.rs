use anyhow::Result;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use sqlx::PgPool;
use std::collections::HashSet;
use tracing::{info, warn};

const NETWORK_SUBGRAPH_ID: &str = "DZz4kDTdmzWLWsV373w2bSmoar3umKKH9y82SUKr5qmp";
const NETWORK_SUBGRAPH_BASE: &str = "https://gateway-arbitrum.network.thegraph.com/api";

#[derive(Deserialize)]
struct GraphQLResponse {
    data: Option<AllocationData>,
}

#[derive(Deserialize)]
struct AllocationData {
    allocations: Vec<AllocationNode>,
}

#[derive(Deserialize)]
struct AllocationNode {
    id: String,
    indexer: IndexerNode,
}

#[derive(Deserialize)]
struct IndexerNode {
    id: String,
    url: Option<String>,
}

/// Resolve unresolved allocation keys to indexer addresses via the Graph Network subgraph.
/// Runs after each probe round. Skips keys resolved within the last 24h (including NULL results).
pub async fn resolve_allocation_keys(
    pool: &PgPool,
    _gateway_url: &str,
    api_key: &str,
) -> Result<()> {
    let unresolved: Vec<String> = sqlx::query_scalar(
        r#"SELECT DISTINCT o.indexer_address
           FROM observation o
           WHERE NOT EXISTS (
               SELECT 1 FROM allocation_map am
               WHERE am.allocation_key = o.indexer_address
               AND am.resolved_at > NOW() - INTERVAL '24 hours'
           )
           LIMIT 200"#,
    )
    .fetch_all(pool)
    .await?;

    if unresolved.is_empty() {
        return Ok(());
    }

    info!(count = unresolved.len(), "Resolving allocation keys");

    let url = format!(
        "{}/{}/subgraphs/id/{}",
        NETWORK_SUBGRAPH_BASE,
        api_key,
        NETWORK_SUBGRAPH_ID
    );

    // A timeout, because the alternative is a task that hangs forever.
    //
    // `Client::new()` has NO default timeout. This loop runs inside the probe round, so a gateway
    // that accepts the connection and then never answers would wedge the whole round silently -
    // the same shape as the panic that froze the grade board overnight: process healthy, logs quiet,
    // work stopped. This was the last client in the codebase without one.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(30))
        .build()
        .unwrap_or_else(|_| reqwest::Client::new());

    for chunk in unresolved.chunks(100) {
        let ids: Vec<String> = chunk.iter().map(|s| s.to_lowercase()).collect();

        let body = json!({
            "query": format!(
                r#"{{ allocations(where: {{ id_in: {:?} }}, first: 100) {{ id indexer {{ id url }} }} }}"#,
                ids
            )
        });

        let resp = match client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&body)
            .timeout(std::time::Duration::from_secs(15))
            .send()
            .await
        {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Network subgraph request failed");
                continue;
            }
        };

        let gql: GraphQLResponse = match resp.json().await {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to parse network subgraph response");
                continue;
            }
        };

        let mut found: HashSet<String> = HashSet::new();

        if let Some(data) = gql.data {
            for alloc in &data.allocations {
                let key = alloc.id.to_lowercase();
                found.insert(key.clone());

                record_attribution(
                    pool,
                    &key,
                    &alloc.indexer.id.to_lowercase(),
                    alloc.indexer.url.as_deref(),
                )
                .await?;
            }

            info!(resolved = found.len(), "Allocation keys resolved");
        }

        // Insert NULL entries for keys not returned — marks them so we don't spam the subgraph.
        // They'll be retried after 24h (the NOT EXISTS clause above).
        for key in &ids {
            if !found.contains(key) {
                sqlx::query(
                    r#"INSERT INTO allocation_map (allocation_key, indexer_address, indexer_url, resolved_at)
                       VALUES ($1, NULL, NULL, NOW())
                       ON CONFLICT (allocation_key) DO UPDATE SET resolved_at = NOW()"#,
                )
                .bind(key)
                .execute(pool)
                .await?;
            }
        }
    }

    Ok(())
}

/// Record who a signing key belongs to. When that changes, the buckets holding the key's
/// observations are re-rolled: they may be days old by the time a NULL key is retried.
pub(crate) async fn record_attribution(
    pool: &PgPool,
    key: &str,
    indexer: &str,
    url: Option<&str>,
) -> Result<()> {
    sqlx::query(
        r#"WITH prev AS (
               SELECT indexer_address FROM allocation_map WHERE allocation_key = $1
           ),
           upsert AS (
               INSERT INTO allocation_map (allocation_key, indexer_address, indexer_url, resolved_at)
               VALUES ($1, $2, $3, NOW())
               ON CONFLICT (allocation_key) DO UPDATE
               SET indexer_address = EXCLUDED.indexer_address,
                   indexer_url     = EXCLUDED.indexer_url,
                   resolved_at     = NOW()
           )
           INSERT INTO qos_reroll (deployment_id, from_ts, to_ts, reason)
           SELECT p.deployment_id, min(p.dispatched_at), max(p.dispatched_at), 'allocation key attributed'
           FROM observation o
           JOIN probe p ON p.id = o.probe_id
           WHERE o.indexer_address = $1
             AND (SELECT indexer_address FROM prev) IS DISTINCT FROM $2
           GROUP BY p.deployment_id"#,
    )
    .bind(key)
    .bind(indexer)
    .bind(url)
    .execute(pool)
    .await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Duration as ChronoDuration, Utc};
    use foghorn_core::config::QosRollupConfig;
    use foghorn_core::testdb::{self, TestDb};

    async fn queries_for(pool: &PgPool, indexer: &str) -> i64 {
        sqlx::query_scalar(
            "SELECT COALESCE(sum(query_count), 0)::bigint FROM foghorn_qos WHERE indexer_address = $1",
        )
        .bind(indexer)
        .fetch_one(pool)
        .await
        .unwrap()
    }

    #[tokio::test]
    async fn a_key_attributed_after_its_buckets_rolled_up_is_counted_in_them() {
        let Some(db) = TestDb::create().await else {
            return;
        };
        testdb::seed_disagreement(&db.pool, Utc::now() - ChronoDuration::hours(2)).await;
        let wide = QosRollupConfig {
            lookback_secs: 3 * 3600,
            ..Default::default()
        };
        crate::qos::rollup_once(&wide, &db.pool).await.unwrap();
        let before = queries_for(&db.pool, testdb::PEER_J).await;

        record_attribution(&db.pool, testdb::KEY_UNRESOLVED, testdb::PEER_J, None)
            .await
            .unwrap();
        crate::qos::rollup_once(&QosRollupConfig::default(), &db.pool)
            .await
            .unwrap();

        assert_eq!(queries_for(&db.pool, testdb::PEER_J).await, before + 1);
        db.drop_database().await;
    }
}
