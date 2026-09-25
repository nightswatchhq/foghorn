//! Evaluates alert subscriptions (lodestar#256) against Lodestar's public API and posts to each
//! subscriber's webhook. The evaluation itself is `foghorn_core::alerts`; this is the plumbing.

use anyhow::{anyhow, Result};
use foghorn_core::alerts::{self, AllocSnap, Snapshot, State};
use serde_json::Value;
use sqlx::{PgPool, Row};
use std::collections::HashMap;
use std::time::Duration;
use tracing::{info, warn};
use uuid::Uuid;

use crate::lodestar::LodestarClient;

const POLL_SECS: u64 = 900;
/// A day of failed posts at the poll interval, then the subscription stops trying.
const DISABLE_AFTER_FAILURES: i32 = 96;

pub async fn run_subscription_loop(client: LodestarClient, pool: PgPool) {
    info!(interval = POLL_SECS, "Alert subscription loop starting");
    loop {
        if let Err(e) = run_cycle(&client, &pool).await {
            warn!(error = %e, "Alert subscription cycle failed");
        }
        tokio::time::sleep(Duration::from_secs(POLL_SECS)).await;
    }
}

struct Sub {
    id: Uuid,
    indexer: String,
    url: String,
    kinds: Vec<String>,
    pct: f64,
    failures: i32,
    state: State,
}

async fn run_cycle(client: &LodestarClient, pool: &PgPool) -> Result<()> {
    let rows = sqlx::query(
        "SELECT s.id, s.indexer_address, s.webhook_url, s.kinds, s.signal_move_pct, s.consecutive_failures, st.state \
         FROM alert_subscription s LEFT JOIN alert_subscription_state st ON st.subscription_id = s.id \
         WHERE s.disabled_at IS NULL ORDER BY s.indexer_address",
    )
    .fetch_all(pool)
    .await?;
    if rows.is_empty() {
        return Ok(());
    }
    let subs: Vec<Sub> = rows
        .iter()
        .map(|r| Sub {
            id: r.get("id"),
            indexer: r.get("indexer_address"),
            url: r.get("webhook_url"),
            kinds: r.get("kinds"),
            pct: r.get("signal_move_pct"),
            failures: r.get("consecutive_failures"),
            state: r
                .get::<Option<Value>, _>("state")
                .and_then(|v| serde_json::from_value(v).ok())
                .unwrap_or_default(),
        })
        .collect();

    let network = client.get_json("/api/network-stats").await?;
    let current_epoch = network["data"]["graphNetwork"]["currentEpoch"]
        .as_i64()
        .ok_or_else(|| anyhow!("network-stats has no currentEpoch"))?;

    let mut snapshots: HashMap<String, Option<Snapshot>> = HashMap::new();
    let (mut delivered, mut failed) = (0, 0);
    for sub in subs {
        if !snapshots.contains_key(&sub.indexer) {
            let snap = match snapshot(client, &sub.indexer, current_epoch).await {
                Ok(s) => Some(s),
                Err(e) => {
                    warn!(indexer = %sub.indexer, error = %e, "No snapshot this cycle; subscriptions keep their state");
                    None
                }
            };
            snapshots.insert(sub.indexer.clone(), snap);
        }
        let Some(snap) = snapshots[&sub.indexer].as_ref() else {
            continue;
        };

        let (events, next) = alerts::evaluate(&sub.state, snap, &sub.kinds, sub.pct);
        if events.is_empty() {
            save_state(pool, sub.id, &next, false).await?;
            continue;
        }
        match alerts::deliver(&sub.url, &alerts::payloads(&sub.url, &sub.indexer, &events)).await {
            Ok(()) => {
                save_state(pool, sub.id, &next, true).await?;
                delivered += 1;
            }
            Err(e) => {
                failed += 1;
                sqlx::query(
                    "UPDATE alert_subscription SET consecutive_failures = consecutive_failures + 1, last_error = $2, \
                     last_evaluated_at = NOW(), disabled_at = CASE WHEN consecutive_failures + 1 >= $3 THEN NOW() END WHERE id = $1",
                )
                .bind(sub.id)
                .bind(&e)
                .bind(DISABLE_AFTER_FAILURES)
                .execute(pool)
                .await?;
                if sub.failures + 1 >= DISABLE_AFTER_FAILURES {
                    warn!(id = %sub.id, error = %e, "Alert subscription disabled after a day of failed posts");
                }
            }
        }
    }
    if delivered + failed > 0 {
        info!(delivered, failed, "Alert subscriptions posted");
    }
    Ok(())
}

async fn save_state(pool: &PgPool, id: Uuid, state: &State, delivered: bool) -> Result<()> {
    let mut tx = pool.begin().await?;
    sqlx::query(
        "INSERT INTO alert_subscription_state (subscription_id, state, updated_at) VALUES ($1, $2, NOW()) \
         ON CONFLICT (subscription_id) DO UPDATE SET state = EXCLUDED.state, updated_at = NOW()",
    )
    .bind(id)
    .bind(serde_json::to_value(state)?)
    .execute(&mut *tx)
    .await?;
    sqlx::query(
        "UPDATE alert_subscription SET last_evaluated_at = NOW(), \
         last_delivered_at = CASE WHEN $2 THEN NOW() ELSE last_delivered_at END, \
         consecutive_failures = CASE WHEN $2 THEN 0 ELSE consecutive_failures END, \
         last_error = CASE WHEN $2 THEN NULL ELSE last_error END WHERE id = $1",
    )
    .bind(id)
    .bind(delivered)
    .execute(&mut *tx)
    .await?;
    tx.commit().await?;
    Ok(())
}

async fn snapshot(client: &LodestarClient, indexer: &str, current_epoch: i64) -> Result<Snapshot> {
    let detail = client.get_json(&format!("/api/indexer/{indexer}")).await?;
    let reo = client
        .get_json(&format!("/api/reo?address={indexer}"))
        .await
        .ok();
    parse_snapshot(&detail, reo.as_ref(), current_epoch)
}

fn wei_to_grt(v: &Value) -> f64 {
    v.as_str()
        .and_then(|s| s.parse::<f64>().ok())
        .unwrap_or(0.0)
        / 1e18
}

/// A missing `allocations` means kittiwake could not read them (kittiwake#153), not that there are
/// none, so it fails the snapshot rather than reading as every allocation having closed.
pub fn parse_snapshot(detail: &Value, reo: Option<&Value>, current_epoch: i64) -> Result<Snapshot> {
    let indexer = &detail["data"]["indexer"];
    if indexer.is_null() {
        return Err(anyhow!("no such indexer"));
    }
    let list = indexer["allocations"]
        .as_array()
        .ok_or_else(|| anyhow!("allocations were not served"))?;
    let allocations = list
        .iter()
        .filter_map(|a| {
            let d = &a["subgraphDeployment"];
            Some(AllocSnap {
                id: a["id"].as_str()?.to_string(),
                ipfs_hash: d["ipfsHash"].as_str()?.to_string(),
                name: d["displayName"].as_str().map(str::to_string),
                created_at_epoch: a["createdAtEpoch"].as_i64()?,
                signal_grt: wei_to_grt(&d["signalledTokens"]),
                denied: d["deniedSince"].as_i64().is_some_and(|b| b > 0),
            })
        })
        .collect();
    let reo_status = reo
        .map(|r| &r["status"])
        .filter(|s| s["available"].as_bool() == Some(true))
        .and_then(|s| s["status"].as_str())
        .map(str::to_string);
    Ok(Snapshot {
        current_epoch,
        allocations,
        indexing_reward_cut: indexer["indexingRewardCut"].as_i64(),
        query_fee_cut: indexer["queryFeeCut"].as_i64(),
        reo_status,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn parses_what_kittiwake_serves() {
        let detail = json!({ "data": { "indexer": {
            "indexingRewardCut": 1000000, "queryFeeCut": 0,
            "allocations": [{ "id": "0xa", "createdAtEpoch": 1390, "subgraphDeployment": {
                "ipfsHash": "QmA", "displayName": "pinlink", "signalledTokens": "3022381637595481637834", "deniedSince": 123
            }}]
        }}});
        let reo = json!({ "status": { "available": true, "status": "eligible" } });
        let s = parse_snapshot(&detail, Some(&reo), 1393).unwrap();
        assert_eq!(s.allocations.len(), 1);
        assert!(s.allocations[0].denied);
        assert!((s.allocations[0].signal_grt - 3022.38).abs() < 0.01);
        assert_eq!(s.reo_status.as_deref(), Some("eligible"));
        assert_eq!(s.indexing_reward_cut, Some(1_000_000));
    }

    #[test]
    fn unread_allocations_are_not_no_allocations() {
        let detail = json!({ "data": { "indexer": { "indexingRewardCut": 0, "queryFeeCut": 0 }, "degraded": ["allocations"] } });
        assert!(parse_snapshot(&detail, None, 1).is_err());
        let unavailable = json!({ "status": { "available": false } });
        let ok = json!({ "data": { "indexer": { "allocations": [] } } });
        assert_eq!(
            parse_snapshot(&ok, Some(&unavailable), 1)
                .unwrap()
                .reo_status,
            None
        );
    }
}
