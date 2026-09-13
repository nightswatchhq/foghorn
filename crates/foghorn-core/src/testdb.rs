//! Throwaway databases for tests of the SQL that decides what Foghorn publishes.
//!
//! Each test gets its own database on the server named by `FOGHORN_TEST_DATABASE_URL`, migrated
//! from scratch. Unset on a workstation, the test says it was skipped and returns. Unset under CI,
//! it fails: a suite that passes by observing nothing is the failure this project exists to catch.

use chrono::{DateTime, Utc};
use sqlx::{Connection, Executor, PgConnection, PgPool};
use uuid::Uuid;

pub struct TestDb {
    pub pool: PgPool,
    admin_url: String,
    name: String,
}

impl TestDb {
    pub async fn create() -> Option<Self> {
        let admin_url = match database_url(
            std::env::var("CI").ok().as_deref(),
            std::env::var("FOGHORN_TEST_DATABASE_URL").ok(),
        ) {
            Ok(Some(url)) => url,
            Ok(None) => {
                eprintln!("FOGHORN_TEST_DATABASE_URL is unset; skipping a database test");
                return None;
            }
            Err(e) => panic!("{e}"),
        };
        let name = format!("foghorn_test_{}", Uuid::new_v4().simple());
        let mut admin = PgConnection::connect(&admin_url)
            .await
            .expect("connect to FOGHORN_TEST_DATABASE_URL");
        admin
            .execute(format!("CREATE DATABASE {name}").as_str())
            .await
            .expect("create the test database");
        let pool = PgPool::connect(&with_database(&admin_url, &name))
            .await
            .expect("connect to the test database");
        crate::db::run_migrations(&pool)
            .await
            .expect("migrate the test database");
        Some(Self {
            pool,
            admin_url,
            name,
        })
    }

    pub async fn drop_database(self) {
        self.pool.close().await;
        let mut admin = PgConnection::connect(&self.admin_url)
            .await
            .expect("reconnect to drop the test database");
        admin
            .execute(format!("DROP DATABASE IF EXISTS {}", self.name).as_str())
            .await
            .expect("drop the test database");
    }
}

fn database_url(ci: Option<&str>, url: Option<String>) -> Result<Option<String>, String> {
    match (url.filter(|u| !u.is_empty()), ci) {
        (Some(url), _) => Ok(Some(url)),
        (None, Some(ci)) if !ci.is_empty() && ci != "false" => Err(
            "CI is set and FOGHORN_TEST_DATABASE_URL is not: the database tests would pass without running"
                .to_string(),
        ),
        (None, _) => Ok(None),
    }
}

fn with_database(url: &str, name: &str) -> String {
    let (base, query) = match url.split_once('?') {
        Some((b, q)) => (b, Some(q)),
        None => (url, None),
    };
    let base = base.rsplit_once('/').map_or(base, |(b, _)| b);
    match query {
        Some(q) => format!("{base}/{name}?{q}"),
        None => format!("{base}/{name}"),
    }
}

pub const INDEXER: &str = "0x00000000000000000000000000000000000000a1";
pub const PEER_J: &str = "0x00000000000000000000000000000000000000a2";
pub const PEER_L: &str = "0x00000000000000000000000000000000000000a3";
pub const PEER_PAID: &str = "0x00000000000000000000000000000000000000a4";
pub const KEY_INDEXER: &str = "0x00000000000000000000000000000000000000b1";
const KEY_J: &str = "0x00000000000000000000000000000000000000b2";
const KEY_L: &str = "0x00000000000000000000000000000000000000b3";
pub const KEY_UNRESOLVED: &str = "0x00000000000000000000000000000000000000b9";
const KEY_UNRESOLVED_2: &str = "0x00000000000000000000000000000000000000b8";
const KEY_UNRESOLVED_3: &str = "0x00000000000000000000000000000000000000b7";

pub const DEPLOYMENT: &str = "QmTestDeterministicDeploymentAAAAAAAAAAAAAAAAA";
pub const NONDETERMINISTIC_DEPLOYMENT: &str = "QmTestNondeterministicDeploymentBBBBBBBBBBBBBB";
pub const ROTATING_DEPLOYMENT: &str = "QmTestRotatingMinorityDeploymentCCCCCCCCCCCCCC";
pub const UNATTRIBUTED_DEPLOYMENT: &str = "QmTestUnattributedRotationDeploymentDDDDDDDDDD";

/// What [`seed_disagreement`] must produce for [`INDEXER`] under the one definition of a fault: an
/// answer that differs from the largest cluster by count, when that cluster is more than half of
/// the answers, on a deployment not known to be non-deterministic.
pub mod expect {
    pub const OBSERVATIONS: i64 = 5;
    pub const ANSWERED: i64 = 4;
    pub const ERRORS: i64 = 1;
    pub const FAULTS: i64 = 1;
    pub const COMPARABLE: i64 = 2;
    pub const ANSWERED_ON_DEPLOYMENT: i64 = 3;
    pub const FAULTS_ON_DEPLOYMENT: i64 = 1;
}

/// Six probes, all dispatched at `at`, built so that each way of counting differently gives a
/// different answer.
pub async fn seed_disagreement(pool: &PgPool, at: DateTime<Utc>) {
    for (key, indexer) in [
        (KEY_INDEXER, Some(INDEXER)),
        (KEY_J, Some(PEER_J)),
        (KEY_L, Some(PEER_L)),
        (KEY_UNRESOLVED, None),
    ] {
        sqlx::query("INSERT INTO allocation_map (allocation_key, indexer_address) VALUES ($1, $2)")
            .bind(key)
            .bind(indexer)
            .execute(pool)
            .await
            .expect("seed allocation_map");
    }
    sqlx::query(
        "INSERT INTO nondeterministic_deployment (deployment_id, divergent_probes, total_probes, divergence_rate)
         VALUES ($1, 10, 10, 1.0)",
    )
    .bind(NONDETERMINISTIC_DEPLOYMENT)
    .execute(pool)
    .await
    .expect("seed nondeterministic_deployment");

    // The heaviest stake sits in the minority, so a stake majority and a count majority disagree.
    let p = probe(pool, DEPLOYMENT, at).await;
    answer(pool, p, KEY_INDEXER, "a", 1.0, "gateway").await;
    answer(pool, p, KEY_J, "a", 1.0, "gateway").await;
    answer(pool, p, KEY_L, "b", 5.0, "gateway").await;
    divergence(pool, p, "a", 2, "b", 5.0).await;

    // Two answers, no majority: the scheduler writes no divergence row.
    let p = probe(pool, DEPLOYMENT, at).await;
    answer(pool, p, KEY_INDEXER, "c", 1.0, "gateway").await;
    answer(pool, p, KEY_J, "d", 1.0, "gateway").await;

    let p = probe(pool, DEPLOYMENT, at).await;
    answer(pool, p, KEY_INDEXER, "e", 1.0, "gateway").await;
    answer(pool, p, KEY_J, "f", 1.0, "gateway").await;
    answer(pool, p, KEY_L, "f", 1.0, "gateway").await;
    answer(pool, p, PEER_PAID, "f", 1.0, "paid").await;
    divergence(pool, p, "f", 3, "f", 3.0).await;

    let p = probe(pool, NONDETERMINISTIC_DEPLOYMENT, at).await;
    answer(pool, p, KEY_INDEXER, "g", 1.0, "gateway").await;
    answer(pool, p, KEY_J, "h", 1.0, "gateway").await;
    answer(pool, p, KEY_L, "h", 1.0, "gateway").await;
    divergence(pool, p, "h", 2, "h", 2.0).await;

    // A real error from the indexer, and a refusal of our payment, which describes our escrow.
    let p = probe(pool, DEPLOYMENT, at).await;
    failure(pool, p, KEY_INDEXER, "http_error", 500, "gateway").await;
    failure(pool, p, INDEXER, "payment_denylisted", 402, "paid").await;

    let p = probe(pool, DEPLOYMENT, at).await;
    answer(pool, p, KEY_UNRESOLVED, "a", 1.0, "gateway").await;
}

/// Four probes on [`ROTATING_DEPLOYMENT`], each with a clear majority and a minority of one that
/// rotates across three indexers: the shape `detect_nondeterministic` flags.
pub async fn seed_rotating_minority(pool: &PgPool, at: DateTime<Utc>) {
    for (key, indexer) in [(KEY_INDEXER, INDEXER), (KEY_J, PEER_J), (KEY_L, PEER_L)] {
        sqlx::query(
            "INSERT INTO allocation_map (allocation_key, indexer_address) VALUES ($1, $2)
             ON CONFLICT (allocation_key) DO NOTHING",
        )
        .bind(key)
        .bind(indexer)
        .execute(pool)
        .await
        .expect("seed allocation_map");
    }
    for minority in [KEY_INDEXER, KEY_J, KEY_L, KEY_INDEXER] {
        let p = probe(pool, ROTATING_DEPLOYMENT, at).await;
        for key in [KEY_INDEXER, KEY_J, KEY_L] {
            let hash = if key == minority { "odd" } else { "even" };
            answer(pool, p, key, hash, 1.0, "gateway").await;
        }
        divergence(pool, p, "even", 2, "even", 2.0).await;
    }
}

/// Four probes on [`UNATTRIBUTED_DEPLOYMENT`] where two attributed indexers always agree, the
/// minority rotates across signing keys nobody could attribute, and a payment is refused beside
/// them. None of it is evidence the rest of Foghorn accepts.
pub async fn seed_unattributed_rotation(pool: &PgPool, at: DateTime<Utc>) {
    for (key, indexer) in [
        (KEY_INDEXER, Some(INDEXER)),
        (KEY_J, Some(PEER_J)),
        (KEY_UNRESOLVED, None),
        (KEY_UNRESOLVED_2, None),
        (KEY_UNRESOLVED_3, None),
    ] {
        sqlx::query(
            "INSERT INTO allocation_map (allocation_key, indexer_address) VALUES ($1, $2)
             ON CONFLICT (allocation_key) DO NOTHING",
        )
        .bind(key)
        .bind(indexer)
        .execute(pool)
        .await
        .expect("seed allocation_map");
    }
    for stray in [
        KEY_UNRESOLVED,
        KEY_UNRESOLVED_2,
        KEY_UNRESOLVED_3,
        KEY_UNRESOLVED,
    ] {
        let p = probe(pool, UNATTRIBUTED_DEPLOYMENT, at).await;
        answer(pool, p, KEY_INDEXER, "even", 1.0, "gateway").await;
        answer(pool, p, KEY_J, "even", 1.0, "gateway").await;
        answer(pool, p, stray, "odd", 1.0, "gateway").await;
        failure(pool, p, PEER_L, "payment_denylisted", 402, "paid").await;
        divergence(pool, p, "even", 2, "even", 2.0).await;
    }
}

/// One paid answer from [`INDEXER`] on its own probe, needing no attribution.
pub async fn seed_answer(pool: &PgPool, at: DateTime<Utc>) {
    let p = probe(pool, DEPLOYMENT, at).await;
    answer(pool, p, INDEXER, "a", 1.0, "paid").await;
}

async fn probe(pool: &PgPool, deployment: &str, at: DateTime<Utc>) -> Uuid {
    let id = Uuid::new_v4();
    sqlx::query(
        "INSERT INTO probe (id, deployment_id, block_hash, block_number, query_hash, query_category, query_text, dispatched_at)
         VALUES ($1, $2, '0xblock', 1000, '0xquery', 'Q_byid', '{}', $3)",
    )
    .bind(id)
    .bind(deployment)
    .bind(at)
    .execute(pool)
    .await
    .expect("seed probe");
    id
}

async fn answer(pool: &PgPool, probe: Uuid, address: &str, hash: &str, stake: f64, mode: &str) {
    sqlx::query(
        "INSERT INTO observation (probe_id, indexer_address, response_hash, latency_ms, http_status, stake_weight, dispatch_mode)
         VALUES ($1, $2, $3, 100, 200, $4, $5)",
    )
    .bind(probe)
    .bind(address)
    .bind(hash)
    .bind(stake)
    .bind(mode)
    .execute(pool)
    .await
    .expect("seed answer");
}

async fn failure(pool: &PgPool, probe: Uuid, address: &str, error: &str, status: i32, mode: &str) {
    sqlx::query(
        "INSERT INTO observation (probe_id, indexer_address, latency_ms, http_status, error_class, dispatch_mode)
         VALUES ($1, $2, 100, $3, $4, $5)",
    )
    .bind(probe)
    .bind(address)
    .bind(status)
    .bind(error)
    .bind(mode)
    .execute(pool)
    .await
    .expect("seed failure");
}

async fn divergence(
    pool: &PgPool,
    probe: Uuid,
    count_hash: &str,
    count_size: i32,
    stake_hash: &str,
    stake_weight: f64,
) {
    sqlx::query(
        "INSERT INTO divergence (probe_id, cluster_count, largest_by_count_hash, largest_by_count_size, largest_by_stake_hash, largest_by_stake_weight)
         VALUES ($1, 2, $2, $3, $4, $5)",
    )
    .bind(probe)
    .bind(count_hash)
    .bind(count_size)
    .bind(stake_hash)
    .bind(stake_weight)
    .execute(pool)
    .await
    .expect("seed divergence");
}

#[cfg(test)]
mod tests {
    use super::database_url;

    #[test]
    fn a_ci_run_without_a_database_fails_instead_of_skipping() {
        assert!(database_url(Some("true"), None).is_err());
        assert!(database_url(Some("1"), Some(String::new())).is_err());
        assert_eq!(database_url(None, None), Ok(None));
        assert_eq!(database_url(Some("false"), None), Ok(None));
        assert_eq!(
            database_url(Some("true"), Some("postgres://x/y".into())),
            Ok(Some("postgres://x/y".into()))
        );
    }
}
